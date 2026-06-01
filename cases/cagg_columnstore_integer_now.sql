-- ============================================================================
-- Case:  00040119
-- Service / project: p3dthsrulv / gbvx1g57lj
--
-- Repro: columnstore policy on a continuous aggregate fails with
--        "integer_now function not set"
--
-- Setup (mirrors the customer's environment):
--   - Base hypertable `sqlth_34_data` chunked by an int8 epoch-ms column
--     (`t_stamp`), with a user-defined `now_ms()` set as the
--     `integer_now_func` on the base hypertable.
--   - Continuous aggregate `tag_values_1s_agg` on the base hypertable.
--     (Customer has a multi-level stack — one level is enough to repro.
--      Stacking cagg-on-cagg over integer time on PG18 hits a SEPARATE
--      validator issue we don't address here.)
--   - Columnstore enabled + columnstore policy on the base and the cagg.
--
-- Expected behavior with the bug:
--   - The base hypertable's columnstore policy job SUCCEEDS.
--   - The cagg columnstore policy job FAILS with
--       ERROR: integer_now function not set
--   - Manual `convert_to_columnstore(<cagg chunk>)` SUCCEEDS regardless.
--
-- Root cause (tsl/src/bgw_policy/job.c policy_recompression_execute):
--   The policy looks up the open dimension on the cagg's materialization
--   hypertable directly. That dimension has empty
--   integer_now_func / integer_now_func_schema — only the original base
--   hypertable has those populated. ts_get_integer_now_func() then raises.
--   The retention policy path does it correctly via
--   ts_continuous_agg_find_integer_now_func_by_materialization_id(); the
--   columnstore/recompression path does not.
--
-- Workaround (demonstrated below):
--   set_integer_now_func() on the materialization hypertable directly.
--
-- ----------------------------------------------------------------------------
-- Notes for running this script:
--
--   * Designed for `psql -f`. With \set ON_ERROR_STOP off, psql keeps going
--     after errors so you can see the BEFORE (fail) and AFTER (succeed)
--     states in one run.
--
--   * `CALL run_job(...)` issues internal COMMITs and CANNOT be wrapped in a
--     PL/pgSQL BEGIN/EXCEPTION block — that creates a subtransaction and
--     produces a spurious "cannot commit while a subtransaction is active"
--     that masks the real error. So we capture job ids with \gset and CALL
--     run_job at the top level.
--
--   * Integer-arithmetic gotcha: `30 * 86400000` is int4 * int4 and
--     overflows int4. Always cast the LITERAL: `30::BIGINT * 86400000`.
-- ============================================================================

\set ON_ERROR_STOP off
\timing off
\pset pager off

-- ---------------------------------------------------------------------------
-- 0. Clean prior repro state (idempotent).
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS tag_values_1s_agg CASCADE;
DROP TABLE IF EXISTS sqlth_34_data CASCADE;
DROP FUNCTION IF EXISTS now_ms() CASCADE;

CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT 'timescaledb version: ' || extversion FROM pg_extension WHERE extname = 'timescaledb';

-- ---------------------------------------------------------------------------
-- 1. Base hypertable: int8 epoch-ms time column.
-- ---------------------------------------------------------------------------
CREATE TABLE sqlth_34_data (
    tagid      INTEGER          NOT NULL,
    intvalue   BIGINT,
    floatvalue DOUBLE PRECISION,
    t_stamp    BIGINT           NOT NULL
);

-- 1 day = 86_400_000 ms per chunk
SELECT create_hypertable('sqlth_34_data', by_range('t_stamp', 86400000));

-- ---------------------------------------------------------------------------
-- 2. integer_now function (epoch ms) and bind it to the base hypertable.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION now_ms() RETURNS BIGINT
LANGUAGE SQL STABLE AS $$
    SELECT (EXTRACT(epoch FROM now()) * 1000)::BIGINT;
$$;

SELECT set_integer_now_func('sqlth_34_data', 'now_ms');

-- ---------------------------------------------------------------------------
-- 3. Insert 30 days of synthetic data so we get many chunks, most older
--    than the 7-day compress_after window.
-- ---------------------------------------------------------------------------
INSERT INTO sqlth_34_data (tagid, intvalue, t_stamp)
SELECT
    (random() * 10)::INT,
    (random() * 1000)::BIGINT,
    (EXTRACT(epoch FROM now() - (g || ' minutes')::interval) * 1000)::BIGINT
FROM generate_series(1, 30 * 24 * 60) g;

SELECT count(*) AS row_count   FROM sqlth_34_data;
SELECT count(*) AS chunk_count FROM show_chunks('sqlth_34_data');

-- ---------------------------------------------------------------------------
-- 4. Enable columnstore on the base hypertable.
-- ---------------------------------------------------------------------------
ALTER TABLE sqlth_34_data SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tagid'
);

-- ---------------------------------------------------------------------------
-- 5. Continuous aggregate on the base hypertable, plus a refresh policy
--    (required before a columnstore policy can be attached to a cagg).
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW tag_values_1s_agg
WITH (timescaledb.continuous) AS
SELECT
    tagid,
    time_bucket(60000, t_stamp) AS bucket,    -- 1-minute buckets (ms)
    avg(intvalue) AS avg_v,
    max(intvalue) AS max_v
FROM sqlth_34_data
GROUP BY tagid, bucket
WITH NO DATA;

CALL refresh_continuous_aggregate('tag_values_1s_agg', NULL, NULL);

ALTER MATERIALIZED VIEW tag_values_1s_agg SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tagid'
);

-- start_offset must be BIGINT-typed because 30 * 86400000 overflows int4.
SELECT add_continuous_aggregate_policy(
    continuous_aggregate => 'tag_values_1s_agg',
    start_offset         => 30::BIGINT * 86400000,
    end_offset           => 60000::BIGINT,
    schedule_interval    => INTERVAL '1 hour'
);

-- ---------------------------------------------------------------------------
-- 6. Register the columnstore policies (base + cagg).
--    `add_columnstore_policy` is a PROCEDURE — invoke with CALL.
-- ---------------------------------------------------------------------------
CALL add_columnstore_policy(
    hypertable    => 'sqlth_34_data',
    after         => (7 * 86400000)::BIGINT,
    if_not_exists => true
);

CALL add_columnstore_policy(
    hypertable    => 'tag_values_1s_agg',
    after         => (7 * 86400000)::BIGINT,
    if_not_exists => true
);

-- Show the columnstore jobs that were created.
SELECT bj.id AS job_id,
       bj.proc_name,
       coalesce(ca.user_view_name, ht.table_name) AS target
FROM _timescaledb_config.bgw_job bj
LEFT JOIN _timescaledb_catalog.hypertable      ht ON bj.hypertable_id = ht.id
LEFT JOIN _timescaledb_catalog.continuous_agg  ca ON bj.hypertable_id = ca.mat_hypertable_id
WHERE bj.proc_name = 'policy_compression'
  AND (ht.table_name = 'sqlth_34_data' OR ca.user_view_name = 'tag_values_1s_agg')
ORDER BY bj.id;

-- ---------------------------------------------------------------------------
-- 7. Catalog inspection: integer_now_func is set ONLY on the base
--    hypertable's dimension. The cagg's materialization hypertable
--    dimension is empty — this is the trigger for the bug.
-- ---------------------------------------------------------------------------
SELECT
    h.table_name,
    d.column_name,
    d.column_type,
    coalesce(d.integer_now_func_schema, '<null>') AS integer_now_schema,
    coalesce(d.integer_now_func,        '<null>') AS integer_now_func
FROM _timescaledb_catalog.hypertable h
JOIN _timescaledb_catalog.dimension  d ON d.hypertable_id = h.id
WHERE h.table_name = 'sqlth_34_data'
   OR h.id IN (
        SELECT mat_hypertable_id
        FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'tag_values_1s_agg'
   )
ORDER BY h.table_name;

-- ---------------------------------------------------------------------------
-- 8. Capture job ids into psql vars so we can CALL run_job at top level.
-- ---------------------------------------------------------------------------
SELECT bj.id AS j_base
FROM _timescaledb_config.bgw_job bj
JOIN _timescaledb_catalog.hypertable ht ON bj.hypertable_id = ht.id
WHERE ht.table_name = 'sqlth_34_data'
  AND bj.proc_name = 'policy_compression'
\gset

SELECT bj.id AS j_cagg
FROM _timescaledb_config.bgw_job bj
JOIN _timescaledb_catalog.continuous_agg ca ON bj.hypertable_id = ca.mat_hypertable_id
WHERE ca.user_view_name = 'tag_values_1s_agg'
  AND bj.proc_name = 'policy_compression'
\gset

\echo
\echo '*** base columnstore job id =' :j_base
\echo '*** cagg columnstore job id =' :j_cagg
\echo

-- ---------------------------------------------------------------------------
-- 9. BEFORE: run both jobs.
--    Base PASSES. Cagg FAILS with "integer_now function not set".
-- ---------------------------------------------------------------------------
\echo '=== BEFORE WORKAROUND ==='
\echo '--- running base hypertable columnstore policy ---'
CALL run_job(:j_base);

\echo '--- running cagg columnstore policy (expect: integer_now function not set) ---'
CALL run_job(:j_cagg);

-- ---------------------------------------------------------------------------
-- 10. Show the *manual* path still works on a cagg chunk even though the
--     scheduled policy failed.
-- ---------------------------------------------------------------------------
SELECT format('%I.%I', chunk_schema, chunk_name) AS cagg_chunk
FROM timescaledb_information.chunks
WHERE hypertable_schema = '_timescaledb_internal'
  AND hypertable_name = (
    SELECT format('_materialized_hypertable_%s', mat_hypertable_id)
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'tag_values_1s_agg'
  )
  AND NOT is_compressed
LIMIT 1
\gset

\echo
\echo '--- manually converting' :cagg_chunk 'to columnstore (expect: success) ---'
CALL convert_to_columnstore(:'cagg_chunk'::regclass);

-- ===========================================================================
-- 11. WORKAROUND: set integer_now_func on the cagg's materialization
--     hypertable directly. After this, the scheduled cagg policy succeeds.
-- ===========================================================================
\echo
\echo '=== APPLYING WORKAROUND ==='
SELECT set_integer_now_func(
    format('_timescaledb_internal._materialized_hypertable_%s',
        (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
         WHERE user_view_name = 'tag_values_1s_agg'))::regclass,
    'now_ms',
    replace_if_exists => true);

-- ---------------------------------------------------------------------------
-- 12. AFTER: re-run the cagg job. Now it should succeed.
-- ---------------------------------------------------------------------------
\echo
\echo '=== AFTER WORKAROUND ==='
\echo '--- re-running cagg columnstore policy (expect: success) ---'
CALL run_job(:j_cagg);

-- ---------------------------------------------------------------------------
-- 13. Final state: cagg materialization hypertable should now show
--     columnstore_chunks > 0.
-- ---------------------------------------------------------------------------
SELECT
    hypertable_name,
    count(*) FILTER (WHERE is_compressed)     AS columnstore_chunks,
    count(*) FILTER (WHERE NOT is_compressed) AS rowstore_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sqlth_34_data'
   OR hypertable_name LIKE '\_materialized\_hypertable\_%' ESCAPE '\'
GROUP BY hypertable_name
ORDER BY hypertable_name;
