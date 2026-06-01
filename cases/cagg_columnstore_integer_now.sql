-- ============================================================================
-- Case:  00040119
-- Service / project: p3dthsrulv / gbvx1g57lj
--
-- Repro: columnstore policy on continuous aggregates fails with
--        "integer_now function not set"
--
-- Setup (mirrors the customer's environment):
--   - Base hypertable `sqlth_34_data` chunked by an int8 epoch-ms column
--     (`t_stamp`), with a user-defined `now_ms()` set as the
--     `integer_now_func` on the base hypertable.
--   - Two stacked continuous aggregates: `tag_values_1s_agg` on the base,
--     and `tag_values_1m_agg` on the inner cagg.
--   - Columnstore enabled + columnstore policy on the base and both caggs.
--
-- Expected behavior with the bug:
--   - The base hypertable's columnstore policy job SUCCEEDS.
--   - Both cagg columnstore policy jobs FAIL with
--       ERROR: integer_now function not set
--   - Manual `convert_to_columnstore(<cagg chunk>)` SUCCEEDS.
--
-- Root cause (timescaledb tsl/src/bgw_policy/job.c policy_recompression_execute):
--   The policy looks up the open dimension on the cagg's materialization
--   hypertable directly. The mat hypertable's dimension has empty
--   integer_now_func / integer_now_func_schema (only the original base
--   hypertable has those populated), so ts_get_integer_now_func() raises.
--   The retention policy path does it correctly via
--   ts_continuous_agg_find_integer_now_func_by_materialization_id() — the
--   columnstore/recompression path does not.
--
-- Workaround demonstrated at the bottom of this script:
--   set_integer_now_func() on the materialization hypertable directly.
-- ============================================================================

\set ON_ERROR_STOP off
\timing off
\pset pager off

-- ---------------------------------------------------------------------------
-- 0. Clean prior repro state (idempotent).
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS tag_values_1m_agg CASCADE;
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
-- 3. Insert 30 days of synthetic data so we have many chunks, most of which
--    are older than the compress_after window (7 days).
-- ---------------------------------------------------------------------------
INSERT INTO sqlth_34_data (tagid, intvalue, t_stamp)
SELECT
    (random() * 10)::INT,
    (random() * 1000)::BIGINT,
    (EXTRACT(epoch FROM now() - (g || ' minutes')::interval) * 1000)::BIGINT
FROM generate_series(1, 30 * 24 * 60) g;

SELECT count(*) AS row_count FROM sqlth_34_data;
SELECT count(*) AS chunk_count FROM show_chunks('sqlth_34_data');

-- ---------------------------------------------------------------------------
-- 4. Enable columnstore + add columnstore policy on the base hypertable.
--    compress_after = 7 days in ms.
-- ---------------------------------------------------------------------------
ALTER TABLE sqlth_34_data SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tagid'
);

-- ---------------------------------------------------------------------------
-- 5. Stacked continuous aggregates.
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

CREATE MATERIALIZED VIEW tag_values_1m_agg
WITH (timescaledb.continuous) AS
SELECT
    tagid,
    time_bucket(3600000, bucket) AS bucket,   -- 1-hour buckets (ms)
    avg(avg_v) AS avg_v
FROM tag_values_1s_agg
GROUP BY tagid, bucket
WITH NO DATA;

CALL refresh_continuous_aggregate('tag_values_1m_agg', NULL, NULL);

-- ---------------------------------------------------------------------------
-- 6. Enable columnstore on both caggs.
-- ---------------------------------------------------------------------------
ALTER MATERIALIZED VIEW tag_values_1s_agg SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tagid'
);
ALTER MATERIALIZED VIEW tag_values_1m_agg SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tagid'
);

-- ---------------------------------------------------------------------------
-- 7. Register columnstore policies. Capture the resulting job ids.
--
-- Note: if your TimescaleDB version predates the columnstore rename, use
--       add_compression_policy() with the same signature.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE _jobs (label TEXT PRIMARY KEY, job_id INTEGER);

INSERT INTO _jobs VALUES
    ('base', add_columnstore_policy('sqlth_34_data',     (7 * 86400000)::BIGINT)),
    ('1s',   add_columnstore_policy('tag_values_1s_agg', (7 * 86400000)::BIGINT)),
    ('1m',   add_columnstore_policy('tag_values_1m_agg', (7 * 86400000)::BIGINT));

SELECT * FROM _jobs ORDER BY label;

-- ---------------------------------------------------------------------------
-- 8. Inspect the catalog: integer_now_func lives ONLY on the base
--    hypertable's dimension. The cagg materialization hypertables have it
--    empty — which is the trigger for the bug.
-- ---------------------------------------------------------------------------
SELECT
    h.table_name,
    d.column_name,
    d.column_type,
    coalesce(d.integer_now_func_schema, '<null>') AS integer_now_schema,
    coalesce(d.integer_now_func,        '<null>') AS integer_now_func
FROM _timescaledb_catalog.hypertable h
JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id
WHERE h.table_name = 'sqlth_34_data'
   OR h.id IN (
        SELECT mat_hypertable_id
        FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name IN ('tag_values_1s_agg', 'tag_values_1m_agg')
   )
ORDER BY h.table_name;

-- ---------------------------------------------------------------------------
-- 9. Reproduce: run the columnstore policy jobs and observe behavior.
--    Base succeeds, both caggs fail with "integer_now function not set".
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    j_base INT;
    j_1s   INT;
    j_1m   INT;
BEGIN
    SELECT job_id INTO j_base FROM _jobs WHERE label = 'base';
    SELECT job_id INTO j_1s   FROM _jobs WHERE label = '1s';
    SELECT job_id INTO j_1m   FROM _jobs WHERE label = '1m';

    RAISE NOTICE '--- running base hypertable columnstore policy (job %) ---', j_base;
    BEGIN
        CALL run_job(j_base);
        RAISE NOTICE 'PASS: base policy succeeded';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'UNEXPECTED: base policy failed: %', SQLERRM;
    END;

    RAISE NOTICE '--- running 1s cagg columnstore policy (job %) ---', j_1s;
    BEGIN
        CALL run_job(j_1s);
        RAISE NOTICE 'UNEXPECTED: 1s cagg policy succeeded (bug not reproduced)';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'REPRODUCED: 1s cagg policy failed: %', SQLERRM;
    END;

    RAISE NOTICE '--- running 1m cagg columnstore policy (job %) ---', j_1m;
    BEGIN
        CALL run_job(j_1m);
        RAISE NOTICE 'UNEXPECTED: 1m cagg policy succeeded (bug not reproduced)';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'REPRODUCED: 1m cagg policy failed: %', SQLERRM;
    END;
END $$;

-- ---------------------------------------------------------------------------
-- 10. Show that the *manual* path works on a cagg chunk even though the
--     scheduled policy failed.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    mat_ht_name TEXT;
    chunk_oid   REGCLASS;
BEGIN
    SELECT format('_timescaledb_internal._materialized_hypertable_%s', mat_hypertable_id)
      INTO mat_ht_name
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'tag_values_1s_agg';

    SELECT format('%I.%I', chunk_schema, chunk_name)::regclass
      INTO chunk_oid
    FROM timescaledb_information.chunks
    WHERE format('%I.%I', hypertable_schema, hypertable_name) = mat_ht_name
      AND NOT is_compressed
    LIMIT 1;

    IF chunk_oid IS NULL THEN
        RAISE NOTICE 'No uncompressed chunks on % — skipping manual convert test', mat_ht_name;
        RETURN;
    END IF;

    RAISE NOTICE 'Manually converting % to columnstore...', chunk_oid;
    BEGIN
        PERFORM convert_to_columnstore(chunk_oid);
        RAISE NOTICE 'PASS: manual convert_to_columnstore(%) succeeded', chunk_oid;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'FAIL: manual convert_to_columnstore failed: %', SQLERRM;
    END;
END $$;

-- ===========================================================================
-- 11. WORKAROUND: set integer_now_func on each cagg's materialization
--     hypertable directly. After this, the scheduled cagg policies succeed.
-- ===========================================================================
DO $$
DECLARE
    mat_1s TEXT;
    mat_1m TEXT;
BEGIN
    SELECT format('_timescaledb_internal._materialized_hypertable_%s', mat_hypertable_id)
      INTO mat_1s
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'tag_values_1s_agg';

    SELECT format('_timescaledb_internal._materialized_hypertable_%s', mat_hypertable_id)
      INTO mat_1m
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'tag_values_1m_agg';

    EXECUTE format($f$ SELECT set_integer_now_func(%L::regclass, 'now_ms') $f$, mat_1s);
    EXECUTE format($f$ SELECT set_integer_now_func(%L::regclass, 'now_ms') $f$, mat_1m);

    RAISE NOTICE 'Applied workaround: integer_now_func set on % and %', mat_1s, mat_1m;
END $$;

-- Re-run the cagg policies after the workaround. They should now succeed.
DO $$
DECLARE
    j_1s INT;
    j_1m INT;
BEGIN
    SELECT job_id INTO j_1s FROM _jobs WHERE label = '1s';
    SELECT job_id INTO j_1m FROM _jobs WHERE label = '1m';

    BEGIN
        CALL run_job(j_1s);
        RAISE NOTICE 'POST-WORKAROUND: 1s cagg policy succeeded';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'POST-WORKAROUND: 1s cagg policy still failing: %', SQLERRM;
    END;

    BEGIN
        CALL run_job(j_1m);
        RAISE NOTICE 'POST-WORKAROUND: 1m cagg policy succeeded';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'POST-WORKAROUND: 1m cagg policy still failing: %', SQLERRM;
    END;
END $$;

-- Final state: count how many chunks ended up in columnstore on each ht.
SELECT
    hypertable_name,
    count(*) FILTER (WHERE is_compressed)     AS columnstore_chunks,
    count(*) FILTER (WHERE NOT is_compressed) AS rowstore_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sqlth_34_data'
   OR hypertable_name LIKE '\_materialized\_hypertable\_%' ESCAPE '\'
GROUP BY hypertable_name
ORDER BY hypertable_name;
