-- V1 SimpleScheduler does not currently use a delta query, so this index
-- is forward-looking: it enables the v1.1 optimization where the scheduler
-- maintains an in-memory WAITING-frame cache refreshed by
--   SELECT ... FROM frame WHERE ts_updated > :last_seen_ts
-- per tick. With this index the delta scan returns ~100 rows in single-digit
-- ms even on a 100M-row frame table.

CREATE INDEX IF NOT EXISTS i_frame_ts_updated
    ON frame (ts_updated);
