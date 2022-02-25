/**
 * Gaps and islands: Merging contiguous ranges
 */

-- Create the table and index to store our ranges
CREATE TABLE ranges (
  set_id integer NOT NULL,
  from_id bigint NOT NULL,
  to_id bigint NOT NULL,
  EXCLUDE USING GIST (
    set_id WITH =,
    int8range(from_id, to_id, '[]') WITH &&
  )
);
CREATE INDEX index_ranges_on_set_id ON ranges USING btree (set_id);


-- Insert some data
INSERT INTO ranges VALUES (1, 1, 10);
INSERT INTO ranges VALUES (1, 11, 15);
INSERT INTO ranges VALUES (1, 16, 20);
INSERT INTO ranges VALUES (1, 25, 30);
INSERT INTO ranges VALUES (1, 31, 40);
INSERT INTO ranges VALUES (1, 45, 50);
INSERT INTO ranges VALUES (1, 55, 60);
INSERT INTO ranges VALUES (1, 61, 80);


-- Check the inserted ranges
SELECT * FROM ranges
WHERE set_id = 1
ORDER BY from_id;


-- Merge the ranges
WITH locked_ranges AS (
  SELECT * FROM ranges WHERE set_id = 1 FOR UPDATE
),
range_islands AS (
  SELECT
    *,
    CASE from_id - LAG(locked_ranges.to_id)
        OVER (PARTITION BY set_id ORDER BY locked_ranges.from_id ASC)
      WHEN NULL THEN 1
      WHEN 1 THEN 0
      ELSE 1
    END AS island_start
  FROM locked_ranges
),
range_island_ids AS (
  SELECT
    *,
    SUM(range_islands.island_start)
      OVER (PARTITION BY set_id ORDER BY range_islands.from_id ASC) AS island_id
  FROM range_islands
),
compacted_ranges AS (
  SELECT
    set_id,
    MIN(from_id) AS from_id,
    MAX(to_id) AS to_id
  FROM range_island_ids
  GROUP BY set_id, island_id
),
delete_gaps AS (
  DELETE FROM ranges
  USING range_islands
  WHERE
    ranges.set_id = range_islands.set_id AND
    ranges.from_id = range_islands.from_id AND
    range_islands.island_start = 0
  RETURNING *
)
UPDATE ranges SET
  to_id = compacted_ranges.to_id
FROM compacted_ranges
WHERE
  ranges.set_id = compacted_ranges.set_id AND
  ranges.from_id = compacted_ranges.from_id AND
  -- This condition ensures delete_gaps completes before executing the update.
  (SELECT COUNT(*) FROM delete_gaps) >= 0;


-- Check the result
SELECT * FROM ranges
WHERE set_id = 1
ORDER BY from_id;
