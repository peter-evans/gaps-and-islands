# Gaps and islands: Merging contiguous ranges

For part of a system I was designing and implementing recently, I needed a solution to merge rows of contiguous ranges in a PostgreSQL table.
The approach I took was based on solutions to the [gaps and islands](https://www.red-gate.com/simple-talk/databases/sql-server/t-sql-programming-sql-server/gaps-islands-sql-server-data/) problem.

There are many ways to solve this by fetching the rows and processing the merge outside of PostgreSQL.
However, I specifically wanted to do this in pure SQL so that the operation could safely modify rows in a transaction, and avoid race conditions with concurrent operations.

Unlike many of the more straightforward examples I found for solutions to the gaps and islands problem, my particular use case required the following.
- Find the gaps and islands between rows containing a numerical range, expressed as two columns, `from_id` and `to_id`.
- Merge the islands (rows of contiguous ranges) into a single row.
- Update the table with the merged islands.

### Solution

This is the table we'll use for the following examples.
`set_id` is a set of ranges, and the merge operation targets a specific set.
The `EXCLUDE` constraint is added to prevent overlapping ranges from being inserted into the table.

```sql
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
```

#### Identify islands

Identifying islands is done in two steps.
The first step adds a column which marks the start of an island.

```sql
SELECT
  *,
  CASE from_id - LAG(ranges.to_id)
      OVER (PARTITION BY set_id ORDER BY ranges.from_id ASC)
    WHEN NULL THEN 1
    WHEN 1 THEN 0
    ELSE 1
  END AS island_start
FROM ranges
WHERE set_id = 1
```

The query uses the `LAG` [window function](https://www.postgresql.org/docs/current/functions-window.html) to evaluate the previous row, and determine if the current row is the start of an island or not. Take care to handle `NULL` for the first row.

Here is an example result, showing the start of four islands has been marked.

| set_id | from_id | to_id | island_start |
| ------ | ------- | ----- | ------------ |
|      1 |       1 |    10 |            1 |
|      1 |      11 |    15 |            0 |
|      1 |      16 |    20 |            0 |
|      1 |      25 |    30 |            1 |
|      1 |      31 |    40 |            0 |
|      1 |      45 |    50 |            1 |
|      1 |      55 |    60 |            1 |
|      1 |      61 |    80 |            0 |

The next step is to give each island a unique ID, so that we can identify what island each row belongs to.

```sql
WITH range_islands AS (
  SELECT
    *,
    CASE from_id - LAG(ranges.to_id)
        OVER (PARTITION BY set_id ORDER BY ranges.from_id ASC)
      WHEN NULL THEN 1
      WHEN 1 THEN 0
      ELSE 1
    END AS island_start
  FROM ranges
  WHERE set_id = 1
)
SELECT
  *,
  SUM(range_islands.island_start)
    OVER (PARTITION BY set_id ORDER BY range_islands.from_id ASC) AS island_id
FROM range_islands
```

The query uses `SUM` as a windowed function over the `island_start` column in the result of our previous query.
This creates a rolling sum, where each island start increases the sum by one, giving us a unique ID.

Here is an example result, showing four islands with their unique ID.

| set_id | from_id | to_id | island_start | island_id |
| ------ | ------- | ----- | ------------ | --------- |
|      1 |       1 |    10 |            1 |         1 |
|      1 |      11 |    15 |            0 |         1 |
|      1 |      16 |    20 |            0 |         1 |
|      1 |      25 |    30 |            1 |         2 |
|      1 |      31 |    40 |            0 |         2 |
|      1 |      45 |    50 |            1 |         3 |
|      1 |      55 |    60 |            1 |         4 |
|      1 |      61 |    80 |            0 |         4 |

#### Merge islands

Once each row has an ID, identifying what island it belongs to, the next step is straightforward.
We group by `island_id` and find the `MIN` and `MAX` of the contiguous ranges.

```sql
WITH range_islands AS (
  SELECT
    *,
    CASE from_id - LAG(ranges.to_id)
        OVER (PARTITION BY set_id ORDER BY ranges.from_id ASC)
      WHEN NULL THEN 1
      WHEN 1 THEN 0
      ELSE 1
    END AS island_start
  FROM ranges
  WHERE set_id = 1
),
range_island_ids AS (
  SELECT
    *,
    SUM(range_islands.island_start)
      OVER (PARTITION BY set_id ORDER BY range_islands.from_id ASC) AS island_id
  FROM range_islands
)
SELECT
  set_id,
  MIN(from_id) AS from_id,
  MAX(to_id) AS to_id
FROM range_island_ids
GROUP BY set_id, island_id
```

Here is the result, showing the merged islands.

| set_id | from_id | to_id |
| ------ | ------- | ----- |
|      1 |       1 |    20 |
|      1 |      25 |    40 |
|      1 |      45 |    50 |
|      1 |      55 |    80 |

#### Update islands

