SELECT * FROM interior LIMIT 10;

-- @block
SELECT * FROM call_for_heat LIMIT 10;

-- @block
SELECT 
    -- date_trunc('minute', timestamp) AS time_truncated
    *
    CASE 
FROM interior
WHERE zone_id = 6
-- GROUP BY 1
LIMIT 10;


-- @block rev 1
WITH t0 as (
    SELECT timestamp
    FROM interior
) 
t1 as (
    SELECT
        *,
        now() - '2 days 8 hours'::interval as my_time
    FROM call_for_heat
),
t2 as (
    SELECT
        *,
        CASE
            WHEN my_time > t_start AND my_time < t_end THEN true
            ELSE false
        END as in_interval
    FROM t1
),
t3 as (
    SELECT
        t_start,
        t_end,
        EXTRACT (EPOCH FROM (t_end - t_start)::interval)::int/60 as duration_min,
        my_time,
        in_interval,
        value,
        zone_id
    FROM
        t2
    WHERE
        in_interval = true
    ORDER BY t_start DESC
)
SELECT *
FROM interior
LIMIT 100



-- @block rev 2
WITH t0 as (
    SELECT *
    FROM interior
), 
t2 as (
    SELECT
        *,
        CASE
            WHEN t0.timestamp > t1.t_start AND t0.timestamp < t1.t_end AND t2.value = true THEN true
            ELSE false
        END as in_interval
    FROM call_for_heat as t1
    RIGHT JOIN t0
        ON t0.timestamp = t1.t_start
)
SELECT *
FROM t2
-- WHERE in_interval = true
ORDER BY timestamp DESC
LIMIT 100


-- @block rev 5
WITH t_low as (
    SELECT 
        *,
        CASE
            WHEN (
                SELECT * FROM (
                    SELECT
                        CASE
                            WHEN  i.timestamp > c1.t_start AND i.timestamp < c1.t_end AND c1.value = 'LOW' THEN true
                            ELSE false
                        END as in_range_inner
                    FROM call_for_heat as c1
                ) as c2
                WHERE in_range_inner = true
                ORDER BY i.timestamp ASC
                LIMIT 1
            ) = true THEN 'LOW'
            ELSE NULL
        END as in_range_outer
    FROM interior as i
),
t_med as (
    SELECT 
        *,
        CASE
            WHEN (
                SELECT * FROM (
                    SELECT
                        CASE
                            WHEN  i.timestamp > c1.t_start AND i.timestamp < c1.t_end AND c1.value = 'MEDIUM' THEN true
                            ELSE false
                        END as in_range_inner
                    FROM call_for_heat as c1
                ) as c2
                WHERE in_range_inner = true
                ORDER BY i.timestamp ASC
                LIMIT 1
            ) = true THEN 'MEDIUM'
            ELSE NULL
        END as in_range_outer
    FROM interior as i
),
t_hi as (
    SELECT 
        *,
        CASE
            WHEN (
                SELECT * FROM (
                    SELECT
                        CASE
                            WHEN  i.timestamp > c1.t_start AND i.timestamp < c1.t_end AND c1.value = 'HIGH' THEN true
                            ELSE false
                        END as in_range_inner
                    FROM call_for_heat as c1
                ) as c2
                WHERE in_range_inner = true
                ORDER BY i.timestamp ASC
                LIMIT 1
            ) = true THEN 'HIGH'
            ELSE NULL
        END as in_range_outer
    FROM interior as i
)
SELECT 
    lo.*,
    coalesce(
        lo.in_range_outer, 
        med.in_range_outer, 
        hi.in_range_outer, 
        'NONE'
    ) as heating_demand
FROM t_low as lo
INNER JOIN t_med as med
    ON lo.extracted_date = med.extracted_date
    AND lo.timestamp = med.timestamp
    AND lo.zone_id = med.zone_id
INNER JOIN t_hi as hi
    ON lo.extracted_date = hi.extracted_date
    AND lo.timestamp = hi.timestamp
    AND lo.zone_id = hi.zone_id
WHERE lo.zone_id = 6
ORDER BY timestamp DESC
LIMIT 50


-- @block rev 5
WITH t0 as (
    SELECT 
        i.timestamp,
        i.zone_id,
        i.humidity,
        i.temperature,
        CASE
            WHEN (
                SELECT * FROM (
                    SELECT
                        CASE
                            WHEN  i.timestamp > c1.t_start AND i.timestamp < c1.t_end AND c1.value = 'NONE' THEN true
                            ELSE false
                        END as in_range_inner
                    FROM call_for_heat as c1
                ) as c2
                WHERE in_range_inner = true
                ORDER BY i.timestamp ASC
                LIMIT 1
            ) = true THEN 'in_range'
            ELSE 'not_in_range'
        END as in_range_outer
    FROM interior as i
)
SELECT *
FROM t0
WHERE zone_id = 6
ORDER BY timestamp DESC
LIMIT 50












-- @block
SELECT 
    *
FROM interior
WHERE zone_id = 6
LIMIT 10;



-- @block  Attempted resampling.  Not quite what I wanted
WITH q as (
    SELECT *
    FROM interior
)
SELECT *
FROM (
    SELECT generate_series(min(q.timestamp), max(q.timestamp), '1 minute'::interval) as dte
    FROM q
) as s
LEFT OUTER JOIN q ON s.dte = q.timestamp
LIMIT 10


-- @block
WITH q as (
    SELECT *
    FROM interior
)
SELECT *
FROM (
    SELECT generate_series(min(q.timestamp), max(q.timestamp), '1 minute'::interval) as dte
    FROM q
) as s



