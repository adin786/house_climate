INSERT INTO call_for_heat (
    extracted_date,
    zone_id,
    t_start,
    t_end,
    value
)
VALUES (:extracted_date,:zone_id,:t_start,:t_end,:value)
ON CONFLICT ON CONSTRAINT cal_unique
DO UPDATE SET
    value = EXCLUDED.value
;