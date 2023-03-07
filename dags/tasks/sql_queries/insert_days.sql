INSERT INTO days (
    hours_in_day,
    zone_type,
    t_start,
    t_end,
    zone_id,
    zone_name,
    extracted_date,
    historic_data,
    zone_metadata
)
VALUES (:hours_in_day,:zone_type,:t_start,:t_end,:zone_id,:zone_name,:extracted_date,:historic_data,:zone_metadata)
ON CONFLICT ON CONSTRAINT day_pkey
DO UPDATE SET
    hours_in_day = EXCLUDED.hours_in_day,
    zone_type = EXCLUDED.zone_type,
    t_start = EXCLUDED.t_start,
    t_end = EXCLUDED.t_end,
    zone_name = EXCLUDED.zone_name,
    historic_data = EXCLUDED.historic_data,
    zone_metadata = EXCLUDED.zone_metadata
;