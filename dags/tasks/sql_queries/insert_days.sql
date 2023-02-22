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
DO NOTHING;