INSERT INTO weather (
    t_start,
    t_end,
    state,
    temp_celsius,
    zone_id,
    extracted_date
)
VALUES (:t_start,:t_end,:state,:temp_celsius,:zone_id,:extracted_date)
ON CONFLICT ON CONSTRAINT wea_unique
DO UPDATE SET
    state = EXCLUDED.state, 
    temp_celsius = EXCLUDED.temp_celsius
;