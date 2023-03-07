INSERT INTO interior (
  timestamp, 
  humidity, 
  humidity_unit, 
  temperature, 
  temperature_unit, 
  zone_id, 
  extracted_date
)
VALUES (:timestamp,:humidity,:humidity_unit,:temperature,:temperature_unit,:zone_id,:extracted_date)
ON CONFLICT ON CONSTRAINT int_unique
DO UPDATE SET 
  humidity = EXCLUDED.humidity,
  humidity_unit = EXCLUDED.humidity_unit,
  temperature = EXCLUDED.temperature,
  temperature_unit = EXCLUDED.temperature_unit
;