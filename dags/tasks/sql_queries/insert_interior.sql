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
DO NOTHING;