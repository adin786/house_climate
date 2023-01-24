CREATE TABLE IF NOT EXISTS days (
    extracted_date DATE NOT NULL,
    t_start TIMESTAMPTZ NOT NULL,
    t_end TIMESTAMPTZ NOT NULL,
    zone_id SMALLINT NOT NULL,
    zone_type VARCHAR(50) NOT NULL,
    hours_in_day SMALLINT NOT NULL,
    json_data json NOT NULL
);

CREATE TABLE IF NOT EXISTS interior (
    extracted_date DATE NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    zone_id SMALLINT NOT NULL,
    humidity DECIMAL(4,3) NOT NULL,
    humidity_unit VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    temperature_unit VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS weather (
    extracted_date DATE NOT NULL,
    t_start TIMESTAMPTZ NOT NULL,
    t_end TIMESTAMPTZ NOT NULL,
    zone_id SMALLINT NOT NULL,
    state VARCHAR(50) NOT NULL,
    temp_celsius DECIMAL(5,2) NOT NULL
);
