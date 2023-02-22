CREATE TABLE IF NOT EXISTS days (
    extracted_date DATE NOT NULL,
    t_start TIMESTAMPTZ NOT NULL,
    t_end TIMESTAMPTZ NOT NULL,
    zone_id SMALLINT NOT NULL,
    zone_type VARCHAR(50) NOT NULL,
    zone_name VARCHAR(50) NOT NULL,
    hours_in_day SMALLINT NOT NULL,
    historic_data json NOT NULL,
    zone_metadata json NOT NULL,
    CONSTRAINT day_pkey PRIMARY KEY (extracted_date, zone_id)
);

CREATE TABLE IF NOT EXISTS interior (
    extracted_date DATE NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    zone_id SMALLINT NOT NULL,
    humidity DECIMAL(4,3) NOT NULL,
    humidity_unit VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    temperature_unit VARCHAR(50) NOT NULL,
    CONSTRAINT int_fkey FOREIGN KEY (extracted_date, zone_id) REFERENCES days (extracted_date, zone_id),
    CONSTRAINT int_unique UNIQUE (extracted_date, zone_id, timestamp)
    -- CONSTRAINT interior_fkey FOREIGN KEY (extracted_date, zone_id) REFERENCES days (extracted_date, zone_id)
);

CREATE TABLE IF NOT EXISTS weather (
    extracted_date DATE NOT NULL,
    t_start TIMESTAMPTZ NOT NULL,
    t_end TIMESTAMPTZ NOT NULL,
    zone_id SMALLINT NOT NULL,
    state VARCHAR(50) NOT NULL,
    temp_celsius DECIMAL(5,2) NOT NULL,
    CONSTRAINT wea_fkey FOREIGN KEY (extracted_date, zone_id) REFERENCES days (extracted_date, zone_id),
    CONSTRAINT wea_unique UNIQUE (extracted_date, zone_id, t_start, t_end)
    -- CONSTRAINT weather_fkey FOREIGN KEY (extracted_date, zone_id) REFERENCES days (extracted_date, zone_id)
);

CREATE TABLE IF NOT EXISTS call_for_heat (
    extracted_date DATE NOT NULL,
    zone_id SMALLINT NOT NULL,
    t_start TIMESTAMPTZ NOT NULL,
    t_end TIMESTAMPTZ NOT NULL,
    value VARCHAR(50) NOT NULL,
    CONSTRAINT cal_fkey FOREIGN KEY (extracted_date, zone_id) REFERENCES days (extracted_date, zone_id),
    CONSTRAINT cal_unique UNIQUE (extracted_date, zone_id, t_start, t_end)
    -- CONSTRAINT call_for_heat_fkey FOREIGN KEY (extracted_date, zone_id) REFERENCES days (extracted_date, zone_id)
);
