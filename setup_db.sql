CREATE TABLE temp_readings (
    id SERIAL PRIMARY KEY,
    unique_id VARCHAR(200),
    room_id VARCHAR(50),
    noted_date TIMESTAMP,
    temp FLOAT,
    in_out VARCHAR(10)
);
