CREATE TABLE IF NOT EXISTS divvy_rides(
    ride_id VARCHAR(50) NOT NULL,
    rideable_type VARCHAR(30),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(100),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(100),
    start_lat DOUBLE PRECISION,
    start_lng DOUBLE PRECISION,
    end_lat DOUBLE PRECISION,
    end_lng DOUBLE PRECISION,
    member_casual VARCHAR(30)
);


COPY divvy_rides
FROM  '{{ti.xcom_pull(task_ids=['divvy_download'], key='filename_link')[0]['saved_filename']}}' --Note: must be absolute path
DELIMITER ','
CSV HEADER;