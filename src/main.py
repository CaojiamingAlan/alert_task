from datetime import datetime, timedelta

import sqlalchemy as sa

people = ["pedestrian", "bicycle"]
vehicles = ["car", "truck", "van"]


def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
    conn = engine.connect()
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS detections "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


person_start_time = datetime.utcfromtimestamp(0)
person_end_time = datetime.utcfromtimestamp(0)
counter = 0


def alert(timestamp_string: str, detection_type: str):
    global person_start_time, person_end_time, counter
    if detection_type in people:
        timestamp = datetime.strptime(timestamp_string, "%Y-%m-%dT%H:%M:%S")
        if timestamp - person_end_time > timedelta(0, 30):
            person_start_time = timestamp
            counter = 1
        elif timestamp == person_end_time:
            return
        else:
            counter += 1
        person_end_time = timestamp

    if counter >= 5:
        print(f"A person is detected in >=5 consecutive intervals, from {person_start_time} to {person_end_time}")


def ingest_data(conn: sa.Connection, timestamp_string: str, detection_type: str):
    alert(timestamp_string, detection_type)
    try:
        conn.execute(
            sa.text("INSERT INTO detections (time, type) VALUES (:timestamp, :detection_type)"),
            {"timestamp": timestamp_string, "detection_type": detection_type}
        )
        print("Data ingested successfully.")
    except Exception as e:
        print(f"Error ingesting data: {e}")


def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    try:
        query = """
            WITH detections_people_vehicles AS (
                SELECT 
                    time,
                    CASE
                        WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
                        ELSE 'vehicles'
                    END AS type
                FROM detections
            ), 
            detections_people_vehicles_prev_time AS (
                SELECT 
                    type,
                    time,
                    LAG(time) OVER (PARTITION BY type ORDER BY time) AS prev_time
                FROM detections_people_vehicles
                UNION
                SELECT type,
                       MAX(time) + interval '1 minute',
                       MAX(time) AS prev_time
                FROM detections_people_vehicles    
                GROUP BY type     
            ), 
            detections_people_vehicles_start_time_end_time AS (
                SELECT 
                    type,
                    time AS start_time,
                    LAG(prev_time) OVER (PARTITION BY type ORDER BY time desc) AS end_time
                FROM detections_people_vehicles_prev_time
                WHERE prev_time IS NULL OR time - prev_time >= interval '1 minute'
                GROUP BY type, time, prev_time
                ORDER BY start_time
            )
            SELECT 
                type,
                TO_CHAR(start_time, 'YYYY-MM-DD"T"HH24:MI:SS') AS start_time,
                TO_CHAR(end_time, 'YYYY-MM-DD"T"HH24:MI:SS') AS end_time
            FROM detections_people_vehicles_start_time_end_time
            WHERE end_time IS NOT NULL
        """

        result = conn.execute(sa.text(query))

        people_periods, vehicle_periods = [], []
        for detection_type, start_time, end_time in result:
            if detection_type == "people":
                people_periods.append((start_time, end_time))
            else:
                vehicle_periods.append((start_time, end_time))

        return {
            "people": people_periods,
            "vehicles": vehicle_periods,
        }
    except Exception as e:
        print(f"Error aggregating detections: {e}")
        return {"people": [], "vehicles": []}


def main():
    conn = database_connection()

    # Simulate real-time detections every 30 seconds
    detections = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    for timestamp, detection_type in detections:
        ingest_data(conn, timestamp, detection_type)

    aggregate_results = aggregate_detections(conn)
    print(aggregate_results)


if __name__ == "__main__":
    main()
