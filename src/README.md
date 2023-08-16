Solution description:

### ingest_data
This is pretty straight forward. If the data volume is huge we may want to do connection pooling and batch processing.

### aggregate_detections
The may complexity is in the SQL query. Let me explain each subquery one by one:

```
            WITH detections_people_vehicles AS (
                SELECT 
                    time,
                    CASE
                        WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
                        ELSE 'vehicles'
                    END AS type
                FROM detections
            ), 
```
This subquery maps types to people and vehicles.

```
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
```
This subquery uses the LAG window function to find the timestamp of the previous datapoint for each row. We also add a datapoint at the end of each type in the UNION query for future simplicity.

```
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
```
This subquery filters out the datapoint whose gap with the previous one >= 1 min. These are the start_time of each interval. However, we also need the information of the end_time of each interval. We can get this timestamp from the next interval's `prev_time`, therefore we use `LAG` again, but in reverse order. We add the 'fake' datapoint in the previous query, so that we can keep the end_time for the last interval. 
```
            SELECT 
                type,
                TO_CHAR(start_time, 'YYYY-MM-DD"T"HH24:MI:SS') AS start_time,
                TO_CHAR(end_time, 'YYYY-MM-DD"T"HH24:MI:SS') AS end_time
            FROM detections_people_vehicles_start_time_end_time
            WHERE end_time IS NOT NULL
```
This query filters the previously added `fake` datapoint and format the time.

### alert
Use a counter to count the number of consecutive intervals. Refresh it if there is a gap.  

### Additional Notes
The docker-compose fails sometime, since it is possible that the service runs before the database is ready. Therefore, I also add an `restart: on-failure` to the yaml file so that it retries when the service fails.