# Schema of the transformed DataFrame
| Column Name            | Type      | Description                                              |
| ---------------------- | --------- | -------------------------------------------------------- |
| `hour_of_day`          | integer   | Hour at which the trip started (0–23)                    |
| `date`                 | date      | Trip start date                                          |
| `driver_id`            | integer   | Synthetic ID representing the driver                     |
| `start_time`           | timestamp | Trip start time                                          |
| `end_time`             | timestamp | Trip end time                                            |
| `passenger_count`      | long      | Number of passengers in the trip                         |
| `distance_miles`       | double    | Trip distance in miles                                   |
| `fare`                 | double    | Fare charged for the trip                                |
| `tip`                  | double    | Tip amount given                                         |
| `pickup_borough`       | string    | NYC borough of pickup location                           |
| `pickup_zone`          | string    | Specific pickup zone name                                |
| `pickup_service_zone`  | string    | Service zone for pickup (e.g., `Boro`, `Yellow Zone`)    |
| `dropoff_borough`      | string    | NYC borough of dropoff location                          |
| `dropoff_zone`         | string    | Specific dropoff zone name                               |
| `dropoff_service_zone` | string    | Service zone for dropoff                                 |
| `trip_duration_min`    | double    | Duration of the trip in minutes                          |
| `day_of_week`          | integer   | Day of the week (0=Monday, 6=Sunday)                     |
| `month`                | integer   | Month of the trip (1–12)                                 |
| `speed_mph`            | double    | Estimated average speed in miles/hour                    |
| `temperature`          | double    | Temperature at trip start (°F or °C)                     |
| `precipitation`        | double    | Precipitation level                                      |
| `windspeed`            | double    | Wind speed during the trip                               |
| `other_fees`           | double    | Additional surcharges or fees                            |
| `is_weekend`           | integer   | 1 if weekend, else 0                                     |
| `is_night`             | integer   | 1 if trip occurred at night (e.g., 10PM–5AM)             |
| `is_rush_hour`         | integer   | 1 if trip occurred during peak rush hours                |
| `tip_percent`          | double    | Tip as a percentage of fare                              |
| `fare_per_mile`        | double    | Fare divided by miles traveled                           |
| `tipped`               | integer   | 1 if tip > 0, else 0                                     |
| `fare_per_min`         | double    | Fare divided by trip duration                            |
| `is_raining`           | integer   | 1 if precipitation > threshold, else 0                   |
| `bad_weather`          | integer   | 1 if weather conditions are poor (rain/snow + wind)      |
| `route`                | string    | Concatenated route name: `"pickup_zone -> dropoff_zone"` |
