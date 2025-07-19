# Schema of the original data
| Column Name             | Data Type | Description                                                     |
| ----------------------- | --------- | --------------------------------------------------------------- |
| `vendorid`              | integer   | Code indicating the provider (1 = Creative, 2 = VeriFone, etc.) |
| `tpep_pickup_datetime`  | timestamp | Trip start timestamp                                            |
| `tpep_dropoff_datetime` | timestamp | Trip end timestamp                                              |
| `passenger_count`       | integer   | Number of passengers                                            |
| `trip_distance`         | double    | Distance of the trip in miles                                   |
| `ratecodeid`            | integer   | Rate code (1 = standard, 2 = JFK, etc.)                         |
| `store_and_fwd_flag`    | string    | Whether the trip record was stored and forwarded (Y/N)          |
| `pulocationid`          | integer   | Pickup location ID (Zone ID from Taxi Zone Lookup Table)        |
| `dolocationid`          | integer   | Dropoff location ID (Zone ID from Taxi Zone Lookup Table)       |
| `payment_type`          | integer   | Type of payment (1 = credit, 2 = cash, etc.)                    |
| `fare_amount`           | double    | Fare charged for the trip                                       |
| `extra`                 | double    | Extra charges (e.g., night surcharge)                           |
| `mta_tax`               | double    | MTA tax (\$0.50)                                                |
| `tip_amount`            | double    | Tip amount (zero for cash trips)                                |
| `tolls_amount`          | double    | Tolls paid                                                      |
| `improvement_surcharge` | double    | \$0.30 improvement surcharge                                    |
| `total_amount`          | double    | Total charged to the passenger                                  |
| `congestion_surcharge`  | double    | Congestion surcharge (e.g., \$2.50 in Manhattan)                |
