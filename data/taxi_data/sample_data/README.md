# Schema of the original data
| Column Name             | Type      | Description                                                           |
| ----------------------- | --------- | --------------------------------------------------------------------- |
| `VendorID`              | integer   | Code indicating the provider (1 or 2)                                 |
| `tpep_pickup_datetime`  | timestamp | Timestamp of when the trip began                                      |
| `tpep_dropoff_datetime` | timestamp | Timestamp of when the trip ended                                      |
| `passenger_count`       | long      | Number of passengers                                                  |
| `trip_distance`         | double    | Distance of the trip in miles                                         |
| `RatecodeID`            | long      | Code representing the rate type                                       |
| `store_and_fwd_flag`    | string    | ‘Y’ if the trip data was stored and forwarded to the vendor, else ‘N’ |
| `PULocationID`          | integer   | Location ID where the trip started                                    |
| `DOLocationID`          | integer   | Location ID where the trip ended                                      |
| `payment_type`          | long      | Integer code for payment type (e.g., 1 = Credit card)                 |
| `fare_amount`           | double    | Fare amount charged by the meter                                      |
| `extra`                 | double    | Extra charges (e.g., for peak time)                                   |
| `mta_tax`               | double    | MTA tax charged                                                       |
| `tip_amount`            | double    | Tip amount paid by the passenger                                      |
| `tolls_amount`          | double    | Amount paid for tolls                                                 |
| `improvement_surcharge` | double    | Fixed \$0.30 NYC Improvement Surcharge                                |
| `total_amount`          | double    | Total amount paid by passenger                                        |
| `congestion_surcharge`  | double    | Surcharge for Manhattan congestion                                    |
| `Airport_fee`           | double    | Fee for trips to/from airports (e.g., JFK)                            |
| `cbd_congestion_fee`    | double    | Central Business District congestion charge                           |
