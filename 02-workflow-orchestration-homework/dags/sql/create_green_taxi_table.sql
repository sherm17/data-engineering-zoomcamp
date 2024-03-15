
set search_path to green_taxi_db;

create table public.green_taxi (
	vendor_id float,
	lpep_pickup_datetime            timestamp,
	lpep_dropoff_datetime           timestamp,
	store_and_fwd_flag              text,
	rate_code_id                    float,
	pu_location_id                  integer,
	do_location_id                  integer,
	passenger_count                 float,
	trip_distance                   float,
	fare_amount                     float,
	extra                           float,
	mta_tax                         float,
	tip_amount                      float,
	tolls_amount                    float,
	ehail_fee                       float,
	improvement_surcharge           float,
	total_amount                    float,
	payment_type                    float,
	trip_type                       float,
	congestion_surcharge            float,
	lpep_pickup_date         		date
);
