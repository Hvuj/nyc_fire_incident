with dim_data as (

-- here it is possible to choose leverage the search function (the index was created auto in the code while using all columns logic for the dim and metrics tables so that it will be possible to find even a most specific use case)
--  the year is a partition so we already filter alot of data and save runtime and costs and so our performance inceared - the date is a cluster is bq already knows how to further optimize the search for a specific incident on a specific time of the day
-- using the primary_key we can join the data after we have what we want and so instead of running 10m rows we ran in this use case 275 rows and in terms of performacne it took 0 seconds to run and 181MB have been bills
--  it is possible to make it even more scaleable and faster with creating key, value logic i.e for each year column create a data colun which has key date i.e. {"date":"2021-01-01", {"date_data":{ here we will have all the other key values }}}
--  this logic will give us the ability to find data even faster using the search function since we will have to only search always 1 dimensions and if we leverage the json function and create this type of logic
-- struct(date,[struct(starfire_incident_id,alarm_box_borough, etc.. )] as data) as data we will be able to further optimize the run time and cost and search even more effectively
--  this logic would be best to create AFTER the load in the ETL i.e. ETL -> T -> L using dbt open source and makre it more scaleable, efficient and maintable - this way we will use the power of google bq which is faster than python
select * from sec
where
 search(sec.alarm_box_borough ,'QUEENS') and
 year=2005 and date='2005-04-11'


),

metrics_data as (

  select * from main

 where year=2005 and date='2005-04-11'
)

select * from dim_data left join metrics_data using(primary_key)





