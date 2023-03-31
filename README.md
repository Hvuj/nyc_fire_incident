<p>
      This function will run on Google cloud function - there for,
      either upload manually to Google cloud function or use Gcloud CLI 
      (after the installation step) and run this command:
      <br>
      <code>gcloud functions deploy geo_service --gen2 --entry-point=run --memory=8GiB --max-instances=1500 --trigger-http --runtime=python310 --source=. --run-service-account=your_service_account -set-env-vars "TOKEN"=your_token --timeout=3600</code>
      <br>
      The auth method is done automatically by the Google cloud python SDK.
    </p>
    <p>
      If you are going to run it locally, please make sure you have the following:
      <ol>
        <li>Gcloud CLI installed - can be done from here: <a href="https://cloud.google.com/sdk/docs/install">gcloud cli install link</a></li>
        <li>After installation is complete - please open gcloud CLI and run: 
        <br>
        <code>gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery</code></li>
      </ol>
    </p>
    <p>
      If the function will be called from Google cloud function, you need to send the following:
      <ol>
        <li>project_id - the project id in bigquery</li>
        <li>dataset_name - the name of the dataset you wish to upload the data.</li>
        <li>table_name - The name of the table you wish to upload the data.</li>
        <li>start_date - from what date you wish to pull the data from.</li>
        <li>end_date - until what date you wish to pull the data from.</li>
      </ol>
    </p>

The query logic for the task:

Based on the data - it is split into 2 tables.
One for dimensions and the other for metrics while both having 3 main dimensions:

1. primary_key - created based on the unique dimensions
2. date - created from the incident_datetime
3. year - created from the incident_datetime

the partition logic is for the year since bq can have max 6000 partition.,
and since the data is from 2005 until 2022 including making the partition by date is not possible.

the clustering is done based on the table i.e. dimensions table or metrics table.

for dimensions: incident_borough, incident_classification_group, incident_classification, alarm_level_index_description

for metrics: year, date, primary_key

explanation:
The metrics is straight forward - order it based on the year , date and the key so it will be fast to search the latest
data
and join with the dimensions table.

the dimensions table-the logic is based on how would one would want to find informative data as quickly as possible,
in order to understand wait is happening i.e. lets say it`s live data and the operator needs to understand quickly where
should he dispatch the firefighters.


How to leverage this logic:


Say this is the query:

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









