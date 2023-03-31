from imports import List, logging, bigquery, BigQueryError, Final, LoadJobConfig, LoadJob, pd, datetime, timedelta, \
    Dict, Any, TypedDict, cf, dd, mp, DateRange, np
from transformations import parse_data, hash_element


def create_partitioned_table(client: bigquery.Client,
                             project_id: str,
                             dataset_name: str,
                             table_name: str) -> str:
    table_id: str = f'{project_id}.{dataset_name}.{table_name}'

    try:
        query_to_run: Final[str] = f"""
        create or replace table `{table_id}`
        partition by RANGE_BUCKET(year,  GENERATE_ARRAY(2005, 6005,1))
        cluster by incident_borough,incident_classification_group,incident_classification,alarm_level_index_description
        as (
           select * from `{project_id}.staging.{table_name}_staging`
        )
        """
        query: bigquery.QueryJob = client.query(query_to_run)
        print('Table deleted successfully')
        for row in query.result():
            return row[0]

    except Exception as error:
        logging.warning(f'There was an error: {error}')
        raise BigQueryError(f'An error occurred while creating the table: {table_id}') from error


def check_if_table_exists(client: bigquery.Client,
                          dataset_name: str,
                          table_name: str) -> str:
    try:
        query_to_run: Final[str] = f"""
        IF EXISTS (
        
        select 
          sum(size_bytes)/pow(10,9) as size
        from
          {dataset_name}.__TABLES__
        where 
          table_id = '{table_name}'
            )
        THEN
            (
               select 1
            );
        ELSE
            SELECT 0;
        END IF
        """
        query: bigquery.QueryJob = client.query(query_to_run)
        for row in query.result():
            if row[0] == 0:
                print('Table does not exists')
            else:
                print('Table does exists')
            return row[0]

    except Exception as error:
        logging.warning(f'There was an error: {error}')
        raise BigQueryError(f'An error occurred while checking if table exists: {table_name}') from error


def create_search_index_on_table(client: bigquery.Client,
                                 project_id: str,
                                 dataset_name: str,
                                 table_name: str) -> str:
    table_id: str = f'{project_id}.{dataset_name}.{table_name}'

    """
    The search index is created on all columns in the table by specifying "ALL COLUMNS" after the table name. 
    The "OPTIONS" section includes the use of the "LOG_ANALYZER" analyzer,
     which is one of several available text analyzers in BigQuery. 
    This analyzer applies basic text normalization and stemming to the indexed data, 
    allowing for more flexible and accurate text searches
    """
    try:
        query_to_run: Final[str] = f"""
        CREATE SEARCH INDEX my_index
        ON `{table_id}`(ALL COLUMNS)
        OPTIONS (
          analyzer = 'LOG_ANALYZER'
        );
        """
        query: bigquery.QueryJob = client.query(query_to_run)
        print('Table deleted successfully')
        return 'True'

    except Exception as error:
        logging.warning(f'There was an error: {error}')
        raise BigQueryError(f'An error occurred while delete data from BigQuery from table {table_id}') from error


def delete_temp_table(client: bigquery.Client,
                      project_id: str,
                      table_name: str) -> str:
    table_id: str = f'{project_id}.staging.{table_name}_staging'

    try:
        query_to_run: Final[str] = f"""drop table if exists `{table_id}; """
        query: bigquery.QueryJob = client.query(query_to_run)
        print(f'Table deleted successfully: {table_id}')
        return 'True'

    except Exception as error:
        logging.warning(f'There was an error: {error}')
        raise BigQueryError(f'An error occurred while delete data from BigQuery from table {table_id}') from error


def merge_data(client: bigquery.Client,
               project_id: str,
               dataset_name: str,
               table_name: str) -> str:
    try:
        query_to_run: Final[str] = f"""
        
        DECLARE min_incident_date DATE;
        DECLARE max_incident_date DATE;
        
        SET min_incident_date = (select min(date(date)) from `{project_id}.staging.{table_name}_staging`);
        
        SET max_incident_date = (select max(date(date)) from `{project_id}.staging.{table_name}_staging`);
        
        delete `{project_id}.{dataset_name}.{table_name}` 
        where date(date)>=min_incident_date and date(date)<=max_incident_date;
        
        insert `{project_id}.{dataset_name}.{table_name}`
        select * from `{project_id}.staging.{table_name}_staging` ;
        
        """
        query: bigquery.QueryJob = client.query(query_to_run)
        logging.info('Data merged successfully')
        print('Data merged successfully')
        return 'True'

    except Exception as error:
        logging.warning(f'There was an error: {error}')
        raise BigQueryError('An error occurred while fetching data from BigQuery') from error


async def upload_data_to_bq(client: bigquery.Client,
                            project_id: str,
                            table_name: str,
                            data_to_send: pd.DataFrame) -> str:
    destination_table_id: Final[str] = f"{project_id}.staging.{table_name}_staging"

    try:
        logging.info('Pushing data from BigQuery successfully')
        return run_job(client, destination_table_id, data_to_send)
    except Exception as e:
        # Log the error message and raise the exception
        logging.error(f'An error occurred while uploading data to BigQuery: {e}')
        raise e


def run_job(client: bigquery.Client, destination_table_id: str, data_to_send: pd.DataFrame) -> str:
    try:
        logging.info(f'Pushing data to table: {destination_table_id}')

        if 'metrics' in destination_table_id:
            job_config: LoadJobConfig = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                skip_leading_rows=0,
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                clustering_fields=['incident_borough',
                                   'incident_classification_group',
                                   'incident_classification',
                                   'alarm_level_index_description'],
                range_partitioning=bigquery.RangePartitioning(
                    range_=bigquery.PartitionRange(start=2005, end=6005, interval=1),
                    field="year"
                )
            )
        else:
            job_config: LoadJobConfig = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                skip_leading_rows=0,
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                clustering_fields=['year', 'date', 'primary_key'],
                range_partitioning=bigquery.RangePartitioning(
                    range_=bigquery.PartitionRange(start=2005, end=6005, interval=1),
                    field="year"
                )
            )
        job: LoadJob = client.load_table_from_dataframe(
            data_to_send, destination_table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.
        table = client.get_table(destination_table_id)  # Make an API request.
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {destination_table_id}")
        logging.info(f'Upload to BigQuery completed, job status: {job.state}')
        return 'True'
    except Exception as error:
        print('Error loading data to bq table')
        raise error


def date_range(start_date: str, end_date: str) -> List[DateRange]:
    try:
        date_list: List[DateRange] = []
        start_date: datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_date: datetime = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as val_error:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD") from val_error

    # Calculate the number of days between start_date and end_date
    delta_days = (end_date - start_date).days

    # Define the number of days for each date range
    range_days = 160

    if delta_days >= range_days:
        # If the difference between the start and end dates is 30 days or more, create ranges of 30 days
        while start_date + timedelta(days=range_days) <= end_date:
            date_list.append(DateRange(start_date=start_date.strftime("%Y-%m-%d"),
                                       end_date=(start_date + timedelta(days=range_days - 1)).strftime("%Y-%m-%d")))
            start_date += timedelta(days=range_days)
    date_list.append(DateRange(start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d")))
    return date_list


def prepare_data_by_day(start_date: str,
                        end_date: str,
                        offset: int = 0) -> List[Dict[str, List[Dict[str, Any]]]]:
    try:
        request_list: Final[List[TypedDict[str, List[TypedDict[str, Any]]]]] = []

        for dates_range in date_range(start_date=start_date, end_date=end_date):
            payload_to_insert: Dict[str, Any] = {
                "$limit": "100000",
                "$offset": f"{offset}",
                "$order": "starfire_incident_id",
                "$where": f"incident_datetime>='{dates_range.start_date}T00:00:00.000' AND "
                          f"incident_datetime<='{dates_range.end_date}T23:59:59.999'"
            }
            request_list.append(payload_to_insert)
        return request_list
    except ValueError as val_error:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD") from val_error


async def push_to_bq_in_parallel(client,
                                 batches_data: List[Any],
                                 project_id: str,
                                 table_name: str) -> bool:
    max_cpu_count: Final[int] = int(mp.cpu_count())

    with cf.ThreadPoolExecutor(max_workers=max_cpu_count + 4) as mp_executor:
        data_results = [mp_executor.submit(parse_data, sec) for sec in batches_data]
        thread_data = [f.result() for f in cf.as_completed(data_results)]

        data_to_send = dd.concat(thread_data).compute(scheduler="processes")
        data_to_send['primary_key'] = np.vectorize(hash_element)(data_to_send['primary_key'])
        metrics_df = data_to_send[['primary_key',
                                   'date',
                                   'year',
                                   'incident_response_seconds_qy',
                                   'incident_travel_tm_seconds_qy',
                                   'engines_assigned_quantity',
                                   'ladders_assigned_quantity',
                                   'other_units_assigned_quantity',
                                   'dispatch_response_seconds_qy']]

        data_to_send = data_to_send.drop(metrics_df.columns)
        data_to_send['primary_key'] = metrics_df['primary_key']
        try:

            await upload_data_to_bq(client=client,
                                    project_id=project_id,
                                    table_name=f'{table_name}_metrics',
                                    data_to_send=metrics_df)
            await upload_data_to_bq(client=client,
                                    project_id=project_id,
                                    table_name=table_name,
                                    data_to_send=data_to_send)
            return True
        except ValueError as missing_data:
            print(missing_data)


def delete_tables(client,
                  project_id,
                  table_name):
    try:
        delete_temp_table(client=client,
                          project_id=project_id,
                          table_name=f'{table_name}_metrics')
        delete_temp_table(client=client,
                          project_id=project_id,
                          table_name=table_name)
    except Exception as e:
        print(f'there was an error while deleting temp tables: {e}')
        raise


def check_if_tables_exist(client,
                          project_id,
                          dataset_name,
                          table_name):
    try:
        metrics_res = check_if_table_exists(client, dataset_name, f'{table_name}_metrics')
        if metrics_res == 0:
            create_partitioned_table(client,
                                     project_id,
                                     dataset_name,
                                     f'{table_name}_metrics')
        res = check_if_table_exists(client, dataset_name, table_name)
        if res == 0:
            create_partitioned_table(client,
                                     project_id,
                                     dataset_name,
                                     table_name)
    except Exception as e:
        print(f'there was an error while checking if the tables exist: {e}')
        raise


def merge_tables(client,
                 project_id,
                 dataset_name,
                 table_name):
    try:
        merge_data(client=client,
                   project_id=project_id,
                   dataset_name=dataset_name,
                   table_name=f'{table_name}_metrics')
        merge_data(client=client,
                   project_id=project_id,
                   dataset_name=dataset_name,
                   table_name=table_name)
    except Exception as e:
        print(f'there was an error while checking if the tables exist: {e}')
        raise


def create_search_indexes(client,
                          project_id,
                          dataset_name,
                          table_name):
    try:
        create_search_index_on_table(
            client=client,
            project_id=project_id,
            dataset_name=dataset_name,
            table_name=f'{table_name}_metrics'
        )

        create_search_index_on_table(
            client=client,
            project_id=project_id,
            dataset_name=dataset_name,
            table_name=table_name
        )
    except Exception as e:
        print(f'there was an error while checking if the tables exist: {e}')
        raise
