from imports import pd, io, bigquery, dd, asyncio, aiohttp, cf, List
from settings import TOKEN
from data_utils import prepare_data_by_day, push_to_bq_in_parallel, create_search_indexes, delete_tables, \
    check_if_tables_exist, merge_tables


async def get_data(session, param, offset: int = 0):
    url = "https://data.cityofnewyork.us/resource/8m42-w767.csv"
    chunk_size = 100000
    reports: List = []
    while True:
        try:
            async with session.get(url=url, params=param, timeout=360) as async_response:
                if not 200 <= async_response.status < 300:
                    raise ValueError(f"Failed to fetch data: {await async_response.text()}")
                response = await async_response.text()
                chunks = list(
                    pd.read_csv(
                        io.BytesIO(bytes(response.encode('utf-8'))),
                        sep=',',
                        header=0,
                        chunksize=chunk_size,
                        iterator=True,
                    )
                )
                # Concatenate the chunks into a single DataFrame
                df = pd.concat(chunks)
                reports.append(df)
                df_len = len(df.index)
                if df_len >= chunk_size:
                    offset += chunk_size
                    param['$offset'] = offset
                    continue
                break
        except Exception as e:
            print(f'There has been an error while fetching the data: {e}')
            raise
    return pd.concat(reports)


async def load_data_to_df(params, token: str):
    headers = {
        'X-App-Token': f'{token}'
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        for param in params:
            task = asyncio.ensure_future(get_data(session=session, param=param))
            tasks.append(task)
        response = await asyncio.gather(*tasks, return_exceptions=True)

        # Read data from response into dataframe
        chunk_dd = dd.from_pandas(pd.concat(response), npartitions=10)

        all_chunks = [chunk_dd]
        # Increment offset for next API call

        return dd.concat(all_chunks)


def load_and_push(chunk, token, project_id, table_name, client):
    async def load_and_push_async(chunk, token, project_id, table_name, client):
        api_results = await load_data_to_df(chunk, token)
        return await push_to_bq_in_parallel(
            client, [api_results], project_id, table_name
        )

    return asyncio.run(load_and_push_async(chunk, token, project_id, table_name, client))


async def main(start_date,
               end_date,
               project_id,
               dataset_name,
               table_name,
               token):
    try:
        client = bigquery.Client(project_id)
        delete_tables(client=client,
                      project_id=project_id,
                      table_name=table_name)

        list_of_params = prepare_data_by_day(start_date, end_date)

        chunk_size = 4
        with cf.ThreadPoolExecutor() as executor:
            for api_chunk in range(0, len(list_of_params), chunk_size):
                chunk = list_of_params[api_chunk:api_chunk + chunk_size]
                future = executor.submit(load_and_push, chunk, token, project_id, table_name, client)
                print(future.result())

        check_if_tables_exist(client=client,
                              project_id=project_id,
                              dataset_name=dataset_name,
                              table_name=table_name)

        merge_tables(
            client=client,
            project_id=project_id,
            dataset_name=dataset_name,
            table_name=table_name
        )

        create_search_indexes(
            client=client,
            project_id=project_id,
            dataset_name=dataset_name,
            table_name=table_name
        )
        return True

    except Exception as error:
        raise error


def run(request):
    # project_id = request.get_json().get('project_id')
    # dataset_name = request.get_json().get('dataset_name')
    # table_name = request.get_json().get('table_name')
    # start_date = request.get_json().get('start_date')
    # end_date = request.get_json().get('end_date')
    project_id = 'main-project-362218'
    dataset_name = 'main_data'
    table_name = 'nyc_fire_incident_data_TEST'
    token = f'{TOKEN}'

    start_date = '2005-01-01'
    end_date = '2005-01-05'

    batches_data = asyncio.run(main(start_date=start_date,
                                    end_date=end_date,
                                    project_id=project_id,
                                    dataset_name=dataset_name,
                                    table_name=table_name,
                                    token=token))

    return 'True' if isinstance(batches_data, bool) else 'False'


if __name__ == '__main__':
    run('us')
