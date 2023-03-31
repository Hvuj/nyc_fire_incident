# from imports import pd, io, bigquery, Dict, dd, asyncio, aiohttp, cf, List
# from settings import TOKEN
# from data_utils import upload_data_to_bq, prepare_data_by_day
#
#
# async def get_data(session, params, offset: int = 0, ):
#     url = "https://data.cityofnewyork.us/resource/8m42-w767.csv"
#     chunk_size = 100000
#     try:
#         async with session.get(url=url, params=params, timeout=360) as async_response:
#             response = await async_response.text()
#             chunks = list(
#                 pd.read_csv(
#                     io.BytesIO(bytes(response.encode('utf-8'))),
#                     sep=',',
#                     header=0,
#                     chunksize=chunk_size,
#                     iterator=True,
#                 )
#             )
#             # Concatenate the chunks into a single DataFrame
#             df = pd.concat(chunks)
#
#             if len(df.index) == 0:
#                 return df
#             offset += 1
#             params['offset'] = offset
#             return get_data(session, params, offset)
#     except Exception as e:
#         print(f'There has been an error while fetching the data: {e}')
#         raise
#
#
# async def load_data_to_df(list_of_days, token: str):
#     headers = {
#         'X-App-Token': f'{token}'
#     }
#     async with aiohttp.ClientSession(headers=headers) as session:
#         tasks = []
#         for params in list_of_days:
#             task = asyncio.ensure_future(get_data(session=session, params=params))
#             tasks.append(task)
#         response = await asyncio.gather(*tasks, return_exceptions=True)
#
#         # Read data from response into dataframe
#         chunk_dd = dd.from_pandas(pd.concat(response), npartitions=10)
#
#         all_chunks = [chunk_dd]
#         # Increment offset for next API call
#
#         return dd.concat(all_chunks)
#
#
# async def parse_data(data_to_parse):
#     data_to_parse['date'] = dd.to_datetime(data_to_parse['incident_datetime']).dt.date
#     data_to_parse['year'] = dd.to_datetime(data_to_parse['incident_datetime']).dt.year
#     return data_to_parse
#
#
# async def run(request: Dict[str, str]):
#     try:
#         # project_id = request.get_json().get('project_id')
#         # dataset_name = request.get_json().get('dataset_name')
#         # table_name = request.get_json().get('table_name')
#         # start_date = request.get_json().get('start_date')
#         # end_date = request.get_json().get('end_date')
#
#         client = bigquery.Client(request["project_id"])
#
#         start_date = '2005-01-01'
#         end_date = '2022-12-31'
#
#         list_of_days = prepare_data_by_day(start_date, end_date)
#         with cf.ThreadPoolExecutor() as executor:
#             results = [executor.submit(load_data_to_df, day) for day in list_of_days]
#             loaded_df = [f.result() for f in cf.as_completed(results)]
#             print(loaded_df)
#             # loaded_df = await load_data_to_df(list_of_days,
#             #                                   request["token"])
#
#             parsed_df = await parse_data(data_to_parse=loaded_df)
#             computed_df = parsed_df.compute()
#
#             table_id = f"{request['project_id']}.{request['dataset_name']}.{request['table_name']}"
#             upload_res = await upload_data_to_bq(client=client,
#                                                  table_id=table_id,
#                                                  data_to_send=computed_df)
#
#     except KeyError as KeyError_error:
#         print(f'Error: {KeyError_error} is missing!')
#         raise
#
#
# if __name__ == '__main__':
#     data = {
#         'project_id': 'main-project-362218',
#         'dataset_name': 'main_data',
#         'table_name': 'nyc_fire_incident_data',
#         'token': f'{TOKEN}'
#     }
#     asyncio.run(run(request=data))
