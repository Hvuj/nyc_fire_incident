# from imports import pd, io, bigquery, Dict, dd, asyncio, aiohttp, cf, List, TypedDict
# from settings import TOKEN
# from data_utils import upload_data_to_bq, prepare_data_by_day
#
#
# async def get_data(session, token: str, offset: int, retries: int = 3):
#     url = "https://data.cityofnewyork.us/resource/8m42-w767.csv"
#
#     params = {
#         "$limit": "100000",
#         "$offset": f"{offset}",
#         "$order": "starfire_incident_id",
#         "$where": "incident_datetime>='2023-03-01' AND incident_datetime<='2023-03-02'"
#     }
#     try:
#         async with session.get(url=url, params=params, timeout=360) as async_response:
#             return await async_response.text()
#     except Exception as e:
#         print(f'There has been an error while fetching the data: {e}')
#         raise
#
#
# async def load_data_to_df(token: str, offset: int):
#     chunk_size = 100000
#     headers = {
#         'X-App-Token': f'{token}'
#     }
#     async with aiohttp.ClientSession(headers=headers) as session:
#         # Make API call with current offset value
#         response = await get_data(session=session, token=token, offset=offset)
#         # response = await asyncio.gather(*tasks, return_exceptions=True)
#         chunks = list(
#             pd.read_csv(
#                 io.BytesIO(bytes(response.encode('utf-8'))),
#                 sep=',',
#                 header=0,
#                 chunksize=chunk_size,
#                 iterator=True,
#             )
#         )
#         # Concatenate the chunks into a single DataFrame
#         df = pd.concat(chunks)
#
#         # Read data from response into dataframe
#         chunk_dd = dd.from_pandas(df, npartitions=10)
#
#         all_chunks = [chunk_dd]
#         # Increment offset for next API call
#         offset += 1
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
#         # start_date = request.get_json().get('start_date')
#         # end_date = request.get_json().get('end_date')
#         # data_type = request.get_json().get('data_type')
#         # time_increment = request.get_json().get('time_increment')
#         # view_ids_string = request.get_json().get('view_ids')
#         # view_ids = [ast.literal_eval(item) for item in view_ids_string]
#         # project_id = request.get_json().get('project_id')
#         # dataset_name = request.get_json().get('dataset_name')
#         client = bigquery.Client(request["project_id"])
#         offset = 0
#
#         start_date = '2022-09-28'
#         end_date = '2022-10-01'
#
#         res = prepare_data_by_day(start_date, end_date, offset)
#         print(res)
#
#         # while True:
#         #     loaded_df = await load_data_to_df(request["token"], offset=offset)
#         #
#         #     parsed_df = await parse_data(data_to_parse=loaded_df)
#         #     computed_df = parsed_df.compute()
#         #
#         #     table_id = f"{request['project_id']}.{request['dataset_name']}.{request['table_name']}"
#         #     upload_res = await upload_data_to_bq(client=client,
#         #                                          table_id=table_id,
#         #                                          data_to_send=computed_df)
#         #     # Check if dataframe is empty
#         #     if len(loaded_df.index) == 0:
#         #         break
#         #
#         #     offset += 1
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
