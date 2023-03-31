from imports import Callable, List, Final, Any, logging, time, cf
from data_utils import upload_data_to_bq


def run_threaded_tasks(func: Callable, data_to_process: List, *args: Any, **kwargs: Any) -> List[Any]:
    start: Final[float] = time.perf_counter()
    try:

        logging.info(f'Received data: {data_to_process} running threaded tasks')

        with cf.ThreadPoolExecutor(max_workers=6) as executor:

            if args:

                logging.info(f'Received args: {args} \n data: {data_to_process} \n run this function: {func}')
                results: cf.Future = executor.submit(func, data_to_process, args[0])
                return results.result()

            elif kwargs:
                logging.info(f'Received args: {args} \n data: {data_to_process} \n run this function: {func}')
                results: List[cf.Future] = [executor.submit(func, data, **kwargs) for data in data_to_process]
            else:

                logging.info(f'Received data: {data_to_process} \n run this function: {func}')
                results: List[cf.Future] = [executor.submit(func, data) for data in data_to_process]

        end: Final[float] = time.perf_counter()
        print(f'Done tasks and it took {round(end - start, 2)} second(s) to finish')

        return [f.result() for f in cf.as_completed(results)]

    except Exception as run_threaded_tasks_error:
        logging.warning(f'There was an error: {run_threaded_tasks_error}')
        raise f'some error: {run_threaded_tasks_error}' from run_threaded_tasks_error


def upload_data_to_bq_worker(data_to_process: List) -> List:
    return run_threaded_tasks(
        upload_data_to_bq,
        data_to_process=data_to_process
    )
