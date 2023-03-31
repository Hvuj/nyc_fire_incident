from __future__ import annotations
from typing import Union, Callable, Mapping, Optional, List, Final, Any, TypedDict, Dict, MutableMapping, NamedTuple
from google.cloud.bigquery.exceptions import BigQueryError
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, Client, LoadJob, Table, QueryJob
from pathlib import Path
from dotenv import load_dotenv
from pytz import timezone
from datetime import timedelta, date, datetime
from hashlib import sha256
import concurrent.futures as cf
import os
import logging
import dask.dataframe as dd
import multiprocessing as mp
import pandas as pd
import numpy as np
import io
import re
import asyncio
import aiohttp
import time


class DateRange(NamedTuple):
    start_date: str
    end_date: str


# define module`s public API imports
__all__ = [
    'Union', 'Callable', 'Mapping', 'Optional', 'List', 'Final', 'Any', 'TypedDict', 'Dict',
    'QueryJob', 'BigQueryError', 'logging', 'Client', 'MutableMapping', 'bigquery',
    'pd', 'LoadJobConfig', 'LoadJob', 'Table', 'io', 'Path', 'load_dotenv', 'os', 'np', 'datetime', 'timezone', 're',
    'dd', 'asyncio', 'aiohttp', 'cf', 'time', 'timedelta', 'date', 'mp', 'DateRange', 'sha256'
]
