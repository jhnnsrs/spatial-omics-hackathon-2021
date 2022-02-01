import s3fs
import os
import pytest_benchmark
import pandas as pd
import pytest
import pyarrow.parquet as pq
from pyarrow import Table


EIFFEL_TOWER_CSV = "csv/eiffel-tower-smlm.csv"
EIFFEL_TOWER_PARQUET = "parquet/eiffel-tower-smlm.parquet"


LOC_SLML_CSV = "big_csv/loc-smlm.csv"
LOC_SLML_PARQUET = "parquet/loc-smlm.parquet"


@pytest.fixture
def s3_filesystem():
    os.environ["AWS_ACCESS_KEY_ID"] = "weak_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "weak_secret_key"

    return s3fs.S3FileSystem(client_kwargs={"endpoint_url": "http://localhost:9090"})


def test_benchmark_csv(s3_filesystem, benchmark):
    def load_csv():
        with s3_filesystem.open(EIFFEL_TOWER_CSV) as f:
            df = pd.read_csv(f)

    benchmark(load_csv)


def test_benchmark_pq(s3_filesystem, benchmark):
    def load_pq():
        df = (
            pq.ParquetDataset(EIFFEL_TOWER_PARQUET, filesystem=s3_filesystem)
            .read_pandas()
            .to_pandas()
        )

    benchmark(load_pq)


def test_benchmark_csv_big(s3_filesystem, benchmark):
    def load_csv():
        with s3_filesystem.open(LOC_SLML_CSV) as f:
            df = pd.read_csv(f)

    benchmark(load_csv)


def test_benchmark_pq_big(s3_filesystem, benchmark):
    def load_pq():
        df = (
            pq.ParquetDataset(LOC_SLML_PARQUET, filesystem=s3_filesystem)
            .read_pandas()
            .to_pandas()
        )

    benchmark(load_pq)
