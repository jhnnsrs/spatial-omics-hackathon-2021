import s3fs
import os
import pytest_benchmark
import pandas as pd
import pytest
<<<<<<< HEAD
import pyarrow.parquet as pq
from pyarrow import Table


EIFFEL_TOWER_CSV = "csv/eiffel-tower-smlm.csv"
EIFFEL_TOWER_PARQUET = "parquet/eiffel-tower-smlm.parquet"


LOC_SLML_CSV = "big_csv/loc-smlm.csv"
LOC_SLML_PARQUET = "parquet/loc-smlm.parquet"
=======
>>>>>>> a833141bfd0ea546c4d3cea3650fe0200446ce30


@pytest.fixture
def s3_filesystem():
    os.environ["AWS_ACCESS_KEY_ID"] = "weak_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "weak_secret_key"

    return s3fs.S3FileSystem(client_kwargs={"endpoint_url": "http://localhost:9090"})


def test_s3_filesystem(s3_filesystem):
    assert s3_filesystem.exists(EIFFEL_TOWER_CSV)


def test_read_csv(s3_filesystem):
    with s3_filesystem.open(EIFFEL_TOWER_CSV) as f:
        df = pd.read_csv(f)

    assert df.shape == (31464, 3)


def test_write_parquet(s3_filesystem):
    with s3_filesystem.open(EIFFEL_TOWER_CSV) as f:
        df = pd.read_csv(f)

    arrow_table = Table.from_pandas(df)
    pq.write_table(arrow_table, EIFFEL_TOWER_PARQUET, filesystem=s3_filesystem)


def test_write_parquet_big(s3_filesystem):
    with s3_filesystem.open(LOC_SLML_CSV) as f:
        df = pd.read_csv(f)

    arrow_table = Table.from_pandas(df)
    pq.write_table(arrow_table, LOC_SLML_PARQUET, filesystem=s3_filesystem)


def test_read_parquet(s3_filesystem):
    df = (
        pq.ParquetDataset(EIFFEL_TOWER_PARQUET, filesystem=s3_filesystem)
        .read_pandas()
        .to_pandas()
    )
    assert df.shape == (31464, 3)
