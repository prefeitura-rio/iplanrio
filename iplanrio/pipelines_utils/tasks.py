# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Union

from prefect import task

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs


@task
def create_table_and_upload_to_gcs_task(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
) -> None:
    create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format=source_format,
    )
