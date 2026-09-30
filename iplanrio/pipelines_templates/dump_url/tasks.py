# -*- coding: utf-8 -*-
# pylint: disable=E1101
"""
General purpose tasks for dumping data from URLs.

This module provides tasks for downloading data from various URL types:
- Direct URLs
- Google Sheets
- Google Drive

All tasks are optimized for Prefect 3.0 and include proper error handling,
retry logic, and logging.
"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional

import gspread
import pandas as pd
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from prefect import task

from iplanrio.pipelines_templates.dump_url.utils import handle_dataframe_chunk
from iplanrio.pipelines_utils.env import get_credentials_from_env
from iplanrio.pipelines_utils.logging import get_logger
from iplanrio.pipelines_utils.pandas import remove_columns_accents

logger = get_logger(__name__)


@task
def download_url(
    url: str,
    fname: str,
    url_type: str = "direct",
    gsheets_sheet_order: int = 0,
    gsheets_sheet_name: Optional[str] = None,
    gsheets_sheet_range: Optional[str] = None,
) -> None:
    """
    Downloads a file from a URL and saves it to a local file.

    This task supports multiple URL types and is optimized to handle large files
    without excessive memory usage. It includes comprehensive error handling
    and logging for debugging purposes.

    Args:
        url: URL to download from.
        fname: Name of the file to save to.
        url_type: Type of URL being processed.
            - `direct`: Common URL to download directly
            - `google_drive`: Google Drive URL
            - `google_sheet`: Google Sheet URL
        gsheets_sheet_order: Worksheet index (0-based) for Google Sheets.
        gsheets_sheet_name: Worksheet name for Google Sheets (takes precedence over order).
        gsheets_sheet_range: Range in selected worksheet (e.g., "A1:D100").

    Returns:
        None

    Raises:
        ValueError: If URL type is invalid or URL format is incorrect.
        HttpError: If there's an error with Google API requests.
        Exception: For other download or processing errors.
    """
    filepath = Path(fname)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting download from URL: {url} (type: {url_type})")

    try:
        if url_type == "google_sheet":
            _download_google_sheet(
                url,
                filepath,
                gsheets_sheet_order,
                gsheets_sheet_name,
                gsheets_sheet_range,
            )
        elif url_type == "direct":
            _download_direct_url(url, filepath)
        elif url_type == "google_drive":
            _download_google_drive(url, filepath)
        else:
            raise ValueError(
                f"Invalid URL type: {url_type}. Supported types: direct, google_drive, google_sheet"
            )

        logger.info(f"Successfully downloaded data to: {filepath}")

    except Exception as e:
        logger.error(f"Error downloading from {url}: {str(e)}")
        raise


def _download_google_sheet(
    url: str,
    filepath: Path,
    sheet_order: int,
    sheet_name: Optional[str],
    sheet_range: Optional[str],
) -> None:
    """Helper function to download data from Google Sheets."""
    url_prefix = "https://docs.google.com/spreadsheets/d/"
    if not url.startswith(url_prefix):
        raise ValueError(
            f"Google Sheets URL must start with {url_prefix}. Invalid URL: {url}"
        )

    logger.info("Processing Google Sheets URL")

    credentials = get_credentials_from_env(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )

    gspread_client = gspread.authorize(credentials)
    sheet = gspread_client.open_by_url(url)

    # Select worksheet by name or order
    if sheet_name:
        worksheet = sheet.worksheet(sheet_name)
        logger.info(f"Selected worksheet by name: {sheet_name}")
    else:
        worksheet = sheet.get_worksheet(sheet_order)
        logger.info(f"Selected worksheet by order: {sheet_order}")

    # Get data from worksheet
    if sheet_range:
        logger.info(f"Getting data from range: {sheet_range}")
        dataframe = pd.DataFrame(worksheet.batch_get((sheet_range,))[0])
    else:
        logger.info("Getting all data from worksheet")
        dataframe = pd.DataFrame(worksheet.get_values())

    if dataframe.empty:
        raise ValueError("No data found in the selected worksheet")

    logger.info(f"Dataframe shape: {dataframe.shape}")
    logger.info(f"Dataframe columns: {dataframe.columns.tolist()}")

    # Process headers and clean data
    new_header = dataframe.iloc[0]
    dataframe = dataframe[1:]
    dataframe.columns = new_header

    # Remove accents from column names
    dataframe.columns = remove_columns_accents(dataframe)
    logger.info(f"Cleaned columns: {dataframe.columns.tolist()}")

    # Save to CSV
    dataframe.to_csv(filepath, index=False)
    logger.info(f"Google Sheets data saved to: {filepath}")


def _download_direct_url(url: str, filepath: Path) -> None:
    """Helper function to download data from direct URLs."""
    logger.info("Downloading from direct URL")

    response = requests.get(url, stream=True, timeout=300)  # 5 minute timeout
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded_size = 0

    with open(filepath, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):  # 8KB chunks
            if chunk:
                file.write(chunk)
                downloaded_size += len(chunk)

                # Log progress for large files
                if total_size > 0:
                    progress = (downloaded_size / total_size) * 100
                    if downloaded_size % (1024 * 1024) == 0:  # Log every MB
                        logger.info(
                            f"Download progress: {progress:.1f}% ({downloaded_size / (1024*1024):.1f} MB)"
                        )

    logger.info(f"Direct URL download completed: {filepath}")


def _download_google_drive(url: str, filepath: Path) -> None:
    """Helper function to download data from Google Drive."""
    logger.info("Processing Google Drive URL")

    # Extract file ID from URL
    url_prefix = "https://drive.google.com/file/d/"
    if not url.startswith(url_prefix):
        raise ValueError(
            f"Google Drive URL must start with {url_prefix}. Invalid URL: {url}"
        )

    file_id = url.removeprefix(url_prefix).split("/")[0]
    logger.info(f"Extracted file ID: {file_id}")

    credentials = get_credentials_from_env(
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    try:
        service = build("drive", "v3", credentials=credentials)
        request = service.files().get_media(fileId=file_id)

        with open(filepath, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    logger.info(f"Google Drive download progress: {progress}%")

        logger.info(f"Google Drive download completed: {filepath}")

    except HttpError as error:
        logger.error(f"Google Drive API error: {error}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Google Drive download: {str(e)}")
        raise


@task
def dump_files(
    file_path: str,
    partition_columns: List[str],
    save_path: str = ".",
    chunksize: int = 1_000_000,  # 1M rows per chunk
    build_json_dataframe: bool = False,
    dataframe_key_column: Optional[str] = None,
    encoding: str = "utf-8",
    on_bad_lines: str = "error",
    separator: str = ",",
) -> None:
    """
    Process CSV files in chunks and optionally partition the data.

    This task reads large CSV files in manageable chunks to avoid memory issues.
    It supports data partitioning and can generate JSON dataframes if needed.

    Args:
        file_path: Path to the input CSV file.
        partition_columns: List of column names to use for partitioning.
        save_path: Directory where processed chunks will be saved.
        chunksize: Number of rows to process in each chunk.
        build_json_dataframe: Whether to build JSON dataframes.
        dataframe_key_column: Column to use as key for JSON dataframes.
        encoding: File encoding (default: utf-8).
        on_bad_lines: How to handle bad lines (default: error).
        separator: CSV separator (default: comma).

    Returns:
        None

    Raises:
        FileNotFoundError: If the input file doesn't exist.
        ValueError: If there are issues with the data processing.
        Exception: For other processing errors.
    """
    input_path = Path(file_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    logger.info(f"Starting file processing: {file_path}")
    logger.info(f"Chunk size: {chunksize:,} rows")
    logger.info(f"Partition columns: {partition_columns}")
    logger.info(f"Save path: {save_path}")

    # Create save directory if it doesn't exist
    save_path_obj = Path(save_path)
    save_path_obj.mkdir(parents=True, exist_ok=True)

    # Generate unique event ID for this processing session
    event_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger.info(f"Processing session ID: {event_id}")

    try:
        chunk_count = 0
        total_rows = 0

        # Process file in chunks
        for idx, chunk in enumerate(
            pd.read_csv(
                input_path,
                chunksize=chunksize,
                encoding=encoding,
                on_bad_lines=on_bad_lines,
                sep=separator,
            )
        ):
            chunk_count += 1
            chunk_rows = len(chunk)
            total_rows += chunk_rows

            logger.info(f"Processing chunk {chunk_count}: {chunk_rows:,} rows")

            # Process the chunk
            handle_dataframe_chunk(
                dataframe=chunk,
                save_path=save_path,
                partition_columns=partition_columns,
                event_id=event_id,
                idx=idx,
                build_json_dataframe=build_json_dataframe,
                dataframe_key_column=dataframe_key_column,
            )

            # Log progress every 10 chunks
            if chunk_count % 10 == 0:
                logger.info(
                    f"Progress: {chunk_count} chunks processed, {total_rows:,} total rows"
                )

        logger.info(f"File processing completed successfully!")
        logger.info(f"Total chunks processed: {chunk_count}")
        logger.info(f"Total rows processed: {total_rows:,}")
        logger.info(f"Output saved to: {save_path}")

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        raise
