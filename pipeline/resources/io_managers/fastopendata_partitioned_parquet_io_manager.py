import os
import requests
import polars as pl
import pyarrow.parquet as pq  # Added for Parquet validation
from dagster import ConfigurableIOManager, OutputContext, InputContext
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

class FastOpenDataPartitionedParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def __init__(self, base_dir: str):
        super().__init__(base_dir=base_dir)
        self._session = requests.Session()

    def handle_output(self, context: OutputContext, obj):
        asset_name = context.asset_key.to_python_identifier()
        context.log.info(f"[FastOpenDataPartitionedParquetIOManager] handle_output for '{asset_name}'")

        if not isinstance(obj, dict):
            context.log.warn(f"Asset '{asset_name}' did not return a dict. Nothing to do.")
            return
        start_year, start_month = obj["start_year"], obj["start_month"]
        end_year, end_month = obj["end_year"], obj["end_month"]

        partitions = []
        year, month = start_year, start_month
        while (year < end_year) or (year == end_year and month <= end_month):
            partitions.append((year, month))
            month += 1
            if month > 12:
                month = 1
                year += 1

        downloaded_files_total = []
        max_workers = 2  # Reduced from 6 to lower server/local load
        context.log.info(f"Downloading {len(partitions)} partitions for '{asset_name}' with {max_workers} workers.")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_partition = {
                executor.submit(self._download_partition, asset_name, yr, mo, context): (yr, mo)
                for (yr, mo) in partitions
            }
            for i, future in enumerate(as_completed(future_to_partition)):
                try:
                    result = future.result(timeout=300)  # 5-minute timeout per partition
                    downloaded_files_total.extend(result)
                    if i % 10 == 0:  # Progress logging
                        context.log.info(f"Processed {i+1}/{len(partitions)} partitions")
                except TimeoutError:
                    (yr, mo) = future_to_partition[future]
                    context.log.error(f"Partition (year={yr}, month={mo}) timed out after 300s")
                    raise
                except Exception as e:
                    (yr, mo) = future_to_partition[future]
                    context.log.error(f"Partition (year={yr}, month={mo}) failed: {e}")
                    raise

        context.log.info(f"[FastOpenDataPartitionedParquetIOManager] Downloaded {len(downloaded_files_total)} "
                         f"files total for asset '{asset_name}'.")

    def _download_partition(self, asset_name: str, year: int, month: int, context):
        """
        Download all batch files for a single (year, month) partition with cleanup on failure.
        """
        MAX_BATCH_PER_MONTH = 1000
        remote_dir = f"https://fastopendata.org/{asset_name}/year={year:04d}/month={month:02d}/"
        local_dir = os.path.join(self.base_dir, asset_name, f"year={year:04d}", f"month={month:02d}")
        os.makedirs(local_dir, exist_ok=True)

        downloaded = []
        batch_num = 1

        try:
            while batch_num <= MAX_BATCH_PER_MONTH:
                file_name = f"{asset_name}_{year:04d}{month:02d}_{batch_num}.parquet"
                file_url = remote_dir + file_name

                resp = self._session.get(file_url, stream=True)
                if resp.status_code == 404 or resp.headers.get("Content-Length", "0") == "0":
                    resp.close()
                    break
                if resp.status_code != 200:
                    context.log.warn(f"HTTP {resp.status_code} on GET {file_url}, skipping.")
                    resp.close()
                    break

                self._download_file_with_retry(file_url, local_dir, file_name)
                downloaded.append(os.path.join(local_dir, file_name))
                batch_num += 1

            if downloaded:
                context.log.info(f"  - Downloaded {len(downloaded)} files for {asset_name} year={year}, month={month}")
            return downloaded
        except Exception as e:
            # Clean up partial downloads on failure
            for file_path in downloaded:
                if os.path.exists(file_path):
                    os.remove(file_path)
            context.log.error(f"Failed downloading partition {asset_name} year={year}, month={month}: {e}")
            raise

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(
            (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
            )
        ),
    )
    def _download_file_with_retry(self, file_url: str, local_dir: str, file_name: str):
        """
        Download a single file with retry and validate Parquet integrity.
        """
        local_file_path = os.path.join(local_dir, file_name)
        with self._session.get(file_url, stream=True, timeout=(5, 60)) as r:
            r.raise_for_status()
            with open(local_file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        # Validate Parquet file
        try:
            pq.read_metadata(local_file_path)  # Raises if file is corrupted
        except Exception as e:
            os.remove(local_file_path)
            raise ValueError(f"Corrupted Parquet file downloaded from {file_url}: {e}")

    def load_input(self, context: InputContext):
        asset_name = context.asset_key.to_python_identifier()
        local_asset_dir = os.path.join(self.base_dir, asset_name)
        if not os.path.exists(local_asset_dir):
            return None

        parquet_paths = []
        for root, dirs, files in os.walk(local_asset_dir):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_paths.append(os.path.join(root, file))

        if not parquet_paths:
            return None

        return pl.concat([pl.read_parquet(p) for p in sorted(parquet_paths)], how="vertical")