import os
import requests
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tenacity imports for retry logic
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

class FastOpenDataPartitionedParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def __init__(self, base_dir: str):
        super().__init__(base_dir=base_dir)
        # Create a session once in the constructor
        self._session = requests.Session()

    def handle_output(self, context: OutputContext, obj):
        """
        Download month-by-month partitioned data for the asset in parallel.
        """
        asset_name = context.asset_key.to_python_identifier()
        context.log.info(f"[FastOpenDataPartitionedParquetIOManager] handle_output for '{asset_name}'")

        # 1) Parse the date range from the returned dict
        if not isinstance(obj, dict):
            context.log.warn(
                f"Asset '{asset_name}' did not return a dict with start/end date. Nothing to do."
            )
            return

        start_year = obj.get("start_year")
        start_month = obj.get("start_month")
        end_year = obj.get("end_year")
        end_month = obj.get("end_month")

        if not all([start_year, start_month, end_year, end_month]):
            context.log.warn(
                f"Asset '{asset_name}' returned incomplete date range. Nothing to do."
            )
            return

        # 2) Build all (year, month) partitions
        partitions = []
        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            partitions.append((current_year, current_month))
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        downloaded_files_total = []

        # 3) Download partitions in parallel using ThreadPoolExecutor
        max_workers = 8  # Adjust concurrency based on your environment
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_partition = {}
            for (yr, mo) in partitions:
                future = executor.submit(self._download_partition, asset_name, yr, mo, context)
                future_to_partition[future] = (yr, mo)

            for future in as_completed(future_to_partition):
                result = future.result()  # list of downloaded file paths
                downloaded_files_total.extend(result)

        context.log.info(
            f"[FastOpenDataPartitionedParquetIOManager] Downloaded {len(downloaded_files_total)} files "
            f"total for asset '{asset_name}'."
        )

    def _download_partition(self, asset_name: str, year: int, month: int, context):
        """
        Download all batch files for a single year-month partition of a given asset.
        We rely on 404 to stop, but also add a maximum safety limit so we never hang.
        """
        # If you know a typical maximum batch count, set it here
        MAX_BATCH_PER_MONTH = 1000

        remote_dir = f"https://fastopendata.org/{asset_name}/year={year:04d}/month={month:02d}/"
        local_dir = os.path.join(self.base_dir, asset_name, f"year={year:04d}", f"month={month:02d}")
        os.makedirs(local_dir, exist_ok=True)

        downloaded = []
        batch_num = 1

        while batch_num <= MAX_BATCH_PER_MONTH:
            file_name = f"{asset_name}_{year:04d}{month:02d}_{batch_num}.parquet"
            file_url = remote_dir + file_name

            resp_head = self._session.head(file_url)
            if resp_head.status_code == 404:
                # The server indicates no more files
                break
            if resp_head.status_code != 200:
                context.log.warn(f"HTTP {resp_head.status_code} on {file_url}, aborting.")
                break

            local_file_path = os.path.join(local_dir, file_name)
            self._download_file(file_url, local_file_path)
            downloaded.append(local_file_path)

            batch_num += 1

        if downloaded:
            context.log.info(
                f"  - Downloaded {len(downloaded)} files for {asset_name} year={year}, month={month}"
            )
        else:
            context.log.debug(
                f"  - No new files found for {asset_name} year={year}, month={month}"
            )
        return downloaded

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_fixed(2),         # Wait 2s between retries
        retry=retry_if_exception_type(
            (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
            )
        )
    )
    def _download_file(self, file_url: str, local_path: str):
        """
        Download a single file with retry, using the shared requests Session.
        """
        with self._session.get(file_url, stream=True, timeout=(5, 30)) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    def load_input(self, context: InputContext):
        """
        If you need to load the data. This example will load everything
        that handle_output just downloaded.
        """
        # Only import polars here to avoid global dependency
        import polars as pl

        asset_name = context.asset_key.to_python_identifier()
        local_asset_dir = os.path.join(self.base_dir, asset_name)
        if not os.path.exists(local_asset_dir):
            return None

        # Recursively find all .parquet
        parquet_paths = []
        for root, dirs, files in os.walk(local_asset_dir):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_paths.append(os.path.join(root, file))

        if not parquet_paths:
            return None

        dfs = [pl.read_parquet(p) for p in sorted(parquet_paths)]
        return pl.concat(dfs) if dfs else None
