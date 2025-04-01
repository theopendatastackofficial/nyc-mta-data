from dagster import asset, Output

from pipeline.utils.open_mateo_free_api import (
    OpenMateoDailyWeatherConfig,
    OpenMateoHourlyWeatherConfig,
    OpenMateoDailyWeatherClient,
    OpenMateoHourlyWeatherClient,
)

class OpenMateoDailyWeatherConstants:
    START_DATE = "2021-02-24"
    END_DATE = "2025-03-15"
    LATITUDE = 40.7143
    LONGITUDE = -74.006
    TIMEZONE = "America/New_York"
    TEMPERATURE_UNIT = "fahrenheit"


class OpenMateoHourlyWeatherConstants:
    START_DATE = "2025-02-01"
    END_DATE = "2025-03-15"
    LATITUDE = 40.7143
    LONGITUDE = -74.006
    TIMEZONE = "America/New_York"
    TEMPERATURE_UNIT = "fahrenheit"


@asset(
    name="daily_weather_asset",
    compute_kind="Polars",
    io_manager_key="single_file_polars_parquet_io_manager",
    group_name="weather",
    tags={"domain": "weather", "type": "ingestion", "source": "open-meteo"},
)
def daily_weather_asset(context):
    config = OpenMateoDailyWeatherConfig(
        start_date=OpenMateoDailyWeatherConstants.START_DATE,
        end_date=OpenMateoDailyWeatherConstants.END_DATE,
        latitude=OpenMateoDailyWeatherConstants.LATITUDE,
        longitude=OpenMateoDailyWeatherConstants.LONGITUDE,
        timezone=OpenMateoDailyWeatherConstants.TIMEZONE,
        temperature_unit=OpenMateoDailyWeatherConstants.TEMPERATURE_UNIT,
    )

    # We create the client with optional arguments for retries, chunking, etc.
    client = OpenMateoDailyWeatherClient(
        config=config,
        max_retries=3,
        backoff_factor=2.0,
        connect_timeout=10.0,
        read_timeout=30.0,
        logger=context.log,  # Use Dagster's log for integrated logging
    )

    # You can pass chunked=True if needed
    daily_df = client.fetch_daily_data(chunked=False)

    if daily_df.is_empty():
        context.log.warning("No daily weather data returned for the specified range.")
    else:
        head_sample = daily_df.head(5).to_dicts()
        context.log.info(f"Daily DataFrame has {daily_df.shape[0]} rows total.")

    return Output(
        value=daily_df,
        metadata={
            "dagster/row_count": daily_df.shape[0],
            "sample_rows": str(daily_df.head(5).to_dicts()) if not daily_df.is_empty() else "[]",
        },
    )


@asset(
    name="hourly_weather_asset",
    compute_kind="Polars",
    io_manager_key="single_file_polars_parquet_io_manager",
    group_name="weather",
    tags={"domain": "weather", "type": "ingestion", "source": "open-meteo"},
)
def hourly_weather_asset(context):
    config = OpenMateoHourlyWeatherConfig(
        start_date=OpenMateoHourlyWeatherConstants.START_DATE,
        end_date=OpenMateoHourlyWeatherConstants.END_DATE,
        latitude=OpenMateoHourlyWeatherConstants.LATITUDE,
        longitude=OpenMateoHourlyWeatherConstants.LONGITUDE,
        timezone=OpenMateoHourlyWeatherConstants.TIMEZONE,
        temperature_unit=OpenMateoHourlyWeatherConstants.TEMPERATURE_UNIT,
    )

    client = OpenMateoHourlyWeatherClient(
        config=config,
        max_retries=3,
        backoff_factor=2.0,
        connect_timeout=10.0,
        read_timeout=30.0,
        logger=context.log,
    )

    hourly_df = client.fetch_hourly_data(chunked=True)  # Example: chunked fetch for hourly data

    if hourly_df.is_empty():
        context.log.warning("No hourly weather data returned for the specified range.")
    else:
        head_sample = hourly_df.head(5).to_dicts()
        context.log.info(f"Hourly DataFrame has {hourly_df.shape[0]} rows total.")

    return Output(
        value=hourly_df,
        metadata={
            "dagster/row_count": hourly_df.shape[0],
            "sample_rows": str(hourly_df.head(5).to_dicts()) if not hourly_df.is_empty() else "[]",
        },
    )
