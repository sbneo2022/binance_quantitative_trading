
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
from influxdb_client import InfluxDBClient, Point
import zulu
from pytz import timezone
import copy

class InfluxdbDataExtraction():
    def __init__(self, host='localhost', port=8086, bucket='binance'):
        self.host = host
        self.port = port
        self.bucket = bucket
        # InfluxDB client setup
        token = "0VSLkSUuQ0EsFKQspxkZc5M25JhFTthlyG1AQSyz1v0tkpzRucDfmCZPrnQyuXdZM9-0lCNarMGfd3EU9RzUug=="
        org = "Mir1"
        url = "http://localhost:8086"
        self.influx_client = InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.influx_client.query_api()

    def extract_data_basic(self, coin_id, measurement="minute_tick", unit="1h", data_to_extract=["close", "volume", "number_of_trades"], start="-30d"):
        """
        Extracts basic data from InfluxDB using the influxdb-client library.

        Parameters:
            coin_id (str): The coin identifier (e.g., "BTCUSDT").
            measurement (str): The measurement name in InfluxDB.
            unit (str): Time interval for aggregation (e.g., "1h" for 1 hour).
            data_to_extract (list): List of fields to extract.
            start (str): Start time for the data range (e.g., "-30d" for last 30 days).

        Returns:
            DataFrame: Pandas DataFrame containing the extracted data.
        """
        # Construct field filters for the Flux query
        field_filters = ' or '.join([f'r["_field"] == "{field}"' for field in data_to_extract])

        # Construct the Flux query
        query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: {start})
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
          |> filter(fn: (r) => r["pair"] == "{coin_id}")
          |> filter(fn: (r) => {field_filters})
          |> aggregateWindow(every: {unit}, fn: last, createEmpty: false)
          |> yield(name: "last")
        '''

        # Execute the query
        result = self.query_api.query(query)

        # Process the query results
        data_list = []
        for table in result:
            for record in table.records:
                data_list.append({
                    'time': record.get_time(),
                    'field': record.get_field(),
                    'value': record.get_value()
                })

        # Convert to DataFrame
        df = pd.DataFrame(data_list)
        if df.empty:
            print("No data returned from query.")
            return df

        # Pivot the DataFrame to have fields as columns
        df = df.pivot_table(values='value', index='time', columns='field').reset_index()

        return df

    def from_date_to_unix(self, date_string, date_format="%Y-%m-%dT%H:%M:%SZ", time_zone="UTC"):
        unix_time_utc = datetime.datetime.strptime(date_string, date_format).replace(tzinfo=timezone(time_zone)).timestamp()
        return unix_time_utc

    def from_unix_to_date(self, date_unix, with_Z=True):
        string_date = zulu.parse(date_unix).isoformat()
        if with_Z:
            string_date = string_date.replace(string_date[-6:], 'Z')
        return string_date

# Example usage:
if __name__ == "__main__":
    # Replace these with your actual InfluxDB credentials
    token = "your_influxdb_token"
    org = "your_influxdb_org"
    bucket = "binance"
    host = 'localhost'
    port = 8086

    # Initialize the InfluxdbDataExtraction class
    tf_influxdb_1 = InfluxdbDataExtraction(host=host, port=port, token=token, org=org, bucket=bucket)

    # Extract data
    data_basic = tf_influxdb_1.extract_data_basic(
        coin_id="BTCUSDT",
        unit="1h",
        data_to_extract=["close"],
        measurement="minute_tick",
        start="-30d"  # Adjust the time range as needed
    )

    # Display the data
    print(data_basic.head())
