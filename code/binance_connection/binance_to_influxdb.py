import numpy as np
from binance.client import Client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import zulu
from binance.websockets import BinanceSocketManager
import yaml
import datetime

class BinanceInfluxdb():
    
    def __init__(self, symbol="IOTAUSDT", is_new_db=False, bucket='binance', measurement='minute_tick', host='localhost', port=8086, api_key="api_key", api_secret="private_api_key"):    

        self.bucket = bucket
        self.client = Client(api_key, api_secret)
        
        # InfluxDB client setup
        token = "0VSLkSUuQ0EsFKQspxkZc5M25JhFTthlyG1AQSyz1v0tkpzRucDfmCZPrnQyuXdZM9-0lCNarMGfd3EU9RzUug=="
        org = "Mir1"
        url = "http://localhost:8086"

        self.influx_client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.influx_client.query_api()
        self.buckets_api = self.influx_client.buckets_api()
        
        # Check if bucket exists, if not, create it
        buckets = self.buckets_api.find_bucket_by_name(self.bucket)
        if not buckets:
            retention_rules = None  # You can set retention rules here if needed
            self.buckets_api.create_bucket(bucket_name=self.bucket, retention_rules=retention_rules, org=org)
            print(f"Bucket '{self.bucket}' created.")

        self.symbol = symbol
        self.measurement_name = measurement
        self.need_data_actualization = True

    def online_process_message(self, msg):
        measurement = self.measurement_name
        msg_type = "raw"
        self.insert_data_point_influxdb(msg, measurement, msg_type)
        
        if self.need_data_actualization:
            print("##############################one time update #####################################")
            current_time_num = msg["k"]["t"] / 1000.0
            self.get_previous_point(current_time_num)
            self.need_data_actualization = False               

            
def get_previous_point(self, current_time_num):
    count = 1
    # Subtract 60 seconds to get the previous time
    pre_previous_time = datetime.datetime.utcfromtimestamp(current_time_num - 60)
    pre_previous_time_str = pre_previous_time.isoformat() + 'Z'

    query = f'''
    from(bucket: "{self.bucket}")
      |> range(start: {repr(pre_previous_time_str)}, stop: {repr(pre_previous_time_str)})
      |> filter(fn: (r) => r["_measurement"] == "{self.measurement_name}")
      |> filter(fn: (r) => r["pair"] == "{self.symbol}")
    '''
    result = self.query_api.query(query)

    # Convert the result to a list to check if it's empty
    result_list = list(result)

    while not result_list:
        count += 1
        pre_previous_time = datetime.datetime.utcfromtimestamp(current_time_num - count * 60)
        pre_previous_time_str = pre_previous_time.isoformat() + 'Z'

        query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: {repr(pre_previous_time_str)}, stop: {repr(pre_previous_time_str)})
          |> filter(fn: (r) => r["_measurement"] == "{self.measurement_name}")
          |> filter(fn: (r) => r["pair"] == "{self.symbol}")
        '''
        result = self.query_api.query(query)
        result_list = list(result)
        print(f"testing_time: {pre_previous_time_str}, count: {count}")
        if count > 1000:
            print("No previous data found.")
            return  # Exit the function if no data is found after 1000 attempts

    # Extract the timestamp of the found data point
    found_time = None
    for table in result_list:
        for record in table.records:
            found_time = record.get_time()
            break  # We only need the first record
        if found_time:
            break

    # Prepare parameters for inserting offline tick data
    event_type = 'kline'
    interval = '1m'
    units = 'minute'
    num_of_units = count + 2  # Just in case, added 2 more units
    msg_type = 'raw'

    # Convert found_time to a string format expected by Binance API
    if found_time:
        from_date = found_time.strftime("%d %b, %Y %H:%M:%S")
        to_date = from_date  # Since we have a single point, from_date and to_date are the same
        self.insert_offline_tick_data(event_type, interval, units, num_of_units, msg_type, from_now=False, from_date=from_date, to_date=to_date)
    else:
            print("Could not extract found_time from query result.")

    def insert_data_point_influxdb(self, msg, measurement, msg_type):
        if msg_type == 'raw':
            point = Point(measurement) \
                .tag("event_type", msg["e"]) \
                .tag("base_currency", msg["s"][:int(len(msg["s"])/2)]) \
                .tag("quote_currency", msg["s"][int(len(msg["s"])/2):]) \
                .tag("pair", msg["s"]) \
                .tag("interval", msg["k"]["i"]) \
                .time(datetime.datetime.utcfromtimestamp(msg["k"]["t"] / 1000.0), WritePrecision.NS) \
                .field("open", float(msg["k"]["o"])) \
                .field("close", float(msg["k"]["c"])) \
                .field("high", float(msg["k"]["h"])) \
                .field("low", float(msg["k"]["l"])) \
                .field("high-low", float(msg["k"]["h"]) - float(msg["k"]["l"])) \
                .field("close-open", float(msg["k"]["c"]) - float(msg["k"]["o"])) \
                .field("volume", float(msg["k"]["v"])) \
                .field("number_of_trades", int(msg["k"]["n"])) \
                .field("quote_volume", float(msg["k"]["q"])) \
                .field("active_buy_volume", float(msg["k"]["V"])) \
                .field("active_buy_quote_volume", float(msg["k"]["Q"])) \
                .field("gain", -1000) \
                .field("lose", -1000) \
                .field("avg_gain", -1000) \
                .field("avg_lose", -1000) \
                .field("RSI", -1000) \
                .field("MACD", -1000) \
                .field("KDJ", -1000) \
                .field("DMI", -1000) \
                .field("OBV", -1000) \
                .field("MTM", -1000) \
                .field("EMA", -1000) \
                .field("VWAP", -1000) \
                .field("AVL", -1000) \
                .field("TRIX", -1000) \
                .field("StochRSI", -1000) \
                .field("EMV", -1000) \
                .field("WR", -1000) \
                .field("BOLL", -1000) \
                .field("SAR", -1000) \
                .field("CCI", -1000) \
                .field("MA", -1000) \
                .field("VOL", -1000)

            self.write_api.write(bucket=self.bucket, org=self.influx_client.org, record=point)
            print(f"Inserting message with time: {point.time}, message type: {msg_type}, measurement: {measurement}")

    def create_msg_from_history(self, event_type, interval, symbol, units, num_of_units, from_now, from_date, to_date):
        if from_now:
            # Fetch historical klines
            if units == 'hour':        
                raw_data = self.client.get_historical_klines(symbol, interval, f"{num_of_units} hour ago UTC")
            elif units == 'day':
                raw_data = self.client.get_historical_klines(symbol, interval, f"{num_of_units} day ago UTC")        
            elif units == 'week':
                raw_data = self.client.get_historical_klines(symbol, interval, f"{num_of_units} week ago UTC")     
            elif units == 'month':
                raw_data = self.client.get_historical_klines(symbol, interval, f"{num_of_units} month ago UTC")
            elif units == 'minute':
                raw_data = self.client.get_historical_klines(symbol, interval, f"{num_of_units} minute ago UTC")         
        else:
            raw_data = self.client.get_historical_klines(symbol, interval, from_date, to_date)

        list_of_points = []

        for i, raw_msg in enumerate(raw_data):
            if i % 10000 == 0:
                print(i)
            
            point = Point(self.measurement_name) \
                .tag("event_type", event_type) \
                .tag("base_currency", symbol[:int(len(symbol)/2)]) \
                .tag("quote_currency", symbol[int(len(symbol)/2):]) \
                .tag("pair", symbol) \
                .tag("interval", interval) \
                .time(datetime.datetime.utcfromtimestamp(raw_msg[0] / 1000.0), WritePrecision.NS) \
                .field("open", float(raw_msg[1])) \
                .field("close", float(raw_msg[4])) \
                .field("high", float(raw_msg[2])) \
                .field("low", float(raw_msg[3])) \
                .field("high-low", float(raw_msg[2]) - float(raw_msg[3])) \
                .field("close-open", float(raw_msg[4]) - float(raw_msg[1])) \
                .field("volume", float(raw_msg[5])) \
                .field("number_of_trades", int(raw_msg[8])) \
                .field("quote_volume", float(raw_msg[7])) \
                .field("active_buy_volume", float(raw_msg[9])) \
                .field("active_buy_quote_volume", float(raw_msg[10])) \
                .field("gain", -1000) \
                .field("lose", -1000) \
                .field("avg_gain", -1000) \
                .field("avg_lose", -1000) \
                .field("RSI", -1000) \
                .field("MACD", -1000) \
                .field("KDJ", -1000) \
                .field("DMI", -1000) \
                .field("OBV", -1000) \
                .field("MTM", -1000) \
                .field("EMA", -1000) \
                .field("VWAP", -1000) \
                .field("AVL", -1000) \
                .field("TRIX", -1000) \
                .field("StochRSI", -1000) \
                .field("EMV", -1000) \
                .field("WR", -1000) \
                .field("BOLL", -1000) \
                .field("SAR", -1000) \
                .field("CCI", -1000) \
                .field("MA", -1000) \
                .field("VOL", -1000)
            
            list_of_points.append(point)
        return list_of_points      

    def insert_offline_data(self, list_of_points):
        self.write_api.write(bucket=self.bucket, org=self.influx_client.org, record=list_of_points)
        print(f"Inserted {len(list_of_points)} points into bucket '{self.bucket}'.")

    def insert_offline_tick_data(self, event_type, interval, units, num_of_units, msg_type, from_now=True, from_date=0, to_date=0):
        measurement = self.measurement_name
        symbol = self.symbol
        list_of_points = self.create_msg_from_history(event_type, interval, symbol, units, num_of_units, from_now, from_date, to_date)
        self.insert_offline_data(list_of_points)
        return list_of_points      

    def websocket_start(self):
        self.bm = BinanceSocketManager(self.client)
        print(f"symbol: {self.symbol}")
        self.bm.start_kline_socket(self.symbol, self.online_process_message)
        self.bm.start()
        
    def websocket_close(self):        
        self.bm.close()
