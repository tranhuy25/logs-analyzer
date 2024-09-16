from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
import re

# Define ApacheAccessLog class
class ApacheAccessLog:
    def __init__(self, ip_address: str, client_identd: str, user_id: str, date_time: str,
                 method: str, endpoint: str, protocol: str, response_code: int, content_size: int):
        self.ip_address = ip_address
        self.client_identd = client_identd
        self.user_id = user_id
        self.date_time = date_time
        self.method = method
        self.endpoint = endpoint
        self.protocol = protocol
        self.response_code = response_code
        self.content_size = content_size

    def __repr__(self):
        return (f"ApacheAccessLog(ip_address={self.ip_address}, client_identd={self.client_identd}, "
                f"user_id={self.user_id}, date_time={self.date_time}, method={self.method}, "
                f"endpoint={self.endpoint}, protocol={self.protocol}, response_code={self.response_code}, "
                f"content_size={self.content_size})")

# Function to parse log lines
def parse_log_line(log: str) -> ApacheAccessLog:
    pattern = re.compile(r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)$')
    match = pattern.match(log)
    
    if match:
        ip_address, client_identd, user_id, date_time, method, endpoint, protocol, response_code, content_size = match.groups()
        return ApacheAccessLog(ip_address, client_identd, user_id, date_time, method, endpoint, protocol,
                               int(response_code), int(content_size))
    else:
        raise ValueError(f"Cannot parse log line: {log}")

# Main function to set up streaming context
def main():
    WINDOW_LENGTH = 30  # Window length in seconds
    SLIDE_INTERVAL = 10  # Sliding interval in seconds

    # Set up SparkConf and StreamingContext
    conf = SparkConf().setAppName("Log Analyzer Streaming in Python").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, SLIDE_INTERVAL)

    # Create DStream from socket
    log_lines_dstream: DStream = ssc.socketTextStream("localhost", 9999)

    # Parse logs and cache
    access_logs_dstream = log_lines_dstream.map(lambda line: parse_log_line(line)).cache()
    window_dstream = access_logs_dstream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    # Process RDDs in the window
    def process_rdd(rdd):
        if rdd.isEmpty():
            print("No access logs received in this time interval")
        else:
            content_sizes = rdd.map(lambda log: log.content_size).cache()
            count = content_sizes.count()
            if count > 0:
                avg_size = content_sizes.reduce(lambda x, y: x + y) / count
                min_size = content_sizes.min()
                max_size = content_sizes.max()
                print(f"Content Size - Avg: {avg_size}, Min: {min_size}, Max: {max_size}")

                response_code_to_count = rdd.map(lambda log: (log.response_code, 1)).reduceByKey(lambda x, y: x + y).take(100)
                print(f"Response code counts: {response_code_to_count}")

                ip_addresses = rdd.map(lambda log: (log.ip_address, 1)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] > 10).map(lambda x: x[0]).take(100)
                print(f"IP Addresses > 10 times: {ip_addresses}")

                top_endpoints = rdd.map(lambda log: (log.endpoint, 1)).reduceByKey(lambda x, y: x + y).top(10, key=lambda x: x[1])
                print(f"Top Endpoints: {top_endpoints}")

    window_dstream.foreachRDD(process_rdd)

    # Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
