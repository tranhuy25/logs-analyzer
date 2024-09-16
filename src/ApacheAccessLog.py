import re

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

# Regular expression pattern for parsing log lines
PATTERN = re.compile(r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)$')

def parse_log_line(log: str) -> ApacheAccessLog:
    match = PATTERN.match(log)
    
    if match:
        ip_address, client_identd, user_id, date_time, method, endpoint, protocol, response_code, content_size = match.groups()
        return ApacheAccessLog(ip_address, client_identd, user_id, date_time, method, endpoint, protocol,
                               int(response_code), int(content_size))
    else:
        raise ValueError(f"Cannot parse log line: {log}")
