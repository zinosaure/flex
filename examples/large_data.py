import sys
import json

sys.path.append("../src")

from pathlib import Path
from datetime import datetime
from app.libs.flex import Flexmeta, Flextable


class Log(Flextable):
    def __init__(self):
        super().__init__(Flexmeta(self, "logs", max_size=500))
        self.date: str = datetime.today().strftime("%Y-%m-%d")
        self.status: str = ""


class LogMessage(Flextable):
    def __init__(self, log: Log = Log()):
        super().__init__(Flexmeta(self, f"logs/{log.date}"))
        self.date: str = log.date
        self.status: str = log.status
        self.ip: str = ""
        self.time: str = ""
        self.service: str = ""
        self.message: str = ""

Flexmeta.setPath(Path("../src"))

## uncomment code below to generate data
# with open('./logs.json') as handle:
#     for data in json.load(handle):
#         date, time = data["date"].split("T")

#         log = Log()
#         log.date = date
#         log.status = data["status"]

#         if log.commit():
#             message = LogMessage(log)
#             message.ip = data["ip"]
#             message.time = time
#             message.service = data["service"]
#             message.message = data["message"]
#             message.commit()


log_s = Log().select()
message_s = LogMessage().select()

print("Logs count (before):", log_s.count())

log_s.where(log_s.date.is_in(["2025-06-15", "2025-06-30"]))

print("Logs count (after):", log_s.count())
print("Log messages count (before):", message_s.count())

for log in log_s:
    temp_message_s = LogMessage(log).select()
    temp_message_s.where(temp_message_s.status == "INFO")
    message_s.extend(temp_message_s)

message_s.distinct(["id", "date"])

for message in message_s.fetch_all():
    print(message.to_json(indent=4))

print("Log messages count (after):", message_s.count())

