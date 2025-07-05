import sys

sys.path.append("../src")

from pathlib import Path
from datetime import datetime
from app.libs.flex import Flexmeta, Flextable


class Log(Flextable):
    def __init__(self):
        super().__init__(Flexmeta(self, "logs"))
        self.type: str = ""
        self.date: str = datetime.today().strftime("%Y-%m-%d")


class LogMessage(Flextable):
    def __init__(self, date: int = 0):
        super().__init__(Flexmeta(self, f"logs/{date}"))
        self.date: int = date
        self.message: str = ""


Flexmeta.setPath(Path("../src"))
