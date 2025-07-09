import sys
import json

sys.path.append("../src")

from pathlib import Path
from typing import Optional
from datetime import datetime
from app.libs.flex import Flexmeta, Flextable


class Contact:
    def __init__(self):
        self.mail: str = ""


class Person(Flextable):
    def __init__(self):
        super().__init__(Flexmeta(self, "persons", 10000, 100))
        self.name: str = ""
        self.dob: str = datetime.today().strftime("%Y-%m-%d")
        self.contact: Contact = Contact()

    @staticmethod
    def load(selected_id: int) -> Optional["Flextable"]:
        return Flextable._load(Person(), selected_id)

    def actual_age(self) -> int:
        return datetime.now().year - datetime.fromisoformat(self.dob).year


Flexmeta.setPath(Path("../src"))

## uncomment code below to generate data
# with open('./persons.json') as handle:
#     for data in json.load(handle):
#         person = Person()
#         person.name = f'{data["name"]["last"]} {data["name"]["first"]}'
#         person.dob = str(data["dob"]["date"]).split("T")[0]
#         person.contact.mail = data["email"]
#         person.commit()

person_s = Person().select()

print("Person count (before):", person_s.count())
person_s.where(person_s.name.contains("jack"))
person_s.where(person_s["contact.mail"].contains("@example.com"))
person_s.where(person_s.actual_age() >= 18)

for person in person_s.fetch_all():
    print(person.to_json(indent=4))

print("Person count (after):", person_s.count())
