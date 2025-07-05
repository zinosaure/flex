import sys

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
        self.birth_year: int = 0
        self.contact: Contact = Contact()

    @staticmethod
    def load(selected_id: int) -> Optional["Flextable"]:
        return Flextable._load(Person(), selected_id)

    def actual_age(self) -> int:
        return datetime.now().year - self.birth_year


Flexmeta.setPath(Path("../src"))
person = Person()
person.name = "Juan Green"
person.birth_year = 2002
person.contact.mail = "thomas15@yahoo.com"
person.commit()

person = Person()
person.name = "Juan Mann"
person.birth_year = 2012
person.contact.mail = "udavis@hotmail.com"
person.commit()

person = Person()
person.name = "Mary Alvarez"
person.birth_year = 1998
person.contact.mail = "leetara@gmail.com"
person.commit()

persons = person.select()

print("Person count (before):", persons.count())
persons.where(persons.name.contains("juan"))
persons.where(persons["contact.mail"].not_suffix("@gmail.com"))
persons.where(persons.actual_age() >= 18)
print("Person count (after):", persons.count())

for person in persons.fetch_all():
    print(person.to_json(indent=4))
