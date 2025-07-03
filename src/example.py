import time
import random
import pathlib
import requests

from enum import IntEnum
from typing import Optional
from datetime import datetime, timedelta
from app.libs.flexdatabase import Flexmeta, Flextable


class GenderEnum(IntEnum):
    MALE = 1
    FEMALE = 2


list_interests = {
    1: "Drawing",
    2: "Blogging",
    3: "Gardening",
    4: "Photography",
    5: "Cooking",
    6: "Dancing",
    7: "Gaming",
}


def convert_to_timestamp(isodate: str, format: str = "%Y-%m-%d") -> int:
    try:
        return int(datetime.strptime(isodate, format).timestamp())
    except Exception:
        pass

    return 0


class Login(Flextable):
    def __init__(self):
        super().__init__(Flexmeta(self, "logins", 10000, 100))
        self.email: str = ""
        self.username: str = ""
        self.password: str = ""
        self.is_banned: bool = False
        self.registered_at: int = 0

    @staticmethod
    def load(selected_id: int) -> Optional["Flextable"]:
        return Flextable._load(Login(), selected_id)


class Photo(Flextable):
    def __init__(self, login_id: int = 0):
        super().__init__(Flexmeta(self, "photos"))
        self.login_id: int = login_id
        self.remote_path: str = ""

    def load(self, id: int) -> Optional["Flextable"]:
        return self._load(self, id)


class GeoPoint:
    def __init__(self):
        self.latitude: float = 0.0
        self.longitude: float = 0.0


class Location:
    def __init__(self):
        self.street: str = ""
        self.city: str = ""
        self.postcode: str = ""
        self.country: str = ""
        self.coordinates: GeoPoint = GeoPoint()

    def __str__(self) -> str:
        return f"{self.street}, {self.city}, {self.postcode}, {self.country}"


class Profile(Flextable):
    def __init__(self, login_id: int = 0):
        super().__init__(Flexmeta(self, "profiles"))
        self.login_id: int = login_id
        self.photo: Photo = Photo(login_id)
        self.gender: GenderEnum = GenderEnum.MALE
        self.name: str = "???"
        self.date_ob: datetime = datetime.now()
        self.location: Location = Location()
        self.phone: str = ""
        self.cellphone: str = ""
        self.interests: list[int] = []

    @property
    def timestamp_dob(self) -> int:
        return convert_to_timestamp(self.text_date_ob())

    @staticmethod
    def load(selected_id: int) -> Optional["Flextable"]:
        if login := Login.load(selected_id):
            select = Profile(login.id).select()
            select.where(select.login_id == login.id)

            if profile := select.fetch_one():
                return profile

    def text_gender(self) -> str:
        return str(GenderEnum(self.gender).name).title()

    def text_date_ob(self) -> str:
        return str(self.date_ob).split(" ")[0]

    def text_interests(self) -> str:
        return ", ".join([list_interests[i] for i in self.interests])

    def calculate_age(self) -> int:
        return (datetime.today() - self.date_ob) // timedelta(days=365.2425)


def setenv(max_items: int = 100) -> str:
    login = Login()
    login_select = login.select()

    r = requests.get(
        f"https://randomuser.me/api/?nat=us,fr,gb&results={max_items - login_select.count()}"
    )

    if r.status_code == 200:
        data = r.json()

        for i, u in enumerate(data["results"]):
            login = Login()
            login.username = f"username_{i}"
            login.email = f"{login.username}@example.com"
            login.password = "123456"
            login.is_banned = False
            login.registered_at = int(time.time() - random.randint(0, 200000))

            if login.commit():
                profile = Profile(login.id)
                profile.photo.remote_path = u["picture"]["large"]
                profile.photo.commit()

                profile.gender = (
                    GenderEnum.MALE if u["gender"] == "male" else GenderEnum.FEMALE
                )
                profile.name = f"{u['name']['first']} {u['name']['last']}"
                profile.date_ob = datetime.strptime(
                    str(u["dob"]["date"]).split("T")[0], "%Y-%m-%d"
                )
                profile.location.street = u["location"]["street"]["name"]
                profile.location.city = u["location"]["city"]
                profile.location.postcode = u["location"]["postcode"]
                profile.location.country = u["location"]["country"]
                profile.location.coordinates.latitude = float(
                    u["location"]["coordinates"]["latitude"]
                )
                profile.location.coordinates.longitude = float(
                    u["location"]["coordinates"]["longitude"]
                )
                profile.phone = u["phone"]
                profile.cellphone = u["cell"]
                profile.interests = []

                for i in range(1, random.randint(1, len(list_interests) + 1)):
                    profile.interests.append(i)

                profile.commit()

        return f"{max_items} models generated!"


def flex_print(data: Flextable, tab_indent: int = 0):
    print(("\t" * tab_indent), "-" * 20)
    print(("\t" * tab_indent), str(data))
    print(("\t" * tab_indent), "-" * 20)

    for k, v in data.__dict__.items():
        if isinstance(v, Flextable):
            print(("\t" * tab_indent), f"{k}:")
            flex_print(v, tab_indent + 1)
        else:
            print(("\t" * tab_indent), f"{k}: {v}")


if __name__ == "__main__":
    Flexmeta.setPath(pathlib.Path(__file__).resolve().parent)
    print("Flexmeta.RootPath:", Flexmeta.RootPath)
    print("-----" * 5)

    if (
        populate := input("Do you want to populate the database? [Y/N]: ")
        .strip()
        .upper()
    ) == "Y":
        setenv(100)
        print("-----" * 5)

    selected_id = 10001

    if login := Login.load(selected_id):
        flex_print(login)

        if profile := Profile.load(login.id):
            flex_print(profile)

    print("-----" * 5)
    login_s = Login().select()
    profile_s = Profile(0).select()
    login_s.left_join("profile", profile_s, "login_id", "id")

    login_s.where(login_s.id >= 10020)
    login_s.where(
        login_s["profile.location.country"].contains("France"),
        login_s["profile.location.country"].contains("Kingdom"),
    )
    login_s.where(login_s["profile.calculate_age"]() < 40)
    login_s.where(login_s["profile.interests"].is_full_intersect([2, 6]))

    login_s.sort("profile.name")
    paging = login_s.paginate(1, 15)

    print("Found:", paging.count)
    print("")
    print("Login ID | Email | Profile ID | Name | Age | Country | Interests")
    print("-------" * 10)
    for item in login_s.fetch_all():
        print(
            item.id,
            "|",
            item.email,
            "|",
            item.profile.id,
            "|",
            item.profile.name,
            "|",
            item["profile"].calculate_age(),
            "|",
            item["profile.location.country"],
            "|",
            item["profile"]["interests"],
        )
