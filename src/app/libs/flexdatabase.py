import math
import os
import glob
import uuid
import pickle

from pathlib import Path
from collections import namedtuple
from typing import Any, Optional, Callable


PaginateT = namedtuple("PaginateT", ["count", "pagination"])


def protect(*protected):
    """Returns a metaclass that protects all attributes given as strings"""

    class Protect(type):
        has_base = False

        def __new__(cls, name, bases, attrs):
            if cls.has_base:
                for attribute in attrs:
                    if attribute in protected:
                        raise AttributeError(
                            'Overriding of attribute "%s" not allowed.' % attribute
                        )
            cls.has_base = True
            klass = super().__new__(cls, name, bases, attrs)
            return klass

    return Protect


class Flex(object):
    def __init__(self, attributes: dict = {}):
        for k, v in attributes.items():
            if isinstance(k, (list, tuple)):
                setattr(self, k, [Flex(x) if isinstance(x, dict) else x for x in v])
            else:
                setattr(self, k, Flex(v) if isinstance(v, dict) else v)


class Flexmeta:
    RootPath: Path = Path("/app/src/devnull")
    Singletons: dict[str, dict[int, dict[str, Any]]] = {}
    ConnectionPool: dict[str, "Flexmeta"] = {}

    def __init__(
        self, flextable: "Flextable", name: str, min_id: int = 0, max_size: int = -1
    ):
        self.flextable: "Flextable" = flextable
        self.name: str = os.path.basename(name)
        self.name_d: str = os.path.join(Flexmeta.RootPath, name)
        self.uniqid: str = str(uuid.uuid5(uuid.NAMESPACE_OID, str(self.name_d)))
        self.min_id: int = min_id
        self.max_size: int = max_size
        journal: str = os.path.join(self.name_d, f"{self.name}.journal")
        self.journal: Flexmeta.Journal = Flexmeta.Journal(
            journal, self.flextable, min_id + 1
        )
        self.journal.load()
        Flexmeta.ConnectionPool[self.uniqid] = self

    def __str__(self) -> str:
        return f'Flexmeta(name="{self.name}", flextable="{self.flextable.__class__.__name__}", max_size={self.max_size}, journal={self.journal})'

    @staticmethod
    def load(uniqid: str) -> "Flexmeta":
        if uniqid in Flexmeta.ConnectionPool:
            return Flexmeta.ConnectionPool[uniqid]

        raise Exception(
            f"Flexmeta is broken, no connection created for this 'uniqid': {uniqid}"
        )

    @staticmethod
    def setPath(path: Path):
        Flexmeta.RootPath = path / "devnull"

    def count(self) -> int:
        return self.journal.count

    def next_id(self) -> int:
        return self.journal.next_id

    def has_commits(self) -> bool:
        return len(self.journal.commits) > 0

    def path_to_object(self, selected_id: int) -> str:
        return os.path.join(self.name_d, f"{selected_id}.object")

    def is_object_exists(self, selected_id: int) -> bool:
        return os.path.exists(self.path_to_object(selected_id))

    def load_object(self, selected_id: int) -> Optional["Flextable"]:
        flextable: "Flextable" = self.flextable.clone()

        if os.path.exists(filename := self.path_to_object(selected_id)):
            with open(filename, "rb") as handle:
                return flextable.clone(pickle.load(handle))

    def save_object(self, flextable: "Flextable") -> bool:
        self.journal.load()

        if not os.path.isdir(self.name_d):
            os.umask(0)
            os.makedirs(self.name_d, mode=0o777, exist_ok=True)

        if self.max_size > 0 and self.journal.count >= self.max_size:
            return False

        is_updated = os.path.exists(self.path_to_object(flextable.id))

        with open(self.path_to_object(flextable.id), "wb") as handle:
            pickle.dump(flextable.on_dump(), handle, protocol=pickle.HIGHEST_PROTOCOL)

        if is_updated:
            self.journal.commits.append(("UPDATED", flextable.id))
        else:
            self.journal.count += 1
            self.journal.next_id += 1
            self.journal.commits.append(("INSERTED", flextable.id))

        return self.journal.save()

    def delete_object(self, selected_id: int) -> bool:
        self.journal.load()

        if os.path.exists(filename := self.path_to_object(selected_id)):
            os.unlink(filename)
            self.journal.count -= 1
            self.journal.commits.append(("DELETED", selected_id))
            return self.journal.save()

        return False

    def load_all(self) -> dict[int, dict[str, Any]]:
        self.journal.load()
        items: dict[int, dict[str, Any]] = {}
        select = os.path.join(self.name_d, f"{self.name}.select")

        if not self.has_commits() and os.path.exists(select):
            if self.uniqid not in Flexmeta.Singletons:
                with open(select, "rb") as handle:
                    Flexmeta.Singletons[self.uniqid] = pickle.load(handle)

            return Flexmeta.Singletons[self.uniqid]
        elif self.has_commits() and self.uniqid in Flexmeta.Singletons:
            items = Flexmeta.Singletons[self.uniqid]

            for what, selected_id in self.journal.commits:
                if what == "DELETED":
                    del items[selected_id]
                elif what in ["INSERTED", "UPDATED"]:
                    if os.path.exists(temp := self.path_to_object(selected_id)):
                        with open(temp, "rb") as handle:
                            if isinstance(n_item := pickle.load(handle), dict):
                                items[selected_id] = n_item
        else:
            for temp in sorted(glob.glob(os.path.join(self.name_d, "*.object"))):
                if os.path.exists(temp):
                    with open(temp, "rb") as handle:
                        if isinstance(item := pickle.load(handle), dict):
                            items[item["id"]] = item

        if os.path.isdir(self.name_d):
            with open(select, "wb") as handle:
                pickle.dump(items, handle, protocol=pickle.HIGHEST_PROTOCOL)

        self.journal.count = len(items)
        self.journal.commits = []
        self.journal.save()
        Flexmeta.Singletons[self.uniqid] = items

        return Flexmeta.Singletons[self.uniqid]

    class Journal:
        def __init__(self, filename: str, flextable: "Flextable", next_id: int):
            self.filename: str = filename
            self.classname: str = flextable.__class__.__name__
            self.count: int = 0
            self.next_id: int = next_id
            self.commits: list[tuple[str, int]] = []

        def __str__(self) -> str:
            return f"Flexmeta.Journal(count={self.count}, next_id={self.next_id})"

        def load(self) -> bool:
            if os.path.exists(self.filename):
                with open(self.filename, "rb") as handle:
                    if isinstance(items := pickle.load(handle), dict):
                        self.count = items["count"]
                        self.next_id = items["next_id"]
                        self.classname = items["classname"]
                        self.commits = items["commits"]

                return True
            return False

        def save(self) -> bool:
            if os.path.isdir(os.path.dirname(self.filename)):
                with open(self.filename, "wb") as handle:
                    journal = {
                        "count": self.count,
                        "next_id": self.next_id,
                        "classname": self.classname,
                        "commits": self.commits,
                    }
                    pickle.dump(journal, handle)

                return True
            return False


class Flextable(
    metaclass=protect(
        "flexmeta", "_load", "prop", "clone", "commit", "delete", "select"
    ),
):
    def __init__(self, flexmeta: Flexmeta):
        self._flexmeta_uniqid_: str = flexmeta.uniqid
        self.id: int = flexmeta.next_id()
        self.uniqid: str = str(uuid.uuid5(uuid.NAMESPACE_OID, str(self.id)))

    def __getitem__(self, name: str) -> Any:
        return self.prop(name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(flexmeta={self.flexmeta})"

    @property
    def flexmeta(self) -> Flexmeta:
        return Flexmeta.load(self._flexmeta_uniqid_)

    @staticmethod
    def _load(flextable: "Flextable", selected_id: int) -> Optional["Flextable"]:
        return flextable.flexmeta.load_object(selected_id)

    def prop(self, name: str, args: list | tuple | dict = ()) -> Any:
        o: Flextable | object = self
        value: Any = None

        for tname in name.split("."):
            if value := getattr(o, tname):
                if isinstance(value, object):
                    o = value
            else:
                return value

        if callable(value):
            if isinstance(args, dict):
                return value(**args)

            return value(*args)

        return value

    def clone(self, items: dict = {}) -> "Flextable":
        flextable = type(self)()

        if items and isinstance(items, dict):
            return flextable.on_load(items)

        return flextable

    def commit(self) -> bool:
        return self.flexmeta.save_object(self)

    def delete(self) -> bool:
        return self.flexmeta.delete_object(self.id)

    def select(self) -> "Flextable.Flexselect":
        return Flextable.Flexselect(self)

    def on_dump(self) -> dict[str, Any]:
        items: dict[str, Any] = {}

        for k, v in self.__dict__.items():
            if isinstance(v, Flextable) and v != self:
                items[k] = v.on_dump()
            else:
                items[k] = v

        return items

    def on_load(self, items: dict) -> "Flextable":
        for k, v in [(k, v) for k, v in self.__dict__.items() if k in items]:
            if isinstance(v, Flextable) and v != self:
                setattr(self, k, v.on_load(items[k]))
            else:
                setattr(self, k, items[k])

        return self

    class Flexselect:
        def __init__(self, flextable: "Flextable"):
            self.flextable: Flextable = flextable
            self.items: list[Flextable] = list(
                map(
                    lambda item: flextable.clone(item),
                    self.flextable.flexmeta.load_all().values(),
                )
            )

        def __getattr__(self, name: str) -> "Flextable.Flexselect.Statement":
            return Flextable.Flexselect.Statement(self.items, name)

        def __getitem__(self, name: str) -> "Flextable.Flexselect.Statement":
            return Flextable.Flexselect.Statement(self.items, name)

        def __len__(self) -> int:
            return len(self.items)

        def __iter__(self):
            for item in self.items:
                yield item

        def empty(self):
            self.items = []

        def count(self) -> int:
            return len(self.items)

        def compact(self, name: str = "") -> list[int]:
            if not name:
                return [item.id for item in self.items]

            return [item.prop(name) for item in self.items]

        def compact_dict(self, name: str = "") -> dict[int, "Flextable"]:
            if not name:
                return {item.id: item for item in self.items}

            return {item.prop(name): item for item in self.items}

        def map(
            self, callback: Callable[["Flextable"], "Flextable"]
        ) -> "Flextable.Flexselect":
            if callable(callback):
                self.items = list(map(callback, self.items))

            return self

        def where(self, *statements: list["Flextable"]):
            self.items = []

            for items in statements:
                if isinstance(items, list):
                    self.items.extend(item for item in items if item not in self.items)

        def sort(self, name: str, desc: bool = False):
            self.items = sorted(
                self.items, key=lambda item: item.prop(name), reverse=desc
            )

        def find(self, selected_id: int) -> Optional["Flextable"]:
            return self.compact_dict().get(selected_id, None)

        def fetch_one(self) -> Optional["Flextable"]:
            for item in self.items:
                return item

        def fetch_all(self) -> list["Flextable"]:
            return self.items

        def paginate(
            self, current: int = 0, items_per_page: int = 10, nb_buttons: int = 11
        ) -> PaginateT:
            buttons = []
            total_item = self.count()

            if current <= 0 or items_per_page <= 0 or total_item <= 0:
                return PaginateT(total_item, buttons)

            if current > (max_button := math.ceil(total_item / items_per_page)):
                current = max_button

            if current < 1:
                current = 1

            offset = abs((current * items_per_page) - items_per_page)
            self.items = self.items[offset : offset + items_per_page]

            if nb_buttons >= max_button:
                nb_buttons = max_button

            if current <= nb_buttons / 2:
                buttons = [i + 1 for i in range(0, nb_buttons)]
            elif current > max_button - (nb_buttons / 2):
                buttons = [i + 1 for i in range(max_button - nb_buttons, max_button)]
            else:
                buttons = [
                    i + 1
                    for i in range(
                        current - math.ceil(nb_buttons / 2),
                        current + math.floor(nb_buttons / 2),
                    )
                ]

            if not buttons:
                return PaginateT(total_item, buttons)

            for i, n in enumerate(buttons):
                buttons[i] = (n, str(n), n == current)

            if buttons[0][0] > 1:
                buttons.insert(0, (1, "1 ... ", False))

            if buttons[-1][0] < max_button:
                buttons.append((max_button, f" ... {max_button}", False))

            return PaginateT(total_item, buttons)

        def union_join(
            self, name: str, select: "Flextable.Flexselect", using: str, on: str = ""
        ) -> "Flextable.Flexselect":
            left_table = self.compact_dict(on)
            right_table = select.compact_dict(using)
            self.items = []

            for k, item in left_table.items():
                if k in right_table:
                    setattr(item, name, right_table[k])
                else:
                    setattr(item, name, None)

                self.items.append(item)

            n_select = Flextable.Flexselect(self.flextable).compact_dict(on)

            for k, item in right_table.items():
                if k not in left_table:
                    n_item = n_select[k]
                    setattr(n_item, name, item)
                    self.items.append(n_item)

            return self

        def left_join(
            self, name: str, select: "Flextable.Flexselect", using: str, on: str = ""
        ) -> "Flextable.Flexselect":
            left_table = self.compact_dict(on)
            right_table = select.compact_dict(using)
            self.items = []

            for k, item in left_table.items():
                if k in right_table:
                    setattr(item, name, right_table[k])
                else:
                    setattr(item, name, None)

                self.items.append(item)

            return self

        class Statement:
            def __init__(
                self, items: list["Flextable"], name: str, args: tuple | list = ()
            ):
                self.name: str = name
                self.args: list = args
                self.items: list["Flextable"] = items

            def __call__(self, *args, **kwargs):
                self.args = args or kwargs
                return self

            def __eq__(self, value: Any) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) == value]

            def __ne__(self, value: Any) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) != value]

            def __lt__(self, value: Any) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) < value]

            def __gt__(self, value: Any) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) > value]

            def __le__(self, value: Any) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) <= value]

            def __ge__(self, value: Any) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) >= value]

            def is_true(self) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) is True]

            def is_false(self) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) is False]

            def is_null(self) -> list["Flextable"]:
                return [e for e in self.items if e.prop(self.name, self.args) is None]

            def is_not_null(self) -> list["Flextable"]:
                return [
                    e for e in self.items if e.prop(self.name, self.args) is not None
                ]

            def is_empty(self) -> list["Flextable"]:
                return [
                    e for e in self.items if e.prop(self.name, self.args) in [None, ""]
                ]

            def is_not_empty(self) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if e.prop(self.name, self.args) not in [None, ""]
                ]

            def is_between(self, item: tuple[int, int]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if (k := e.prop(self.name, self.args)) >= item[0] and k <= item[1]
                ]

            def is_not_between(self, item: tuple[int, int]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if not (
                        (k := e.prop(self.name, self.args)) >= item[0] and k <= item[1]
                    )
                ]

            def is_in(self, pattern: list[Any]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if e.prop(self.name, self.args) in set(pattern)
                ]

            def is_not_in(self, pattern: list[Any]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if e.prop(self.name, self.args) not in set(pattern)
                ]

            def is_intersect(self, pattern: list[Any]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if [p for p in set(pattern) if p in e.prop(self.name, self.args)]
                ]

            def is_not_intersect(self, pattern: list[Any]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if [
                        p for p in set(pattern) if p not in e.prop(self.name, self.args)
                    ]
                ]

            def is_full_intersect(self, pattern: list[Any]) -> list["Flextable"]:
                pattern = set(pattern)
                return [
                    e
                    for e in self.items
                    if len([p for p in pattern if p in e.prop(self.name, self.args)])
                    == len(pattern)
                ]

            def is_not_full_intersect(self, pattern: list[Any]) -> list["Flextable"]:
                return [
                    e
                    for e in self.items
                    if len(
                        [p for p in set(pattern) if p in e.prop(self.name, self.args)]
                    )
                    == 0
                ]

            def prefix(
                self, substring: str, sensitive: bool = False
            ) -> list["Flextable"]:
                if sensitive:
                    return [
                        e
                        for e in self.items
                        if str(e.prop(self.name, self.args)).startswith(substring)
                    ]
                return [
                    e
                    for e in self.items
                    if str(e.prop(self.name, self.args))
                    .lower()
                    .startswith(substring.lower())
                ]

            def not_prefix(
                self, substring: str, sensitive: bool = False
            ) -> list["Flextable"]:
                if sensitive:
                    return [
                        e
                        for e in self.items
                        if not str(e.prop(self.name, self.args)).startswith(substring)
                    ]
                return [
                    e
                    for e in self.items
                    if not str(e.prop(self.name, self.args))
                    .lower()
                    .startswith(substring.lower())
                ]

            def suffix(
                self, substring: str, sensitive: bool = False
            ) -> list["Flextable"]:
                if sensitive:
                    return [
                        e
                        for e in self.items
                        if str(e.prop(self.name, self.args)).endswith(substring)
                    ]
                return [
                    e
                    for e in self.items
                    if str(e.prop(self.name, self.args))
                    .lower()
                    .endswith(substring.lower())
                ]

            def not_suffix(
                self, substring: str, sensitive: bool = False
            ) -> list["Flextable"]:
                if sensitive:
                    return [
                        e
                        for e in self.items
                        if not str(e.prop(self.name, self.args)).endswith(substring)
                    ]
                return [
                    e
                    for e in self.items
                    if not str(e.prop(self.name, self.args))
                    .lower()
                    .endswith(substring.lower())
                ]

            def contains(
                self, substring: str, sensitive: bool = False
            ) -> list["Flextable"]:
                if sensitive:
                    return [
                        e
                        for e in self.items
                        if str(e.prop(self.name, self.args)).find(substring) >= 0
                    ]

                return [
                    e
                    for e in self.items
                    if str(e.prop(self.name, self.args)).lower().find(substring.lower())
                    >= 0
                ]

            def not_contains(
                self, substring: str, sensitive: bool = False
            ) -> list["Flextable"]:
                if sensitive:
                    return [
                        e
                        for e in self.items
                        if not str(e.prop(self.name, self.args)).find(substring) >= 0
                    ]

                return [
                    e
                    for e in self.items
                    if not str(e.prop(self.name, self.args))
                    .lower()
                    .find(substring.lower())
                    >= 0
                ]
