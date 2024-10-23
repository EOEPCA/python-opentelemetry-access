from collections.abc import Iterator
from typing import Optional, Tuple, TypeVar, List, Union, Dict, Generic
from itertools import chain

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common


T = TypeVar("T")


class DumpIterator(Generic[T], Iterator[T]):
    iterator: Iterator[T]

    def __init__(self, iterable):
        try:
            head = next(iterable)
            self.iterator = chain([head], iterable)
            self._has_head = True
        except StopIteration:
            self.iterator = iter([])
            self._has_head = False

    def __iter__(self):
        return iter(self.iterator)

    def __next__(self):
        return next(self.iterator)

    def initially_empty(self) -> bool:
        return not self._has_head

    def __bool__(self) -> bool:
        return self._has_head


class ListLikeDumpIterator(DumpIterator[T], list):
    def __len__(self):
        raise NotImplementedError("JSONLikeListIter has no length")

    def __getitem__(self, i):
        raise NotImplementedError("JSONLikeListIter cannot getitem")

    def __setitem__(self, i):
        raise NotImplementedError("JSONLikeListIter cannot setitem")


class JSONLikeDictIter(DumpIterator[Tuple[str, "JSONLikeIter"]]):
    pass


class JSONLikeListIter(ListLikeDumpIterator["JSONLikeIter"]):
    pass


JSONLikeLiteralIter = Union[str, bool, int, float]
JSONLikeIter = Union[JSONLikeLiteralIter, JSONLikeDictIter, JSONLikeListIter]

JSONLikeDict = Dict[str, "JSONLike"]
JSONLikeList = List["JSONLike"]
JSONLikeLiteral = Union[str, bool, int, float]
JSONLike = Union[JSONLikeLiteral, JSONLikeDict, JSONLikeList]


def jsonlike_iter_to_any_value(jsiter: JSONLikeIter) -> common.AnyValue:
    if isinstance(jsiter, JSONLikeDictIter):
        return common.AnyValue(
            kvlist_value=common.KeyValueList(jsonlike_dict_iter_to_kvlist(jsiter))
        )
    elif isinstance(jsiter, JSONLikeListIter):
        return common.AnyValue(
            array_value=common.ArrayValue(
                [jsonlike_iter_to_any_value(x) for x in iter(jsiter)]
            )
        )
    elif isinstance(jsiter, str):
        return common.AnyValue(string_value=jsiter)
    ## Bool must be before int
    elif isinstance(jsiter, bool):
        return common.AnyValue(bool_value=jsiter)
    elif isinstance(jsiter, int):
        return common.AnyValue(int_value=jsiter)
    elif isinstance(jsiter, float):
        return common.AnyValue(double_value=jsiter)


def jsonlike_dict_iter_to_kvlist(jsiter: JSONLikeDictIter) -> List[common.KeyValue]:
    match jsiter:
        case JSONLikeDictIter(iterator=inner):
            return [
                common.KeyValue(key=k, value=jsonlike_iter_to_any_value(v))
                for k, v in inner
            ]


def _expect_field_type(jobj, field, type_, optional=False, default=None):
    if field not in jobj:
        if optional:
            return default
        else:
            raise KeyError(f"Expected to have field {field} of type {type_.__name__}")
    x = jobj[field]
    if isinstance(x, type_):
        return x
    else:
        raise TypeError(f"{field} is expeted to be {type_.__name__}, got {type(x)}")


def expect_dict(x: JSONLike) -> JSONLikeDict:
    if isinstance(x, dict):
        return x
    else:
        raise TypeError(f"Expected dict, got {type(x)}")


def force_jsonlike_iter(jobj: JSONLikeIter) -> JSONLike:
    if (
        isinstance(jobj, int)
        or isinstance(jobj, float)
        or isinstance(jobj, str)
        or isinstance(jobj, bool)
    ):
        return jobj
    elif isinstance(jobj, JSONLikeDictIter):
        return force_jsonlike_dict_iter(jobj)
    elif isinstance(jobj, JSONLikeListIter):
        return force_jsonlike_list_iter(jobj)
    else:
        raise TypeError(f"Expected JSONLikeIter, got {type(jobj)}")


def force_jsonlike_list_iter(jobj: JSONLikeListIter) -> JSONLikeList:
    return [force_jsonlike_iter(x) for x in jobj]


def force_jsonlike_dict_iter(jobj: JSONLikeDictIter) -> JSONLikeDict:
    return {k: force_jsonlike_iter(v) for k, v in jobj}


T = TypeVar("T")


def peek_iterator(iter: Iterator[T]) -> Optional[Tuple[T, Iterator[T]]]:
    try:
        x = next(iter)
        return (x, chain([x], iter))
    except StopIteration:
        return None
