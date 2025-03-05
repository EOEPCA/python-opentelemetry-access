from collections.abc import Iterator
from typing import Optional, Tuple, TypeVar, List, Union, Dict, Generic, override
from itertools import chain, groupby

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

    @override
    def __iter__(self):
        return iter(self.iterator)

    @override
    def __next__(self):
        return next(self.iterator)

    def initially_empty(self) -> bool:
        return not self._has_head

    def __bool__(self) -> bool:
        return self._has_head


class ListLikeDumpIterator(DumpIterator[T], list):
    @override
    def __len__(self):
        raise NotImplementedError("JSONLikeListIter has no length")

    @override
    def __getitem__(self, i):
        raise NotImplementedError("JSONLikeListIter cannot getitem")

    @override
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


def to_otlp_any_value_iter(jval: JSONLikeIter) -> JSONLikeDictIter:
    ## Bool must be before int
    if isinstance(jval, bool):
        return JSONLikeDictIter(iter([("boolValue", jval)]))
    elif isinstance(jval, int):
        return JSONLikeDictIter(iter([("intValue", str(jval))]))
    elif isinstance(jval, str):
        return JSONLikeDictIter(iter([("stringValue", jval)]))
    elif isinstance(jval, float):
        return JSONLikeDictIter(iter([("doubleValue", jval)]))
    elif isinstance(jval, JSONLikeDictIter):
        return JSONLikeDictIter(iter([("kvlistValue", to_kv_list_iter(jval))]))
    elif isinstance(jval, JSONLikeListIter):
        return JSONLikeDictIter(
            iter(
                [
                    (
                        "arrayValue",
                        JSONLikeListIter((to_otlp_any_value_iter(x) for x in jval)),
                    )
                ]
            )
        )
    else:
        raise TypeError(f"Unexpected anytype {type(jval)}")


def to_kv_list_iter(jsdict: JSONLikeDictIter) -> JSONLikeListIter:
    return JSONLikeListIter(
        (
            JSONLikeDictIter(iter([("key", k), ("value", to_otlp_any_value_iter(v))]))
            for k, v in iter(jsdict)
        )
    )


def to_kv_list(jsobj: JSONLikeDict) -> JSONLikeList:
    return force_jsonlike_list_iter(to_kv_list_iter(iter_jsonlike_dict(jsobj)))


def from_otlp_any_value_iter(jsobj: JSONLikeDictIter) -> JSONLikeIter:
    anyval = {k: v for k, v in jsobj}
    keys = list(anyval)
    if not len(keys) == 1:
        raise ValueError(
            f"AnyValue expected to have exactly one key, got {''.join(keys)}"
        )
    key = keys[0]
    match key:
        case "stringValue" | "intValue" | "boolValue" | "doubleValue":
            return anyval[key]
        case "kvlistValue":
            return from_kv_list_iter(anyval[key])
        case "listValue":
            return JSONLikeListIter((from_otlp_any_value_iter(x) for x in anyval[key]))
        case other:
            raise ValueError(f"Unknown AnyValue type '{other}'")


def from_kv_list_iter(jsobj: JSONLikeListIter) -> JSONLikeDictIter:
    return JSONLikeDictIter(
        (kv["key"], from_otlp_any_value_iter(expect_dict_iter(kv["value"])))
        for kv in ({k: v for k, v in kvi} for kvi in jsobj)
    )


def from_kv_list(jsobj: JSONLikeList) -> JSONLikeDict:
    return force_jsonlike_dict_iter(from_kv_list_iter(iter_jsonlike_list(jsobj)))


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


def expect_dict_iter(x: JSONLike) -> JSONLikeDictIter:
    if isinstance(x, JSONLikeDictIter):
        return x
    else:
        raise TypeError(f"Expected dict, got {type(x)}")


def expect_dict(x: JSONLike) -> JSONLikeDict:
    if isinstance(x, dict):
        return x
    else:
        raise TypeError(f"Expected dict, got {type(x)}")


def expect_list(x: JSONLike) -> JSONLikeList:
    if isinstance(x, list):
        return x
    else:
        raise TypeError(f"Expected list, got {type(x)}")


def expect_literal(x: object) -> JSONLikeLiteral:
    if isinstance(x, str) or isinstance(x, int) or isinstance(x, float):
        return x
    else:
        raise TypeError(f"Expected literal, got {type(x)}")


def expect_str(x: object) -> str:
    if isinstance(x, str):
        return x
    else:
        raise TypeError(f"Expected str, got {type(x)}")


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


def iter_jsonlike(jobj: JSONLike) -> JSONLikeIter:
    if (
        isinstance(jobj, int)
        or isinstance(jobj, float)
        or isinstance(jobj, str)
        or isinstance(jobj, bool)
    ):
        return jobj
    elif isinstance(jobj, dict):
        return iter_jsonlike_dict(jobj)
    elif isinstance(jobj, list):
        return iter_jsonlike_list(jobj)
    else:
        raise TypeError(f"Expected JSONLike, got {type(jobj)}")


def iter_jsonlike_list(jobj: JSONLikeList) -> JSONLikeListIter:
    return JSONLikeListIter(iter_jsonlike(x) for x in jobj)


def iter_jsonlike_dict(jobj: JSONLikeDict) -> JSONLikeDictIter:
    return JSONLikeDictIter((k, iter_jsonlike(v)) for k, v in jobj.items())


def peek_iterator(iter: Iterator[T]) -> Optional[Tuple[T, Iterator[T]]]:
    try:
        x = next(iter)
        return (x, chain([x], iter))
    except StopIteration:
        return None


def _normalise_attributes_shallow_any(jobj: JSONLikeIter) -> JSONLikeIter:
    if (
        isinstance(jobj, int)
        or isinstance(jobj, float)
        or isinstance(jobj, str)
        or isinstance(jobj, bool)
    ):
        return jobj
    elif isinstance(jobj, JSONLikeDictIter):
        return normalise_attributes_shallow_iter(jobj)
    elif isinstance(jobj, list):
        return JSONLikeListIter(_normalise_attributes_shallow_any(x) for x in jobj)
    else:
        raise TypeError(f"Expected JSONLike, got {type(jobj)}")


def normalise_attributes_shallow_iter(jobj: JSONLikeDictIter) -> JSONLikeDictIter:
    """
    Flattens nested dictionaries, e.g. { 'outer' : { 'inner' : v }} into { 'outer.inner' : v }
    """

    def inner(jobj_, path=""):
        for k, v in jobj_:
            path = path + k
            if isinstance(v, JSONLikeDictIter):
                yield from inner(v, path=path + ".")
            else:
                yield (k, _normalise_attributes_shallow_any(v))

    return JSONLikeDictIter(inner(jobj))


def normalise_attributes_shallow(jobj: JSONLikeDict) -> JSONLikeDict:
    return force_jsonlike_dict_iter(
        normalise_attributes_shallow_iter(iter_jsonlike_dict(jobj))
    )


def normalise_attributes_deep_iter(jobj: JSONLikeDictIter) -> JSONLikeDictIter:
    """
    Turns dotted keys into nested dictionaries, e.g. { 'outer.inner' : v } into { 'outer' : { 'inner' : v }}
    """

    def outer(kvs_outer):
        for k_primary, kvs_inner in groupby(
            map(lambda kv: (kv[0].split(".", 1), kv[1]), kvs_outer),
            key=lambda kv: kv[0][0],
        ):
            yield (k_primary, inner(kvs_inner))

    def inner(kvs):
        kv, kvs = peek_iterator(kvs)
        if len(kv[0]) == 1:
            return kv[1]
        else:
            return JSONLikeDictIter(outer(((k[1], v) for k, v in kvs)))

    return JSONLikeDictIter(outer(jobj))


def normalise_attributes_deep(jobj: JSONLikeDict) -> JSONLikeDict:
    return force_jsonlike_dict_iter(
        normalise_attributes_deep_iter(iter_jsonlike_dict(jobj))
    )


def match_attributes(
    actual_attributes: JSONLikeDict, expected_attributes: Optional[dict[str, list[str]]]
) -> bool:
    if expected_attributes is None:
        return True
    
    normalized_attributes = dict(
        normalise_attributes_shallow_iter(iter_jsonlike_dict(actual_attributes))
    )
    return all(
        (
            k in normalized_attributes
            and str(expect_literal(normalized_attributes[k])) in vs
        )
        for k, vs in expected_attributes.items()
    )
