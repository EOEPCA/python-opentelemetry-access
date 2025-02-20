from typing import Any
from urllib import parse
from pydantic import BaseModel


class LinkObject(BaseModel):
    href: str
    # rel: str | None = None
    # describedby: str | None = None
    title: str | None = None
    # Double comment so that mypy doesn't think I'm trying to specify type using comment
    ## type: str | None = None
    # hreflang: str | None = None
    # meta: Json | None = None


type Link = str | LinkObject


# class Relationships(BaseModel):
#     links: dict[str, Link] | None = None
#     data: object | None = None
#     meta: object | None = None


class Links(BaseModel):
    self: Link | None = None
    # related: object | None = None
    describedby: Link | None = None
    first: Link | None = None
    last: Link | None = None
    prev: Link | None = None
    next: Link | None = None
    root: Link | None = None


class Resource[T](BaseModel):
    id: str
    type: str
    attributes: T | None = None
    # relationships: object | None = None
    links: dict[str, Link] | None = None
    # meta: object | None = None


# class ResourceIdentifier(BaseModel):
#     id: str
# Double comment so that mypy doesn't think I'm trying to specify type using comment
##     type: str
#     meta: object | None = None


class Error(BaseModel):
    # id: str | None = None
    # links: dict[str, Link] | None = None
    # status: str | None = None
    code: str | None = None
    title: str | None = None
    detail: str | None = None
    # source: str | None = None
    meta: object | None = None


class APIOKResponse[T](BaseModel):
    data: Resource[T]
    # meta: Json | None = None
    # jsonapi: Json | None = None
    links: Links | None = None
    # included: list[Resource] | None = None


class APIOKResponseList[T](BaseModel):
    data: list[Resource[T]]
    meta: dict[str, Any] | None = None
    # jsonapi: Json | None = None
    links: Links | None = None
    # included: list[Resource] | None = None


# class APIOKResponseNoData[T](BaseModel):
#     # meta: Json | None = None
#     # jsonapi: Json | None = None
#     links: Links | None = None
#     # included: list[Resource] | None = None


class APIErrorResponse(BaseModel):
    errors: list[Error] | None = None
    # meta: Json | None = None
    # jsonapi: Json | None = None
    # links: Links | None = None


# Can't accept dict[str, str] for query params because the same key might have multiple values.
def get_url_str(
    base_url: str,
    path: str,
    path_params: dict[str, str] | None = None,
    query_params_list: list[tuple[str, str]] | None = None,
) -> str:
    if base_url.endswith("/"):
        base_url = base_url[:-1]
    path_str = (
        path.format_map(
            {
                parse.quote(key, safe=""): parse.quote(value, safe="")
                for key, value in path_params.items()
            }
        )
        if path_params
        else path
    )
    query_str = "?" + parse.urlencode(query_params_list) if query_params_list else ""
    return base_url + path_str + query_str
