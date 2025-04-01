from typing import Any
from pydantic import BaseModel

type Json = dict[str, Any]


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


class Links(BaseModel, extra="allow"):
    self: Link | None = None
    # related: object | None = None
    describedby: Link | None = None
    first: Link | None = None
    # last: Link | None = None
    # prev: Link | None = None
    next: Link | None = None
    root: Link | None = None


class Resource[T](BaseModel):
    id: str
    type: str
    attributes: T
    # relationships: object | None = None
    links: dict[str, Link] | None = None
    # meta: object | None = None


# class ResourceIdentifier(BaseModel):
#     id: str
# Double comment so that mypy doesn't think I'm trying to specify type using comment
##     type: str
#     meta: object | None = None


# A JSON Pointer [RFC6901] to the value in the request document that caused the error
# [e.g. "/data" for a primary data object, or "/data/attributes/title" for a specific attribute].
# This MUST point to a value in the request document that exists; if it doesn’t, the client SHOULD simply ignore the pointer.
class ErrorSourcePointer(BaseModel):
    pointer: str


# A string indicating which URI query parameter caused the error.
class ErrorSourceParameter(BaseModel):
    parameter: str


# a string indicating the name of a single request header which caused the error.
class ErrorSourceHeader(BaseModel):
    header: str | None


# See https://jsonapi.org/examples/#error-objects
type ErrorSource = ErrorSourcePointer | ErrorSourceParameter | ErrorSourceHeader


# # See https://jsonapi.org/examples/#error-objects
# class ErrorSource(BaseModel):
#     # a JSON Pointer [RFC6901] to the value in the request document that caused the error
#     # [e.g. "/data" for a primary data object, or "/data/attributes/title" for a specific attribute].
#     # This MUST point to a value in the request document that exists; if it doesn’t, the client SHOULD simply ignore the pointer.
#     pointer: str | None
#     # a string indicating which URI query parameter caused the error.
#     parameter: str | None
#     # a string indicating the name of a single request header which caused the error.
#     header: str | None


class Error(BaseModel):
    # id: str | None = None
    # links: dict[str, Link] | None = None
    status: str
    """HTTP status code"""
    code: str
    title: str
    detail: str | None = None
    source: ErrorSource | None = None
    meta: object | None = None


class APIOKResponse[T](BaseModel):
    data: Resource[T]
    # meta: Json | None = None
    # jsonapi: Json | None = None
    links: Links | None = None
    # included: list[Resource] | None = None


class APIOKResponseList[T, U](BaseModel):
    data: list[Resource[T]]
    meta: U
    # jsonapi: Json | None = None
    links: Links | None = None
    # included: list[Resource] | None = None


# class APIOKResponseNoData[T](BaseModel):
#     # meta: Json | None = None
#     # jsonapi: Json | None = None
#     links: Links | None = None
#     # included: list[Resource] | None = None


class APIErrorResponse(BaseModel):
    errors: list[Error]
    # meta: Json | None = None
    # jsonapi: Json | None = None
    # links: Links | None = None
