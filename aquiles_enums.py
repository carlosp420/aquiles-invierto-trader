from enum import IntEnum

from django_enum import django_enum


@django_enum
class Right(IntEnum):
    call = 0
    put = 1


@django_enum
class Status(IntEnum):
    open = 0
    closed = 1
