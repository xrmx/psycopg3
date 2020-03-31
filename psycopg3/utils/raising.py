from typing import Any, Callable, cast, TypeVar

from functools import wraps
from .. import exceptions as exc

F = TypeVar("F", bound=Callable[..., Any])


def raise_from_here(f: F) -> F:
    """
    A decorator to pretend an exception happened in the outermost method.

    This masks the origin of the exception so that it seems to have been
    raised by `cursor.execute()` rather by some internal module.
    """

    @wraps(f)
    def raise_from_here_(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except exc.DatabaseError as e:
            raise type(e)(*e.args) from None

    return cast(F, raise_from_here_)
