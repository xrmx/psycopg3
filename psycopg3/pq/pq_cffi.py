"""
libpq Python wrapper using cffi bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Callable, List, Optional, Sequence, Union
from typing import cast as t_cast, TYPE_CHECKING

from cffi import FFI

from .enums import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)
from .misc import error_message, ConninfoOption
from ._pq_cffi import lib as impl, ffi
from ..errors import OperationalError, NotSupportedError

if TYPE_CHECKING:
    from psycopg3 import pq  # noqa

__impl__ = "cffi"


def version() -> int:
    return impl.PQlibVersion()


class PQerror(OperationalError):
    pass


class PGconn:
    __slots__ = ("pgconn_ptr",)

    def __init__(self, pgconn_ptr):
        self.pgconn_ptr = pgconn_ptr

    def __del__(self) -> None:
        self.finish()

    @classmethod
    def connect(cls, conninfo: bytes) -> "PGconn":
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        pgconn_ptr = impl.PQconnectdb(conninfo)
        if not pgconn_ptr:
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    @classmethod
    def connect_start(cls, conninfo: bytes) -> "PGconn":
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        pgconn_ptr = impl.PQconnectStart(conninfo)
        if not pgconn_ptr:
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    def connect_poll(self) -> Union[int, PollingStatus]:
        rv = self._call_int(impl.PQconnectPoll)
        return rv

    def finish(self) -> None:
        self.pgconn_ptr, p = ffi.NULL, self.pgconn_ptr
        if p:
            impl.PQfinish(p)

    @property
    def info(self) -> List["ConninfoOption"]:
        self._ensure_pgconn()
        opts = impl.PQconninfo(self.pgconn_ptr)
        if not opts:
            raise MemoryError("couldn't allocate connection info")
        try:
            return Conninfo._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    def reset(self) -> None:
        self._ensure_pgconn()
        impl.PQreset(self.pgconn_ptr)

    def reset_start(self) -> None:
        if not impl.PQresetStart(self.pgconn_ptr):
            raise PQerror("couldn't reset connection")

    def reset_poll(self) -> Union[int, PollingStatus]:
        rv = self._call_int(impl.PQresetPoll)
        return rv

    @classmethod
    def ping(self, conninfo: bytes) -> Ping:
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        rv = impl.PQping(conninfo)
        return Ping(rv)

    @property
    def db(self) -> bytes:
        return self._call_bytes(impl.PQdb)

    @property
    def user(self) -> bytes:
        return self._call_bytes(impl.PQuser)

    @property
    def password(self) -> bytes:
        return self._call_bytes(impl.PQpass)

    @property
    def host(self) -> bytes:
        return self._call_bytes(impl.PQhost)

    @property
    def hostaddr(self) -> bytes:
        lib = self._get_lipq12()
        if lib is not None:
            return self._call_bytes(lib.PQhostaddr)  # type: ignore
        else:
            raise NotSupportedError(
                "PQhostaddr requires libpq from PostgreSQL 12,"
                f" {version()} available instead"
            )

    @property
    def port(self) -> bytes:
        return self._call_bytes(impl.PQport)

    @property
    def tty(self) -> bytes:
        return self._call_bytes(impl.PQtty)

    @property
    def options(self) -> bytes:
        return self._call_bytes(impl.PQoptions)

    @property
    def status(self) -> Union[ConnStatus, int]:
        rv = impl.PQstatus(self.pgconn_ptr)
        return rv

    @property
    def transaction_status(self) -> Union[TransactionStatus, int]:
        rv = impl.PQtransactionStatus(self.pgconn_ptr)
        return rv

    def parameter_status(self, name: bytes) -> Optional[bytes]:
        self._ensure_pgconn()
        rv = impl.PQparameterStatus(self.pgconn_ptr, name)
        return ffi.string(rv) if rv else None

    @property
    def error_message(self) -> bytes:
        return ffi.string(impl.PQerrorMessage(self.pgconn_ptr))

    @property
    def protocol_version(self) -> int:
        return self._call_int(impl.PQprotocolVersion)

    @property
    def server_version(self) -> int:
        return self._call_int(impl.PQserverVersion)

    @property
    def socket(self) -> int:
        return self._call_int(impl.PQsocket)

    @property
    def backend_pid(self) -> int:
        return self._call_int(impl.PQbackendPID)

    @property
    def needs_password(self) -> bool:
        return self._call_bool(impl.PQconnectionNeedsPassword)

    @property
    def used_password(self) -> bool:
        return self._call_bool(impl.PQconnectionUsedPassword)

    @property
    def ssl_in_use(self) -> bool:
        return self._call_bool(impl.PQsslInUse)

    def exec_(self, command: bytes) -> "PGresult":
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        self._ensure_pgconn()
        rv = impl.PQexec(self.pgconn_ptr, command)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query(self, command: bytes) -> None:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        self._ensure_pgconn()
        if not impl.PQsendQuery(self.pgconn_ptr, command):
            raise PQerror(
                "sending query failed:"
                f" {error_message(t_cast('pq.PGconn', self))}"
            )

    def exec_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> "PGresult":
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        self._ensure_pgconn()
        rv = impl.PQexecParams(*args)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        self._ensure_pgconn()
        if not impl.PQsendQueryParams(*args):
            raise PQerror(
                "sending query and params failed:"
                f" {error_message(t_cast('pq.PGconn', self))}"
            )

    def send_prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> None:
        if param_types is None:
            nparams = 0
            atypes = ffi.NULL
        else:
            nparams = len(param_types)
            atypes = ffi.new(f"Oid[{nparams}]", tuple(param_types))

        self._ensure_pgconn()
        if not impl.PQsendPrepare(
            self.pgconn_ptr, name, command, nparams, atypes
        ):
            raise PQerror(
                "sending query and params failed:"
                f" {error_message(t_cast('pq.PGconn', self))}"
            )

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        # repurpose this function with a cheeky replacement of query with name,
        # drop the param_types from the result
        args = self._query_params_args(
            name, param_values, None, param_formats, result_format
        )
        args = args[:3] + args[4:]

        self._ensure_pgconn()
        if not impl.PQsendQueryPrepared(*args):
            raise PQerror(
                "sending prepared query failed:"
                f" {error_message(t_cast('pq.PGconn', self))}"
            )

    def _query_params_args(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> Any:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")

        nparams = len(param_values) if param_values is not None else 0

        if param_values:
            aparams = ffi.new(
                f"char*[{nparams}]",
                tuple(
                    ffi.from_buffer(v) if v is not None else ffi.NULL
                    for v in param_values
                ),
            )
            alenghts = ffi.new(
                f"int[{nparams}]",
                tuple(len(p) if p is not None else 0 for p in param_values),
            )
        else:
            aparams = alenghts = ffi.NULL

        if param_types is None:
            atypes = ffi.NULL
        else:
            if len(param_types) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_types))
                )
            atypes = ffi.new(f"Oid[{nparams}]", tuple(param_types))

        if param_formats is None:
            aformats = ffi.NULL
        else:
            if len(param_formats) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_formats))
                )
            aformats = ffi.new(f"int[{nparams}]", tuple(param_formats))

        return (
            self.pgconn_ptr,
            command,
            nparams,
            atypes,
            aparams,
            alenghts,
            aformats,
            result_format,
        )

    def prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        if not isinstance(command, bytes):
            raise TypeError(
                f"'command' must be bytes, got {type(command)} instead"
            )

        if param_types is None:
            nparams = 0
            atypes = ffi.NULL
        else:
            nparams = len(param_types)
            atypes = ffi.new(f"Oid[{nparams}]", tuple(param_types))

        self._ensure_pgconn()
        rv = impl.PQprepare(self.pgconn_ptr, name, command, nparams, atypes)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def exec_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[bytes]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = 0,
    ) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        nparams = len(param_values) if param_values is not None else 0
        if param_values:
            aparams = ffi.new(
                f"char*[{nparams}]",
                tuple(
                    ffi.from_buffer(v) if v is not None else ffi.NULL
                    for v in param_values
                ),
            )
            alenghts = ffi.new(
                f"int[{nparams}]",
                tuple(len(p) if p is not None else 0 for p in param_values),
            )
        else:
            aparams, alenghts = ffi.NULL

        if param_formats is None:
            aformats = ffi.NULL
        else:
            if len(param_formats) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_formats))
                )
            aformats = ffi.new(f"int[{nparams}]", tuple(param_formats))

        self._ensure_pgconn()
        rv = impl.PQexecPrepared(
            self.pgconn_ptr,
            name,
            nparams,
            aparams,
            alenghts,
            aformats,
            result_format,
        )
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def describe_prepared(self, name: bytes) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        rv = impl.PQdescribePrepared(self.pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def describe_portal(self, name: bytes) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        rv = impl.PQdescribePortal(self.pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def get_result(self) -> Optional["PGresult"]:
        rv = impl.PQgetResult(self.pgconn_ptr)
        return PGresult(rv) if rv else None

    def consume_input(self) -> None:
        if 1 != impl.PQconsumeInput(self.pgconn_ptr):
            raise PQerror(
                "consuming input failed:"
                f" {error_message(t_cast('pq.PGconn', self))}"
            )

    def is_busy(self) -> int:
        return impl.PQisBusy(self.pgconn_ptr)

    @property
    def nonblocking(self) -> int:
        return impl.PQisnonblocking(self.pgconn_ptr)

    @nonblocking.setter
    def nonblocking(self, arg: int) -> None:
        if 0 > impl.PQsetnonblocking(self.pgconn_ptr, arg):
            raise PQerror(
                f"setting nonblocking failed:"
                f" {error_message(t_cast('pq.PGconn', self))}"
            )

    def flush(self) -> int:
        rv: int = impl.PQflush(self.pgconn_ptr)
        if rv < 0:
            raise PQerror(
                f"flushing failed:{error_message(t_cast('pq.PGconn', self))}"
            )
        return rv

    def make_empty_result(self, exec_status: ExecStatus) -> "PGresult":
        rv = impl.PQmakeEmptyPGresult(self.pgconn_ptr, exec_status)
        if not rv:
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult(rv)

    def _call_bytes(
        self, func: Callable[["PGconn_struct"], Optional[bytes]]
    ) -> bytes:
        """
        Call one of the pgconn libpq functions returning a bytes pointer.
        """
        if not self.pgconn_ptr:
            raise PQerror("the connection is closed")
        rv = func(self.pgconn_ptr)
        assert rv
        return ffi.string(rv)

    def _call_int(self, func: Callable[["PGconn_struct"], int]) -> int:
        """
        Call one of the pgconn libpq functions returning an int.
        """
        if not self.pgconn_ptr:
            raise PQerror("the connection is closed")
        return func(self.pgconn_ptr)

    def _call_bool(self, func: Callable[["PGconn_struct"], int]) -> bool:
        """
        Call one of the pgconn libpq functions returning a logical value.
        """
        if not self.pgconn_ptr:
            raise PQerror("the connection is closed")
        return bool(func(self.pgconn_ptr))

    def _ensure_pgconn(self) -> None:
        if not self.pgconn_ptr:
            raise PQerror("the connection is closed")

    libpq12: None

    @classmethod
    def _get_lipq12(cls):
        try:
            return cls.libpq12
        except Exception:
            pass

        if version() >= 120000:
            ffi = FFI()
            ffi.cdef("char *PQhostaddr(void *conn);")
            cls.libpq12 = ffi.dlopen("pq")
        else:
            cls.libpq12 = None

        return cls.libpq12


class PGresult:
    __slots__ = ("pgresult_ptr", "_length")

    def __init__(self, pgresult_ptr: "PGresult_struct"):
        self.pgresult_ptr: Optional["PGresult_struct"] = pgresult_ptr
        self._length = ffi.new("int *")

    def __del__(self) -> None:
        self.clear()

    def clear(self) -> None:
        self.pgresult_ptr, p = ffi.NULL, self.pgresult_ptr
        if p:
            impl.PQclear(p)

    @property
    def status(self) -> ExecStatus:
        rv = impl.PQresultStatus(self.pgresult_ptr)
        return rv

    @property
    def error_message(self) -> bytes:
        return ffi.string(impl.PQresultErrorMessage(self.pgresult_ptr))

    def error_field(self, fieldcode: DiagnosticField) -> Optional[bytes]:
        rv = impl.PQresultErrorField(self.pgresult_ptr, fieldcode)
        return ffi.string(rv) if rv else None

    @property
    def ntuples(self) -> int:
        return impl.PQntuples(self.pgresult_ptr)

    @property
    def nfields(self) -> int:
        return impl.PQnfields(self.pgresult_ptr)

    def fname(self, column_number: int) -> Optional[bytes]:
        rv = impl.PQfname(self.pgresult_ptr, column_number)
        return ffi.string(rv) if rv else None

    def ftable(self, column_number: int) -> int:
        return impl.PQftable(self.pgresult_ptr, column_number)

    def ftablecol(self, column_number: int) -> int:
        return impl.PQftablecol(self.pgresult_ptr, column_number)

    def fformat(self, column_number: int) -> Format:
        return impl.PQfformat(self.pgresult_ptr, column_number)

    def ftype(self, column_number: int) -> int:
        return impl.PQftype(self.pgresult_ptr, column_number)

    def fmod(self, column_number: int) -> int:
        return impl.PQfmod(self.pgresult_ptr, column_number)

    def fsize(self, column_number: int) -> int:
        return impl.PQfsize(self.pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> Format:
        return impl.PQbinaryTuples(self.pgresult_ptr)

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        p = impl.pg3_get_value(
            self.pgresult_ptr, row_number, column_number, self._length
        )
        if p:
            return ffi.buffer(p, self._length[0])
        else:
            return None

    @property
    def nparams(self) -> int:
        return impl.PQnparams(self.pgresult_ptr)

    def param_type(self, param_number: int) -> int:
        return impl.PQparamtype(self.pgresult_ptr, param_number)

    @property
    def command_status(self) -> Optional[bytes]:
        rv = impl.PQcmdStatus(self.pgresult_ptr)
        return ffi.string(rv) if rv else None

    @property
    def command_tuples(self) -> Optional[int]:
        rv = ffi.string(impl.PQcmdTuples(self.pgresult_ptr))
        return int(rv) if rv else None

    @property
    def oid_value(self) -> int:
        return impl.PQoidValue(self.pgresult_ptr)


class Conninfo:
    @classmethod
    def get_defaults(cls) -> List[ConninfoOption]:
        opts = impl.PQconndefaults()
        if not opts:
            raise MemoryError("couldn't allocate connection defaults")
        try:
            return cls._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    @classmethod
    def parse(cls, conninfo: bytes) -> List[ConninfoOption]:
        if not isinstance(conninfo, bytes):
            raise TypeError(
                f"bytes expected, got {type(conninfo).__name__} instead"
            )

        p_errmsg = ffi.new("char **")
        rv = impl.PQconninfoParse(conninfo, p_errmsg)
        if not rv:
            errmsg = p_errmsg[0]
            if not errmsg:
                raise MemoryError("couldn't allocate on conninfo parse")
            else:
                exc = PQerror((ffi.string(errmsg)).decode("utf8", "replace"))
                impl.PQfreemem(errmsg)
                raise exc

        try:
            return cls._options_from_array(rv)
        finally:
            impl.PQconninfoFree(rv)

    @classmethod
    def _options_from_array(
        cls, opts: Sequence["PQconninfoOption_struct"]
    ) -> List[ConninfoOption]:
        def getkw(opt, kw: str) -> str:
            val = getattr(opt, kw)
            return ffi.string(val) if val else None

        rv = []
        skws = "keyword envvar compiled val label dispchar".split()
        i = 0
        while True:
            opt = opts[i]
            if not opt.keyword:
                break
            d = {kw: getkw(opt, kw) for kw in skws}
            d["dispsize"] = opt.dispsize
            rv.append(ConninfoOption(**d))
            i += 1

        return rv


class Escaping:
    def __init__(self, conn: Optional[PGconn] = None):
        self.conn = conn

    def escape_bytea(self, data: bytes) -> bytes:
        len_out = ffi.new("size_t *")
        if self.conn is not None:
            self.conn._ensure_pgconn()
            out = impl.PQescapeByteaConn(
                self.conn.pgconn_ptr, data, len(data), len_out
            )
        else:
            out = impl.PQescapeBytea(data, len(data), len_out,)
        if not out:
            raise MemoryError(
                f"couldn't allocate for escape_bytea of {len(data)} bytes"
            )

        # out includes final 0
        rv = ffi.unpack(ffi.cast("char *", out), len_out[0] - 1)
        # TODO: can it be done without a copy using ffi.buffer()?
        impl.PQfreemem(out)
        return rv

    def unescape_bytea(self, data: bytes) -> bytes:
        # not needed, but let's keep it symmetric with the escaping:
        # if a connection is passed in, it must be valid.
        if self.conn is not None:
            self.conn._ensure_pgconn()

        len_out = ffi.new("size_t *")
        out = impl.PQunescapeBytea(ffi.from_buffer(data), len_out)
        if not out:
            raise MemoryError(
                f"couldn't allocate for unescape_bytea of {len(data)} bytes"
            )

        rv = ffi.unpack(ffi.cast("char *", out), len_out[0])
        # TODO: can it be done without a copy using ffi.buffer()?
        impl.PQfreemem(out)
        return rv
