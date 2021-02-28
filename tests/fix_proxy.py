import time
import socket
import logging
import subprocess as sp
from shutil import which

import pytest

import psycopg3
from psycopg3 import conninfo

logger = logging.getLogger()


@pytest.fixture
def proxy(dsn):
    """Return a proxy to the --test-dsn database"""
    p = Proxy(dsn)
    yield p
    p.stop()


class Proxy:
    """
    Proxy a Postgres service for testing purpose.

    Allow to lose connectivity and restart it using stop/start.
    """

    def __init__(self, server_dsn):
        cdict = conninfo.conninfo_to_dict(server_dsn)

        # Get server params
        self.server_port = cdict.get("port", "5432")
        if "host" not in cdict or cdict["host"].startswith("/"):
            self.server_host = "localhost"
        else:
            self.server_host = cdict["host"]

        # Get client params
        self.client_host = "localhost"
        self.client_port = self._get_random_port()

        # Make a connection string to the proxy
        cdict["host"] = self.client_host
        cdict["port"] = self.client_port
        cdict["sslmode"] = "disable"  # not supported by the proxy
        self.client_dsn = conninfo.make_conninfo(**cdict)

        # The running proxy process
        self.proc = None

    def start(self):
        if self.proc:
            raise ValueError("proxy already running")

        logging.info("starting proxy")
        pproxy = which("pproxy")
        if not pproxy:
            raise ValueError("pproxy program not found")
        cmdline = [pproxy, "--reuse"]
        cmdline.extend(["-l", f"tunnel://:{self.client_port}"])
        cmdline.extend(
            ["-r", f"tunnel://{self.server_host}:{self.server_port}"]
        )

        self.proc = sp.Popen(cmdline, stdout=sp.DEVNULL)
        logging.info("proxy started")
        self._wait_listen()

        # verify that the proxy works
        # TODO: investigate why it doesn't on Travis
        try:
            with psycopg3.connect(self.client_dsn):
                pass
        except Exception as e:
            pytest.xfail(f"failed to create a working proxy: {e}")

    def stop(self):
        if not self.proc:
            return

        logging.info("stopping proxy")
        self.proc.terminate()
        self.proc.wait()
        logging.info("proxy stopped")
        self.proc = None

    @classmethod
    def _get_random_port(cls):
        with socket.socket() as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def _wait_listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for i in range(20):
                if 0 == sock.connect_ex((self.client_host, self.client_port)):
                    break
                time.sleep(0.1)
            else:
                raise ValueError("the proxy didn't start listening in time")

        logging.info("proxy listening")
