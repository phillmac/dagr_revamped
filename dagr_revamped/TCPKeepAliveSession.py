# https://www.finbourne.com/blog/the-mysterious-hanging-client-tcp-keep-alives
# https://github.com/finbourne/lusid-sdk-python/pull/58/files#diff-f4bf636dd1b58b95528bba991d6dc9fcc2dcba76e4de1b317321b69cf729db81R87

import socket
import sys

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import (HTTPConnectionPool, HTTPSConnectionPool, PoolManager,
                     ProxyManager)
from urllib3.util.retry import Retry

# The content to send on Mac OS in the TCP Keep Alive probe
TCP_KEEPALIVE = 0x10
# The maximum time to keep the connection idle before sending probes
TCP_KEEP_IDLE = 60
# The interval between probes
TCP_KEEPALIVE_INTERVAL = 60
# The maximum number of failed probes before terminating the connection
TCP_KEEP_CNT = 3


class TCPKeepAliveValidationMethods:
    """
    This class contains a single method whose sole purpose is to set up TCP Keep Alive probes on the socket for a
    connection. This is necessary for long running requests which will be silently terminated by the AWS Network Load
    Balancer which kills a connection if it is idle for more then 350 seconds.
    """
    @staticmethod
    def adjust_connection_socket(conn):
        # TCP Keep Alive Probes for different platforms
        platform = sys.platform
        if not getattr(conn, 'sock', None):  # AppEngine might not have  `.sock`
            if conn.sock is None:  # HTTPS _validate_conn calls conn.connect() already HTTP doesn't
                conn.connect()
        # TCP Keep Alive Probes for Linux
        if platform == 'linux' and hasattr(socket, "TCP_KEEPIDLE") and hasattr(socket, "TCP_KEEPINTVL") and hasattr(socket, "TCP_KEEPCNT"):
            conn.sock.setsockopt(socket.IPPROTO_TCP,
                                 socket.TCP_KEEPIDLE, TCP_KEEP_IDLE)
            conn.sock.setsockopt(socket.IPPROTO_TCP,
                                 socket.TCP_KEEPINTVL, TCP_KEEPALIVE_INTERVAL)
            conn.sock.setsockopt(socket.IPPROTO_TCP,
                                 socket.TCP_KEEPCNT, TCP_KEEP_CNT)

        # TCP Keep Alive Probes for Windows OS
        elif platform == 'win32' and hasattr(socket, "SIO_KEEPALIVE_VALS"):
            conn.sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1,
                                                        TCP_KEEP_IDLE * 1000, TCP_KEEPALIVE_INTERVAL * 1000))

        # TCP Keep Alive Probes for Mac OS
        elif platform == 'darwin':
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            conn.sock.setsockopt(socket.IPPROTO_TCP,
                                 TCP_KEEPALIVE, TCP_KEEPALIVE_INTERVAL)


class TCPKeepAliveHTTPSConnectionPool(HTTPSConnectionPool):
    """
    This class overrides the _validate_conn method in the HTTPSConnectionPool class. This is the entry point to use
    for modifying the socket as it is called after the socket is created and before the request is made.
    """

    def _validate_conn(self, conn):
        """
        Called right before a request is made, after the socket is created.
        """
        # Call the method on the base class
        super()._validate_conn(conn)

        # Set up TCP Keep Alive probes, this is the only line added to this function
        TCPKeepAliveValidationMethods.adjust_connection_socket(conn)


class TCPKeepAliveHTTPConnectionPool(HTTPConnectionPool):
    """
    This class overrides the _validate_conn method in the HTTPSConnectionPool class. This is the entry point to use
    for modifying the socket as it is called after the socket is created and before the request is made.
    In the base class this method is passed completely.
    """

    def _validate_conn(self, conn):
        """
        Called right before a request is made, after the socket is created.
        """
        # Call the method on the base class
        super()._validate_conn(conn)

        # Set up TCP Keep Alive probes, this is the only line added to this function
        TCPKeepAliveValidationMethods.adjust_connection_socket(conn)


class TCPKeepAlivePoolManager(PoolManager):
    """
    This Pool Manager has only had the pool_classes_by_scheme variable changed. This now points at the TCPKeepAlive
    connection pools rather than the default connection pools.
    """

    def __init__(self, num_pools=10, headers=None, **connection_pool_kw):
        super().__init__(num_pools=num_pools, headers=headers, **connection_pool_kw)
        self.pool_classes_by_scheme = {
            "http": TCPKeepAliveHTTPConnectionPool, "https": TCPKeepAliveHTTPSConnectionPool}


class TCPKeepAliveProxyManager(ProxyManager):
    """
    This Proxy Manager has only had the pool_classes_by_scheme variable changed. This now points at the TCPKeepAlive
    connection pools rather than the default connection pools.
    """

    def __init__(self, proxy_url, num_pools=10, headers=None, proxy_headers=None, **connection_pool_kw):
        super().__init__(proxy_url=proxy_url, num_pools=num_pools,
                         headers=headers, proxy_headers=proxy_headers, **connection_pool_kw)
        self.pool_classes_by_scheme = {
            "http": TCPKeepAliveHTTPConnectionPool, "https": TCPKeepAliveHTTPSConnectionPool}


class TCPKeepAliveHttpAdapter(HTTPAdapter):

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        self.poolmanager = TCPKeepAlivePoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, **kwargs)


class TCPKeepAliveSession(Session):
    def __init__(self, max_poolsize=100, total_retries=5):
        super().__init__()
        self.mount('https://', TCPKeepAliveHttpAdapter(
            max_retries=Retry(
                total=total_retries,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 504]
            ),
            pool_connections=max_poolsize,
            pool_maxsize=max_poolsize
        ))

        self.mount('http://', TCPKeepAliveHttpAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 504]
            ),
            pool_connections=max_poolsize,
            pool_maxsize=max_poolsize
        ))
