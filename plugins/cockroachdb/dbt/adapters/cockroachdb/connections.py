from multiprocessing import Lock
from contextlib import contextmanager
from typing import Optional , Tuple, Any, Union,List
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.base import Credentials
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions
import dbt.flags
from dbt import flags
from dataclasses import dataclass,field
#from dbt.dataclass_schema import FieldEncoder, dbtClassMixin, StrEnum
import psycopg2


#from dbt.adapters.sql import SQLConnectionManager

import time
import agate
from dbt.helper_types import Port
from dbt.contracts.connection import (
      Connection,
      AdapterResponse
)
drop_lock: Lock = dbt.flags.MP_CONTEXT.Lock()



@dataclass
class CockroachDBCredentials(Credentials):
    host: str
    user: str
    port: Port
    password: str  # on cockroachdb the password is mandatory
    role: Optional[str] = None
    search_path: Optional[str] = None
    keepalives_idle: int = 0  # 0 means to use the default value
    sslmode: Optional[str] = None
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None
    application_name: Optional[str] = 'dbt'

    _ALIASES = {
        'dbname': 'database',
        'pass': 'password'
    }



    @property
    def type(self):
        return 'cockroachdb'

    def _connection_keys(self):
        return ('host', 'port', 'user', 'database', 'schema', 'search_path',
                'keepalives_idle', 'sslmode')



class CockroachDBConnectionManager(SQLConnectionManager):
    TYPE = 'cockroachdb'

    @contextmanager
    # def fresh_transaction(self, name=None):
    #     """On entrance to this context manager, hold an exclusive lock and
    #     create a fresh transaction for redshift, then commit and begin a new
    #     one before releasing the lock on exit.

    #     See drop_relation in RedshiftAdapter for more information.

    #     :param Optional[str] name: The name of the connection to use, or None
    #         to use the default.
    #     """
    #     with drop_lock:
    #         connection = self.get_thread_connection()

    #         if connection.transaction_open:
    #             self.commit()

    #         self.begin()
    #         yield

    #         self.commit()
    #         self.begin()   
    
    def exception_handler(self, sql):
        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug('CockroachDB error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except psycopg2.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e) from e



    

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}
        # we don't want to pass 0 along to connect() as Cockraochdb will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if credentials.keepalives_idle:
            kwargs['keepalives_idle'] = credentials.keepalives_idle

        # psycopg2 doesn't support search_path officially,
        # see https://github.com/psycopg/psycopg2/issues/465
        search_path = credentials.search_path
        if search_path is not None and search_path != '':
            # see https://postgresql.org/docs/9.5/libpq-connect.html
            kwargs['options'] = '-c search_path={}'.format(
                search_path.replace(' ', '\\ '))

        if credentials.sslmode:
            kwargs['sslmode'] = credentials.sslmode

        if credentials.sslcert is not None:
            kwargs["sslcert"] = credentials.sslcert

        if credentials.sslkey is not None:
            kwargs["sslkey"] = credentials.sslkey

        if credentials.sslrootcert is not None:
            kwargs["sslrootcert"] = credentials.sslrootcert

        if credentials.application_name:
            kwargs['application_name'] = credentials.application_name

        try:
            handle = psycopg2.connect(
                dbname=credentials.database,
                user=credentials.user,
                host=credentials.host,
                password=credentials.password,
                port=credentials.port,
                connect_timeout=10,
                **kwargs)
            
            
            if credentials.role:
                handle.cursor().execute('set role {}'.format(credentials.role))

            connection.handle = handle
            connection.state = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a cockroachdb "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        connection_name = connection.name
        try:
            pid = connection.handle.get_backend_pid()
        except psycopg2.InterfaceError as exc:
            # if the connection is already closed, not much to cancel!
            if 'already closed' in str(exc):
                logger.debug(
                    f'Connection {connection_name} was already closed'
                )
                return
            # probably bad, re-raise it
            raise



        sql = "CANCEL QUERY '{}'".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))


    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=connection.name,
                sql=log_sql,
            )
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)
            logger.debug(
                "SQL status: {status} in {elapsed:0.2f} seconds",
                status=self.get_response(cursor),
                elapsed=(time.time() - pre)
            )

            return connection, cursor




    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        message = str(cursor.statusmessage)
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_messsage_strings = [
            part
            for part in status_message_parts
            if not part.isdigit()
        ]
        code = ' '.join(status_messsage_strings)
        return AdapterResponse(
            _message=message,
            code=code,
            rows_affected=rows
        )


    def add_begin_query(self):
        return self.add_query("select 'NOT_ADDING_BEGIN_FOR_CRDB_TESTING'", auto_begin=False)

    def add_commit_query(self):
        return self.add_query('COMMIT', auto_begin=False)

    def begin(self):
        connection = self.get_thread_connection()

        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'In begin, got {connection} - not a Connection!'
                )

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.name))

        self.add_begin_query()

        connection.transaction_open = True
        return connection


    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return response, table


    def commit(self):
        connection = self.get_thread_connection()
        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'In commit, got {connection} - not a Connection!'
                )

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: COMMIT'.format(connection.name))
        self.add_commit_query()

        connection.transaction_open = False

        return connection   