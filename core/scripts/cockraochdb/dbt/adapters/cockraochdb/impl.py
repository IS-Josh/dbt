from dbt.adapters.sql import SQLAdapter
from dbt.adapters.cockraochdb import CockraochDBConnectionManager
from dataclasses import dataclass
from dbt.adapters.base import (
    BaseAdapter, available, RelationType, SchemaSearchMap, AdapterConfig
)
class CockraochDBAdapter(SQLAdapter):
    ConnectionManager = CockraochDBConnectionManager
from typing import Dict, List, Optional, Any, Set, Union,Tuple



from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Set, List, Any
from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.connection import Connection
from dbt.adapters.cockraochdb import CockroachDBConnectionManager
from dbt.adapters.cockraochdb import CockroachDBColumn
from dbt.adapters.cockraochdb import CockroachDBRelation
from dbt.dataclass_schema import dbtClassMixin, ValidationError
import dbt.exceptions
import dbt.utils


# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = 'CockroachDB_get_relations'


@dataclass
class CockroachDBIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = None

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/fishtown-analytics/dbt/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.utcnow().isoformat()
        inputs = (self.columns +
                  [relation.render(), str(self.unique), str(self.type), now])
        string = '_'.join(inputs)
        return dbt.utils.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional['CockroachDBIndexConfig']:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            dbt.exceptions.raise_compiler_error(
                f'Could not parse index config: {msg}'
            )
        except TypeError:
            dbt.exceptions.raise_compiler_error(
                f'Invalid index config:\n'
                f'  Got: {raw_index}\n'
                f'  Expected a dictionary with at minimum a "columns" key'
            )


@dataclass
class CockroachDBConfig(AdapterConfig):
    unlogged: Optional[bool] = None
    indexes: Optional[List[CockroachDBIndexConfig]] = None


class CockroachDBAdapter(SQLAdapter):
    Relation = CockroachDBRelation
    ConnectionManager = CockroachDBConnectionManager
    Column = CockroachDBColumn

    AdapterSpecificConfigs = CockroachDBConfig

    @classmethod
    def date_function(cls):
        return 'now()'

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        if database.lower() != expected.lower():
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references not allowed in {} ({} vs {})'
                .format(self.type(), database, expected)
            )
        # return an empty string on success so macros can call this
        return ''

    @available
    def parse_index(self, raw_index: Any) -> Optional[CockroachDBIndexConfig]:
        return CockroachDBIndexConfig.parse(raw_index)

    def _link_cached_database_relations(self, schemas: Set[str]):
        """
        :param schemas: The set of schemas that should have links added.
        """
        database = self.config.credentials.database
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME)

        for (dep_schema, dep_name, refed_schema, refed_name) in table:
            dependent = self.Relation.create(
                database=database,
                schema=dep_schema,
                identifier=dep_name
            )
            referenced = self.Relation.create(
                database=database,
                schema=refed_schema,
                identifier=refed_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(referenced, dependent)

    def _get_catalog_schemas(self, manifest):
        # postgres only allow one database (the main one)
        schemas = super()._get_catalog_schemas(manifest)
        try:
            return schemas.flatten()
        except dbt.exceptions.RuntimeException as exc:
            dbt.exceptions.raise_compiler_error(
                'Cross-db references not allowed in adapter {}: Got {}'.format(
                    self.type(), exc.msg
                )
            )

    def _link_cached_relations(self, manifest):
        schemas: Set[str] = set()
        relations_schemas = self._get_cache_schemas(manifest)
        for relation in relations_schemas:
            self.verify_database(relation.database)
            schemas.add(relation.schema.lower())

        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest):
        super()._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = 'hour'
    ) -> str:
        return f"{add_to} + interval '{number} {interval}'"

    
    @available.parse(lambda *a, **k: (None, None))
    def add_query(
        self,
        sql: str,
        auto_begin: bool = False,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        """Add a query to the current transaction. A thin wrapper around
        ConnectionManager.add_query.

        :param sql: The SQL query to add
        :param auto_begin: If set and there is no transaction in progress,
            begin a new one.
        :param bindings: An optional list of bindings for the query.
        :param abridge_sql_log: If set, limit the raw sql logged to 512
            characters
        """
        return self.connections.add_query(sql, auto_begin, bindings,
                                          abridge_sql_log)