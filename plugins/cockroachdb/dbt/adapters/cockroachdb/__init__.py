from dbt.adapters.cockroachdb.connections import CockroachDBConnectionManager
from dbt.adapters.cockroachdb.connections import CockroachDBCredentials
from dbt.adapters.cockroachdb.relation import CockroachDBColumn  # noqa
from dbt.adapters.cockroachdb.relation import CockroachDBRelation  # noqa: F401
from dbt.adapters.cockroachdb.impl import CockroachDBAdapter


from dbt.adapters.base import AdapterPlugin
from dbt.include import cockroachdb


Plugin = AdapterPlugin(
    adapter=CockroachDBAdapter,
    credentials=CockroachDBCredentials,
    include_path=cockroachdb.PACKAGE_PATH
    ,dependencies=['postgres'])




