from dbt.adapters.cockraochdb.connections import CockraochDBConnectionManager
from dbt.adapters.cockraochdb.connections import CockraochDBCredentials
from dbt.adapters.cockraochdb.impl import CockraochDBAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import cockraochdb


Plugin = AdapterPlugin(
    adapter=CockraochDBAdapter,
    credentials=CockraochDBCredentials,
    include_path=cockraochdb.PACKAGE_PATH)
