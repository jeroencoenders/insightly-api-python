from playhouse.migrate import *
from playhouse.sqlite_ext import SqliteExtDatabase
from peewee import *

DATABASE_FILE = 'insightly30_data/cache.db'
db = SqliteExtDatabase(DATABASE_FILE)
tables = db.get_tables()
migrator = SqliteMigrator(db)

rescan = BooleanField(default=False)

for table in tables:
    if (table != 'databaseclass'):
        migrate(
            migrator.add_column(table, 'dirty', rescan)
        )
