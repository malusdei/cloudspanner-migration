# cloudspanner-migration

So far, there are two files:
  - mysql_schema_migration.py
  - mysql_data_migration.py

They do exactly what they sound like.
Both files take the following arguments:

--myhost (optional) - The MySQL host you are connecting to.  If left blank, it will assume localhost.
--myuser (required) - The MySQL username you are connecting as.
--mypwd (optional) - The MySQL password.  If left blank, it will be blank.
--db (required) - The name of the database on MySQL and Cloud Spanner. The same name will be used for both.
--instance (required) - The Cloud Spanner instance you are exporting to.
--table (optional) - The table to be exported.  If not specified, all tables in the named --db will be exported.

The mysql_schema_migration.py script assumes you have not yet created a database of the same name on the Spanner side yet.
This script will create a new database and the table you specified (or all the tables, if you did not specify).
This script will ignore any tables that do not have a Primary Key.  That is because Spanner tables must have a PK.
This script does not create any indexes in the DDL other than the Primary Key.  This is because users are strongly advised not to have any secondary indexes when they are bulk loading data.

The mysql_data_migration.py tool will copy data for the table you specify (or all tables, if you did not specify.
)
