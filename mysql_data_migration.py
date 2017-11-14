#!/bin/python

import time
import argparse
import MySQLdb
import psycopg2
from google.cloud import spanner
from google.cloud.proto.spanner.v1 import type_pb2
RECORDSATATIME = 2000
dbSource = None
mycur= None

parser = argparse.ArgumentParser(description='Transfer data from a MySQL server to the Cloudspanner instance')
parser.add_argument('--db', help='MySQL DB you are using. It is assumed that a Spanner DB with this name already exists.')
parser.add_argument('--user', help='MySQL user')
parser.add_argument('--mypw', help='MySQL password')
parser.add_argument('--host', help='MySQL host. If left blank, localhost is assumed')
parser.add_argument('--table', help='Table whose data you are exporting.')
parser.add_argument('--instance', help='Spanner instance you are importing to. If blank, SolomonInstance is assumed.')
parser.add_argument('--platform', help='Source platform you are migrating from. "postgres" or "mysql"')

args = parser.parse_args()
if args.db == None:
    args.db = "all"
if args.instance == None:
    args.instance = "SolomonInstance"
if args.platform == None:
    args.platform = "mysql"
if args.host == None:
    args.host = "localhost"
if args.user == None:
    if args.platform == "postgres":
        args.user = "postgres"
    else:
        args.user = "root"
if args.mypw == None:
    args.mypw = ""


def lsUnicodeList(mylist):
    # Receives a list of tuples, and forces them to be numbers, strings, or nothing.
    lsUnicodeList = []
    tpTemp = []
    for mytuple in mylist:
        tpTemp = []
        for t in mytuple:
            if str(t).replace(".","").isdigit():
                tpTemp.append(float(t) if '.' in t else int(t))
            elif t.lower() in ["none", "null"]:
                 tpTemp.append(None)
            else:
                tpTemp.append(t.decode("latin1", "replace"))
        lsUnicodeList.append(tuple(tpTemp))
    return lsUnicodeList

def lsPgTables(dbSource):
    # Return a list of Postgres tables to cycle through.
    lsTables = []
    strTmp = ""
    if args.table == None:
        pgcur = dbSource.cursor()
        pgcur.execute("select table_name from information_schema.tables where table_schema = '%s';"%(args.db))
        for row in pgcur.fetchall():
            lsTables.append(row[0])
    else:
        # Only build the one specified table
        lsTables.append(args.table)
    return lsTables

def lsPgFields(dbSource, mytable):
    # Return a list of fields for the specified table
    lsFields = []
    pgcur = dbSource.cursor()
    pgcur.execute("select * from information_schema.columns where table_schema = '%s' and table_name = '%s'"%(args.db, mytable))
    for row in pgcur.fetchall():
        lsFields.append(row[0])
    return lsFields

def getPostgresData(dbSource, mytable):
    lsData = []
    lsItem = []
    mycur = dbSource.cursor()
    print "select %s from %s"%(", ".join(lsMyFields(dbSource, mytable)), mytable)
    mycur.execute("select %s from %s"%(", ".join(lsMyFields(dbSource, mytable)), mytable))
    for row in mycur.fetchall():
        #Sorry, had to strip out all the data type markup code in the fetchall() function output
        lsItem = []
        for r in row:
            lsItem.append(str(r))
        lsData.append(tuple(lsItem))
        ## Change in plans: Leave data type in. Screen in Unicode list.
        #lsData.append(row)
    return lsData

def lsMyTables(dbSource):
    # Return a list of MySQL tables to cycle through.
    lsTables = []
    strTmp = ""
    if args.table == None:
        mycur = dbSource.cursor()
        mycur.execute("Show tables")
        for row in mycur.fetchall():
            lsTables.append(row[0])
    else:
        # Only build the one specified table
        lsTables.append(args.table)
    return lsTables

def lsMyFields(dbSource, mytable):
    # Return a list of fields for the specified table
    lsFields = []
    mycur = dbSource.cursor()
    mycur.execute("describe %s"%(mytable))
    for row in mycur.fetchall():
        lsFields.append(row[0])
    return lsFields

def getMysqlData(dbSource, mytable):
    lsData = []
    lsItem = []
    mycur = dbSource.cursor()
    print "select %s from %s"%(", ".join(lsMyFields(dbSource, mytable)), mytable)
    mycur.execute("select %s from %s"%(", ".join(lsMyFields(dbSource, mytable)), mytable))
    for row in mycur.fetchall():
        #Sorry, had to strip out all the data type markup code in the fetchall() function output
        lsItem = []
        for r in row:
            lsItem.append(str(r))
        lsData.append(tuple(lsItem))
        ## Change in plans: Leave data type in. Screen in Unicode list.
        #lsData.append(row)
    return lsData

def lsTables(dbSource):
    # Postgres or MySQL.  Splitting here leaves room for Cassandra, etc.
    if args.platform == "postgres":
        return lsPgTables(dbSource)
    else:
        return lsMyTables(dbSource)

def getData(dbSource, strTable):
    # Postgres or MySQL.  Splitting here leaves room for Cassandra, etc.
    if args.platform == "postgres":
        return getPostgresData(dbSource, strTable)
    else:
        return getMysqlData(dbSource, strTable)

def insertData(instance_id, database_id, dbSource):
    lsColumns = []
    lsData = []
    cntRecords = 0

    start = time.clock()
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    print("Connection established in %f ms"%((time.clock()-start)*1000))

    for strTable in lsTables(dbSource):
        lsData = getData(dbSource, strTable)
        #We'll need to "chunk" the data for import, about 2000 rows each
        cntRecords = 0
        while cntRecords < len(lsData):
            #print (lsUnicodeList(lsData[cntRecords:cntRecords + RECORDSATATIME]))
            with database.batch() as batch:
                start = time.clock()
                batch.insert(
                    table=strTable,
                    columns=tuple(lsMyFields(dbSource,strTable)),
                    values=lsUnicodeList(lsData[cntRecords:cntRecords + RECORDSATATIME]))
            print('Inserted %i of %i records for table %s in %f ms.'%(cntRecords, len(lsData), strTable, (time.clock()-start)*1000))
            cntRecords = cntRecords + RECORDSATATIME
        print('Inserted %i of %i records for table %s.'%(len(lsData), len(lsData), strTable))


def main():
    instance_id = args.instance
    database_id = args.db

    if args.platform == "postgres":
        dbSource = psycopg2.connect(hostname=args.host, user=args.user, password=args.mypw, db=args.db)
    else:
        dbSource = MySQLdb.connect(host=args.host, user=args.user, passwd=args.mypw, db=args.db)
    
    insertData(instance_id, database_id, dbSource)
    return

if __name__ == "__main__":
    main()