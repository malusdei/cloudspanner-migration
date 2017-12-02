#!/bin/python

import sys
import time
import argparse
import MySQLdb
import psycopg2
from google.cloud import spanner
from google.cloud.spanner_v1.proto import type_pb2
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
parser.add_argument('--logto', help='Dry run (do not write data), and output to this log instead.')
parser.add_argument('--verbosity', help='How much information to return. Default 1. Values can be 0 - 3')

# I like to define m possible globals, ven if they are not used.
dryrun = 0
fileLog = None

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
if args.logto == None:
    args.logto == ""
else:
    dryrun = 1
    fileLog = open(args.logto, "w+")
if args.verbosity == None:
    args.verbosity == 1

def logTo(strMsg, verbosity):
    global fileLog
    global args
    if verbosity > args.verbosity:
        return
    strMsg = "%s\n"%(str(strMsg))
    if args.logto == None:
        print strMsg
    else:
        fileLog.write(strMsg)
        #fileLog.write("\n")

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

# ======== Postgres Functions ========

def lsPgTables(dbSource):
    # Return a list of Postgres tables to cycle through.
    lsTables = []
    strTmp = ""
    logTo("Copying following tables:", 2)
    if args.table == None:
        pgCur = dbSource.cursor()
        strSQL = "select table_name from information_schema.tables where table_schema = '%s';"%(args.db)
        pgCur.execute(strSQL)
        for row in pgCur.fetchall():
            lsTables.append(row[0])
            logTo(row[0], 2)
    else:
        # Only build the one specified table
        lsTables.append(args.table)
        logTo(args.table, 2)
    return lsTables

def lsPgFields(dbSource, pgTable):
    # Return a list of fields for the specified table
    lsFields = []
    pgCur = dbSource.cursor()
    strSQL = "select column_name from information_schema.columns where table_catalog = '%s' and table_name = '%s'"%(args.db, pgTable)
    logTo(strSQL, 1)
    pgCur.execute(strSQL)
    for row in pgCur.fetchall():
        lsFields.append(row[0])
    if lsFields == []:
        sys.exit("Unable to retrieve column names.  Check to make sure the table exists and the specified user has  permission to read the table.")
    return lsFields

def getPostgresData(dbSource, pgTable):
    lsData = []
    lsItem = []
    pgCur = dbSource.cursor()
    strSQL = "select %s from %s"%(", ".join(lsPgFields(dbSource, pgTable)), pgTable)
    logTo("Getting Postgres Data", 1)
    logTo(strSQL, 1)
    pgCur.execute(strSQL)
    for row in pgCur.fetchall():
        #Sorry, had to strip out all the data type markup code in the fetchall() function output
        lsItem = []
        for r in row:
            lsItem.append(str(r))
        lsData.append(tuple(lsItem))
        logTo(lsItem, 3)
        ## Change in plans: Leave data type in. Screen in Unicode list.
        #lsData.append(row)
    return lsData

# ======== ======== ========

# ======== MySQL Functions ========

def lsMyTables(dbSource):
    # Return a list of MySQL tables to cycle through.
    lsTables = []
    strTmp = ""
    logTo("Copying following tables:", 2)
    if args.table == None:
        mycur = dbSource.cursor()
        mycur.execute("Show tables")
        for row in mycur.fetchall():
            lsTables.append(row[0])
            logTo(row[0], 2)
    else:
        # Only build the one specified table
        lsTables.append(args.table)
        logTo(args.table, 2)
    return lsTables

def lsMyFields(dbSource, mytable):
    # Return a list of fields for the specified table
    lsFields = []
    mycur = dbSource.cursor()
    mycur.execute("describe %s"%(mytable))
    for row in mycur.fetchall():
        lsFields.append(row[0])
    if lsFields == []:
        sys.exit("Unable to retrieve column names.  Check to make sure the table exists and the specified user has 
 permission to read the table.")
    return lsFields

def getMysqlData(dbSource, mytable):
    lsData = []
    lsItem = []
    mycur = dbSource.cursor()
    strSQL = "select %s from %s"%(", ".join(lsMyFields(dbSource, mytable)), mytable)
    logTo("Getting MySQL Data", 1)
    logTo(strSQL, 1)
    mycur.execute(strSQL)
    for row in mycur.fetchall():
        #Sorry, had to strip out all the data type markup code in the fetchall() function output
        lsItem = []
        for r in row:
            lsItem.append(str(r))
        lsData.append(tuple(lsItem))
        logTo(lsItem, 3)
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
    global dryrun
    lsColumns = []
    lsData = []
    cntRecords = 0

    start = time.clock()
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    logTo("Connection established in %f ms"%((time.clock()-start)*1000), 1)

    for strTable in lsTables(dbSource):
        lsData = getData(dbSource, strTable)
        #We'll need to "chunk" the data for import, about 2000 rows each
        cntRecords = 0
        while cntRecords < len(lsData):
            #print (lsUnicodeList(lsData[cntRecords:cntRecords + RECORDSATATIME]))
            logTo("Inserting records into %s..."%(strTable), 2)
            with database.batch() as batch:
                start = time.clock()
                if dryrun == 1:
                    logTo(lsData[cntRecords:cntRecords + RECORDSATATIME], 3)
                    #logTo(lsData[cntRecords], 2)
                else:
                    batch.insert(
                        table=strTable,
                        columns=tuple(lsMyFields(dbSource,strTable)),
                        values=lsUnicodeList(lsData[cntRecords:cntRecords + RECORDSATATIME]))
            logTo('Inserted %i of %i records for table %s in %f ms.'%(cntRecords, len(lsData), strTable, (time.clock()-start)*1000), 1)
            cntRecords = cntRecords + RECORDSATATIME
        logTo('Inserted %i of %i records for table %s.'%(len(lsData), len(lsData), strTable), 1)


def main():
    instance_id = args.instance
    database_id = args.db

    if args.platform == "postgres":
        dbSource = psycopg2.connect(host=args.host, user=args.user, password=args.mypw, dbname=args.db)
    else:
        dbSource = MySQLdb.connect(host=args.host, user=args.user, passwd=args.mypw, db=args.db)
    
    insertData(instance_id, database_id, dbSource)
    return

if __name__ == "__main__":
    main()
