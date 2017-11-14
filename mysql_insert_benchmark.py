#!/bin/python
# Different from the original migration tool in that it draw from a localhost MySQL DB, and insert into
# a target MySQL or Postgres DB.
# It is assumed that the data source is local MySQL database, root user, without a password.
# It is assumed that the database name and table name exist on both the source and the target.
# It is assumed that the table on the target has the same structure, but is empty.
# If you are targeting to Postgres, it is assumed that the target user has no password.

# For timing purposes specifically.

import time
import argparse
import MySQLdb
import psycopg2

RECORDSATATIME = 3000
dbSource = None
curSource= None

parser = argparse.ArgumentParser(description='Transfer data from a MySQL server to the Cloudspanner instance')
parser.add_argument('--db', help='MySQL DB you are using. It is assumed that a Spanner DB with this name already exists.')
parser.add_argument('--myuser', help='MySQL user')
parser.add_argument('--mypw', help='MySQL password')
parser.add_argument('--myhost', help='MySQL host. If left blank, localhost is assumed')
parser.add_argument('--table', help='Table whose data you are exporting.')
parser.add_argument('--instance', help='"mysql" or "postgres"')

args = parser.parse_args()
if args.db == None:
    args.db = "all"
if args.instance == None:
    args.instance = "mysql"
if args.myhost == None:
    args.myhost = "localhost"
if args.myuser == None:
    args.myuser = "changsolomon"
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

def lsTables(dbSource):
    # Return a list of tables to cycle through.
    lsTables = []
    strTmp = ""
    if args.table == None:
        curSource = dbSource.cursor()
        curSource.execute("Show tables")
        for row in curSource.fetchall():
            lsTables.append(row[0])
    else:
        # Only build the one specified table
        lsTables.append(args.table)
    return lsTables

def lsFields(dbSource, mytable):
    # Return a list of fields for the specified table
    lsFields = []
    curSource = dbSource.cursor()
    curSource.execute("describe %s"%(mytable))
    for row in curSource.fetchall():
        lsFields.append(row[0])
    return lsFields

def getMysqlData(dbSource, mytable):
    lsData = []
    lsItem = []
    curSource = dbSource.cursor()
    #print "select %s from %s"%(", ".join(lsFields(dbSource, mytable)), mytable)
    curSource.execute("select %s from %s"%(", ".join(lsFields(dbSource, mytable)), mytable))
    for row in curSource.fetchall():
        #Sorry, had to strip out all the data type markup code in the fetchall() function output
        lsItem = []
        for r in row:
            lsItem.append(str(r))
        lsData.append(tuple(lsItem))
        ## Change in plans: Leave data type in. Screen in Unicode list.
        #lsData.append(row)
    return lsData

def strTupleToString(lsData):
    # Converts a list to a string that can be passed in through a SQL Values keyword
    strVal = "VALUES "
    for tupFields in lsData:
        strTemp = ""
        for t in tupFields:
            if str(t).replace(".","").isdigit():
                strTemp = "%s %s, "%(strTemp, str(t))
            elif t.lower() in ["none", "null"]:
                strTemp = "%s NULL, "%(strTemp)
            else:
                strTemp = "%s \"%s\", "%(strTemp, str(t).replace('\"','\\"'))
        strVal = "%s (%s), "%(strVal, strTemp[0:-2])
    return strVal[0:-2]

def insertData(dbSource):
    lsColumns = []
    lsData = []
    cntRecords = 0

    start = time.clock()
    # MySQLdb and psycopg2 have same calls for the APIs for our requirements.
    if args.instance == "mysql":
        dbTarget = MySQLdb.connect(host=args.myhost, user=args.myuser, passwd=args.mypw, db=args.db)
    else:
        # Otherwise, Postgres
        dbTarget = psycopg2.connect(hostname=args.myhost, user=args.myuser, db=args.db)
    curTarget = dbTarget.cursor()
    print("-- Connection established in %f ms"%((time.clock()-start)*1000))

    for strTable in lsTables(dbSource):
        lsData = getMysqlData(dbSource, strTable)
        #We'll need to "chunk" the data for import, about 2000 rows each
        # Cloudspanner maxes our at about 3K rows.  x50 bytes per row, is about 150K per statement.
        cntRecords = 0
        while cntRecords < len(lsData):
            start = time.clock()
            sqlInsert = "INSERT INTO test.%s (%s) %s"%(strTable, ", ".join(lsFields(dbSource, strTable)), strTupleToString(lsData[cntRecords:cntRecords + RECORDSATATIME]))
            #print (sqlInsert)
            curTarget.execute(sqlInsert)
            dbTarget.commit()
            print('-- Inserted %i of %i records for table %s in %f ms.'%(cntRecords, len(lsData), strTable, (time.clock()-start)*1000))
            cntRecords = cntRecords + RECORDSATATIME
        print('-- Inserted %i of %i records for table %s.'%(len(lsData), len(lsData), strTable))


def main():

    dbSource = MySQLdb.connect(host=args.myhost, user="root", passwd="", db=args.db)
    
    insertData(dbSource)
    return

if __name__ == "__main__":
    main()