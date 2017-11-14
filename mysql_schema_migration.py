#!/bin/python

# Some of my programming conventions:
# I tend to Camel-Case functions
# I try to prefix objects and variables by type...
#   Lists begin with ls
#   Strings beging with str
#   Dictionaries begin with d
#   and so on

import time
import argparse
import MySQLdb
from google.cloud import spanner
from google.cloud.proto.spanner.v1 import type_pb2
mydb = None
mycur= None
lsFields = []

parser = argparse.ArgumentParser(description='Transfer a table from the local MySQL server to the Cloudspanner instance')
parser.add_argument('--db', help='MySQL DB you are using. A Spanner DB with this name will also be created.')
parser.add_argument('--myuser', help='MySQL user')
parser.add_argument('--mypw', help='MySQL password')
parser.add_argument('--myhost', help='MySQL host. If left blank, localhost is assumed')
parser.add_argument('--table', help='Table whose data you are exporting.')
parser.add_argument('--instance', help='Spanner instance you are importing to')

args = parser.parse_args()

if args.db == None:
    args.db = "all"
if args.instance == None:
    args.instance = "SolomonInstance"
if args.myhost == None:
    args.myhost = "localhost"
if args.myuser == None:
    args.myuser = "changsolomon"
if args.mypw == None:
    args.mypw = ""



def buildDDL(mydb, mytable):
    # Build the DDL string for one single table
    global lsFields
    strNull = ""
    strPri = ""
    strDDL = ""
    lsFields = []
    mycur = mydb.cursor()
    mycur.execute("describe %s"%(mytable))
    for row in mycur.fetchall():
        dFields = {}
        dFields["field"] = row[0]
        if row[1][:3].lower() == "int":
            dFields["type"] = "INT64"
        elif row[1][:5].lower() == "float":
            dFields["type"] = "FLOAT64"
        elif row[1][:4].lower() == "date":
            dFields["type"] = "DATE"
        #else if row[1][:4].lower() == "time":
        #    lsTypes.append("DATETIME")
        elif row[1][:4].lower() == "char":
            dFields["type"] = "STRING(MAX)"
        elif row[1][:7].lower() == "varchar":
            dFields["type"] = "STRING(MAX)"
        elif row[1][:4].lower() == "enum":
            dFields["type"] = "STRING(MAX)"
        else:
            dFields["type"] = "STRING(MAX)"

        dFields["null"] = row[2]
        if row[3] == "PRI":
            strPri += "%s, "%(row[0])
        dFields["default"] = row[4]
        lsFields.append(dFields)

    strDDL = "CREATE TABLE %s ("%(mytable)
    for d in lsFields:
        if d["default"] == "None":
            if d["null"] == "NO":
                strNull = "NOT NULL"
            else:
                strNull = ""
        else:
            # Spanner does not use te DEFAULT keyword, so commenting out this branch for now.
            """
            if d["type"] == "INT64":
                strNull = "DEFAULT %s"%(d["default"]) 
            elif d["type"] == "FLOAT":
                strNull = "DEFAULT %s"%(d["default"]) 
            else:
                # note to self: need to code to escape quotes
                strNull = "DEFAULT '%s'"%(d["default"]) 
            """
            strNull = ""
        strDDL = strDDL + "%s %s %s, "%(d["field"], d["type"], strNull)
        #strReturn = strReturn + "%s %s, "%(d["field"], d["type"])
    # Remove the last space and comma, and close
    strDDL = strDDL[:-2] + ")"
    # In Spanner SQL, "Primary Key" comes *after* field collection
    if strPri == "":
        # Need to code an exception here.  All Spanner table DDLs must have a primary key.
        # If this branch runs, the script will fail.
        return ""
    else:
        # Strip off end comma
        if strPri[-2:] == ", ":
            strPri = strPri[:-2]
        strDDL = strDDL + " PRIMARY KEY (%s)"%(strPri)
    #strReturn = strReturn + ");"

    return strDDL

def ddlStatements(mydb):
    # return (multiple) DDLs in list form to prep for database creation.
    lsDDL = []
    strTmp = ""
    if args.table == None:
        mycur = mydb.cursor()
        mycur.execute("Show tables")
        for row in mycur.fetchall():
            strTmp = buildDDL(mydb, row[0])
            if strTmp != "":
                #Only append if PK is present
                lsDDL.append(buildDDL(mydb, row[0]))
    else:
        # Only build the one specified table
        lsDDL.append(buildDDL(mydb, args.table))
    return lsDDL


def createDatabase(instance_id, database_id, mydb, lsDDL):
    # Creates a database and tables for sample data
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id, ddl_statements=lsDDL)

    operation = database.create()

    print('Waiting for operation to complete...')
    operation.result()

    print('Created database {} on instance {}'.format(
        database_id, instance_id))


def main():
    instance_id = args.instance
    database_id = args.db

    mydb = MySQLdb.connect(host=args.myhost, user=args.myuser, passwd=args.mypw, db=args.db)
    

    print (ddlStatements(mydb))
    createDatabase(instance_id, database_id, mydb, ddlStatements(mydb))
    return

if __name__ == "__main__":
    main()