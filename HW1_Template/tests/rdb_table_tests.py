# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.RDBDataTable import RDBDataTable
import logging
import json
import pymysql
import pymysql.cursors

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def t1():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019",
        "cursorclass" : pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)

#t1()

def test_RDB_match_all():
    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }

    tmp = {'nameLast': 'Williams', 'birthCity': 'San Diego'}
    field = ['playerID','birthYear','birthMonth','birthCountry','nameLast', 'birthCity']
    rdb_tbl = RDBDataTable("people", connect_info=connect_info, key_columns=['playerID'])
    find = rdb_tbl.find_by_template(tmp, field_list=field)
    print("Result = \n", json.dumps(find, indent=2))

#test_RDB_match_all()

def test_update_by_template():

    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }

    tmp = {'playerID': 'aardsda01'}
    new_val = {'nameLast': 'shin', 'birthCity':'Seoul'}
    rdb_tbl = RDBDataTable("people", connect_info=connect_info, key_columns=['playerID'])
    find = rdb_tbl.update_by_template(tmp, new_val)
    print("Number of rows updated: ", find)

#test_update_by_template()

def test_insert():
    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }

    rdb_tbl = RDBDataTable("Batting", connect_info=connect_info, key_columns=['playerID','teamID','yearID','stint'])
    new_row = {"playerID":"ss5644", "teamID":"BOS"}

    result = rdb_tbl.insert(new_row)
    print("Number of rows inserted: ",result)

#test_insert()

def test_delete_by_template():

    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }

    rm_val = {"playerID":"willite01", "teamID":"BOS"}
    rdb_tbl = RDBDataTable("Batting", connect_info=connect_info, key_columns=['playerID','teamID','yearID','stint'])
    number_of_deleted_rows = rdb_tbl.delete_by_template(rm_val)
    print("Number of deleted rows: ", number_of_deleted_rows)

#test_delete_by_template()

def test_find_by_key():
    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }

    rdb_tbl = RDBDataTable("Batting", connect_info=connect_info, key_columns=['playerID','teamID','yearID','stint'])
    res = rdb_tbl.find_by_primary_key(key_fields = ['abercda01', 'TRO', '1871', '1'], field_list=['playerID','teamID','yearID','stint','H'])

    print("Find by key result: ", res)

#test_find_by_key()



def test_update_by_key():
    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }
    rdb_tbl = RDBDataTable("Batting", connect_info=connect_info, key_columns=['playerID', 'teamID', 'yearID', 'stint'])
    rows_updated = rdb_tbl.update_by_key(['abercda01', 'TRO', '1871', '1'], {'H': '7', '2B': '1'})
    print("Number of rows updated: ", rows_updated)

#test_update_by_key()

def test_delete_by_key():
    connect_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "ss5644atcolumbia",
        "db": "lahman2019"
    }
    rdb_tbl = RDBDataTable("Batting", connect_info=connect_info, key_columns=['playerID', 'teamID', 'yearID', 'stint'])

    rows_deleted = rdb_tbl.delete_by_key(dict(zip(['playerID', 'teamID', 'yearID', 'stint'],['beaveed01', 'TRO', '1871', '1'])))
    print("Number of rows deleted: ", rows_deleted)

#test_delete_by_key()

