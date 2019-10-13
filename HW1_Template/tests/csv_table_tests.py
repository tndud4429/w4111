
from src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")

def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))

def test_init():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    print("Loaded table = \n", csv_tbl)

def test_match():

    row = {"cool":'yes', "bd":'no'}
    t = {"cool": 'yes'}
    result = CSVDataTable.matches_template(row,t)
    print("Matching by template result: \n", result)

def test_match_all_by_template():
    tmp = {'nameLast': 'Williams', 'birthCity': 'San Diego'}

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    result = csv_tbl.find_by_template(tmp)
    print("Matching all by templates result: \n",json.dumps(result, indent=2))

def test_match_all_by_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'yearID', 'stint', 'teamID']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', '1960', '1', 'BOS']
    csv_tbl = CSVDataTable("Batting.csv", connect_info, key_columns=key_cols)
    result = csv_tbl.find_by_primary_key(key_vals)
    print("Maching all by key result: \n",json.dumps(result, indent=2))

def test_insert_good():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    new_info =  {
        "playerID": "willitr01",
        "birthYear": "1992",
        "birthMonth": "4",
        "birthDay": "25",
        "birthCountry": "USA",
        "birthState": "CA",
        "birthCity": "San Diego",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "Trevor",
        "nameLast": "Williams",
        "nameGiven": "Trevor Anthony",
        "weight": "230",
        "height": "75",
        "bats": "R",
        "throws": "R",
        "debut": "2016-09-07",
        "finalGame": "2018-09-27",
        "retroID": "willt002",
        "bbrefID": "willitr01"
    }

    csv_tbl = CSVDataTable("people_test.csv", connect_info, None)
    result = csv_tbl.insert(new_info)
    print("Number of inserted row = ", result)
    result1 = csv_tbl.find_by_template(new_info)
    print("The new row inserted: \n", json.dumps(result1, indent=2))

def test_insert_bad():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    new_info =  {
        "playerID": "ss5644",
        "birthYear": "1981",
        "birthMonth": "12",
        "birthDay": "27",
        "birthCountry": "USA",
        "birthState": "CO",
        "birthCity": "Denver",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "David",
        "nameLast": "Aardsma",
        "nameGiven": "David Allan",
        "weight": "215",
        "height": "75",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "aardd001",
        "bbrefID": "aardsda01",
        "Unknown" : "column"
    }

    csv_tbl = CSVDataTable("people_test.csv", connect_info, None)
    result = csv_tbl.insert(new_info)
    print("Number of inserted row = ", result)
    result1 = csv_tbl.find_by_template(new_info)
    print("The new row inserted: \n", json.dumps(result1, indent=2))

def test_delete_by_template_good():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    del_info = {"birthYear": "1981",
                "birthMonth": "12",
                "birthDay": "27"}
    csv_tbl = CSVDataTable("people_test.csv", connect_info, None)
    result = csv_tbl.delete_by_template(del_info)
    print("Number of rows deleted by template(testing good):  ", result)

def test_delete_by_template_bad():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    del_info = {"birthYear": "1980",
                "birthMonth": "0",
                "birthDay": "27"}
    csv_tbl = CSVDataTable("people_test.csv", connect_info, None)
    result = csv_tbl.delete_by_template(del_info)
    print("Number of rows deleted by template(testing bad): ", result)

def test_delete_by_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    key_cols = ['playerID']
    key_vals = ["aardsda01"]
    csv_tbl = CSVDataTable("people_test.csv", connect_info, key_columns=key_cols)
    result = csv_tbl.delete_by_key(dict(zip(key_cols, key_vals)))
    print("Number of rows deleted by key: ", result)

def test_update_by_template_good():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    tmp = {
        "playerID": "aardsda01",
        "birthYear": "1981",
        "birthMonth": "12",
        "birthDay": "27",
        "birthCountry": "USA",
        "birthState": "CO",
        "birthCity": "Denver",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "David",
        "nameLast": "Aardsma",
        "nameGiven": "David Allan",
        "weight": "215",
        "height": "75",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "aardd001",
        "bbrefID": "aardsda01"
    }
    new_info = {
        "playerID": "ss5644",
        "birthYear": "1993",
        "birthMonth": "5",
        "birthDay": "19",
        "birthCountry": "Korea",
        "birthState": "Seoul",
        "birthCity": "Somewhere",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "Soo",
        "nameLast": "Shin",
        "nameGiven": "Soo Young Shin",
        "weight": "I dont know",
        "height": "I dont know",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "ss5644",
        "bbrefID": "ss5644"
    }

    csv_tbl = CSVDataTable("people_test.csv", connect_info, None)
    result = csv_tbl.update_by_template(tmp, new_info)
    print("Number of rows updated by template(testing good): ", result)

def test_update_by_template_bad():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    tmp = {
        "playerID": "aardsda01",
        "birthYear": "1981",
        "birthMonth": "12",
        "birthDay": "27",
        "birthCountry": "USA",
        "birthState": "CO",
        "birthCity": "Denver",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "David",
        "nameLast": "Aardsma",
        "nameGiven": "David Allan",
        "weight": "215",
        "height": "75",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "aardd001",
        "bbrefID": "aardsda01"
    }
    new_info = {
        "playerID": "ss5644",
        "birthYear": "1993",
        "birthMonth": "5",
        "birthDay": "19",
        "birthCountry": "Korea",
        "birthState": "Seoul",
        "birthCity": "Somewhere",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "Soo",
        "nameLast": "Shin",
        "nameGiven": "Soo Young Shin",
        "weight": "I dont know",
        "height": "I dont know",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "ss5644",
        "bbrefID": "ss5644",
        "hi" : "hi"
    }

    csv_tbl = CSVDataTable("people_test.csv", connect_info, None)
    result = csv_tbl.update_by_template(tmp, new_info)
    print("Number of rows updated by template(testing bad): ", result)

def test_update_by_key_bad():

    connect_info = {
        "directory": data_dir,
        "file_name": "People_test.csv"
    }

    tmp = {
        "playerID": "aardsda01",
        "birthYear": "1981",
        "birthMonth": "12",
        "birthDay": "27",
        "birthCountry": "USA",
        "birthState": "CO",
        "birthCity": "Denver",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "David",
        "nameLast": "Aardsma",
        "nameGiven": "David Allan",
        "weight": "215",
        "height": "75",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "aardd001",
        "bbrefID": "aardsda01"
    }
    new_info = {
        "playerID": "ss5644",
        "birthYear": "1993",
        "birthMonth": "5",
        "birthDay": "19",
        "birthCountry": "Korea",
        "birthState": "Seoul",
        "birthCity": "Somewhere",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "Soo",
        "nameLast": "Shin",
        "nameGiven": "Soo Young Shin",
        "weight": "I dont know",
        "height": "I dont know",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "ss5644",
        "bbrefID": "ss5644",
        "extra" : "column"
    }
    key_cols = ['playerID']
    key_val = ['aardsda01']
    csv_tbl = CSVDataTable("people_test.csv", connect_info, key_columns=key_cols)
    result = csv_tbl.update_by_key(key_val, new_info)
    print("Number of rows updated by key(testing bad): ", result)

t_load()
test_match()
test_match_all_by_template()
test_match_all_by_key()
test_insert_good()
test_insert_bad()
test_delete_by_template_good()
test_delete_by_template_bad()   ####Bad input will result the same data table without any deletion
test_delete_by_key()       ####Bad input will result the same data table without any deletion
test_update_by_template_good()  ###Not required for the Assignment
test_update_by_template_bad()  ###Not required for the Assignment
test_update_by_key_bad()   ###Not required for the Assignment