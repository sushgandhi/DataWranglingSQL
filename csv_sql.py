import sqlite3
import csv
from pprint import pprint
import os

con = sqlite3.connect("OCM.db")
cur = con.cursor()

def insert_table():
    '''
    Funtion to read the csv and load data into respective tables

    '''
    with open('nodes_tags.csv','rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
    cur.executemany("INSERT INTO nodes_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)
    with open('nodes.csv','rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['id'].decode("utf-8"), i['lat'].decode("utf-8"), i['lon'].decode("utf-8"), i['user'].decode("utf-8"), i['uid'].decode("utf-8"), i['version'].decode("utf-8"), i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]
    cur.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db)
    with open('ways_tags.csv','rb') as fin:
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"), i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
    cur.executemany("INSERT INTO ways_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)
    with open('ways.csv','rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['id'].decode("utf-8"), i['user'].decode("utf-8"), i['uid'].decode("utf-8"), i['version'].decode("utf-8"), i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]
    cur.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db)
    with open('ways_nodes.csv','rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['id'].decode("utf-8"), i['node_id'].decode("utf-8"), i['position'].decode("utf-8")) for i in dr]
    cur.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db)

    con.commit()
    con.close()
if __name__ == '__main__':
    #create_table()
    insert_table()
