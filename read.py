from pymongo import Connection
from contextlib import contextmanager
import datetime
import random
from main import make_string,timer

MONGO_SERVER = 'localhost'
mongo_url = 'mongodb://localhost:27017/performance'
db = Connection(MONGO_SERVER, 27017)['performance']


def do_read(l):
    return list(db.bos1.find({"arrays 15": {'$in': l}}))

def do_read_str(l):
    return list(db.bos1.find({"str 26": {'$in': l}}))

"""
Tests the speed of reading an object from the DB when it is 'in' a list somewhere
"""

def read_it():
    # I know this number exists in the DB.
    giant_l = [0.9369193303512304]

    for i in range(1, 100000):
        giant_l.append(random.random())

    print "reading on double in array"
    with timer():
        results = do_read(giant_l[0:1])

    with timer():
        results = do_read(giant_l[0:10])
    with timer():
        results = do_read(giant_l[0:100])
    with timer():
        results = do_read(giant_l[0:1000])
    with timer():
        results = do_read(giant_l[0:10000])
    with timer():
        results = do_read(giant_l[0:100000])
    #print results

def read_it_str():
    # I know this string exists in the DB.
    giant_l = ["RGOSECGLXZHOSZRTNUYOLFGETJIGMYWKJJSGTKHJFTJNPOFKESAEMKELHXYXUXLMRBQWKQUANATQXKINKXMYKFAAIBTXVLDYJDO"]

    for i in range(1, 100000):
        giant_l.append(make_string(100))

    print "reading on strings"
    with timer():
        results = do_read_str(giant_l[0:1])

    with timer():
        results = do_read_str(giant_l[0:10])
    with timer():
        results = do_read_str(giant_l[0:100])
    with timer():
        results = do_read_str(giant_l[0:1000])
    with timer():
        results = do_read_str(giant_l[0:10000])
    with timer():
        results = do_read_str(giant_l[0:100000])

if __name__ == '__main__':
    with timer():
        do_read([0])
        print "warm up the timer"

    read_it()
    #read_it_str()
