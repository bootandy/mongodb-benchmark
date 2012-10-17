from pymongo import Connection
from contextlib import contextmanager
import datetime
import random
from main import make_string

MONGO_SERVER = 'localhost'
mongo_url = 'mongodb://localhost:27017/performance'
db = Connection(MONGO_SERVER, 27017)['performance']



@contextmanager
def timer():
    pre = datetime.datetime.now()
    yield
    post = datetime.datetime.now()
    s = 'Time: ' + str(post - pre)
    print s

def do_read(l):
    return db.bos1.find({"arrays 15": {'$in': l}})[0]

def do_read_str(l):
    return db.bos1.find({"str 26": {'$in': l}})[0]

"""
Tests the speed of reading an object from the DB when it is 'in' a list somewhere
"""

def read_it():
    # I know this number exists in the DB.
    giant_l = [0.9369193303512304]

    for i in range(1, 100000):
        giant_l.append(random.random())

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
        giant_l.append(main.make_string(100))

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
    #read_it()
    read_it_str()
