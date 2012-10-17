from pymongo import Connection
from contextlib import contextmanager
from collections import defaultdict
import datetime
import random
from main import make_string,timer

MONGO_SERVER = 'localhost'
mongo_url = 'mongodb://localhost:27017/performance'
db = Connection(MONGO_SERVER, 27017)['performance']

user_2_msg = defaultdict(list)

def populate():
    for i in range(1, 10000):
        obj = {}
        obj['user'] = i % 200
        obj['msg'] = make_string(100)
        _id = db.long.insert(obj)
        user_2_msg[i % 200].append(_id)

def load_by_user():
    return list(db.long.find({'user': 0}).limit(5))

def load_by_ids():
    return list(db.long.find({'_id': {'$in': user_2_msg[0]}}).limit(5))

if __name__ == '__main__':
    populate()
    with timer():
        load_by_user()
    with timer():
        load_by_ids()

    # print db.long.find({'user': 0}).limit(5).explain()
    # print db.long.find({'_id': {'$in': user_2_msg[0]}}).limit(5).explain()
