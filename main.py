from contextlib import contextmanager
from pymongo import Connection
import random
import datetime
import cProfile
import copy

MONGO_SERVER = 'localhost'
mongo_url = 'mongodb://localhost:27017/performance'
db = Connection(MONGO_SERVER, 27017)['performance']

# --------------------- object generation -------------------
def make_string():
    s = ''
    for i in range(1, 100):
        s += chr((int)(random.random()*26) + 65)
    return s

def make_big_object(size):
    big_object = {}
    for i in range(1, size):
        big_object[str(i)] = random.random() * 10000

    for i in range(1, size):
        big_object['arrays ' + str(i)] = [random.random(), random.random(), random.random(), random.random(), random.random() ]

    for i in range(1, size):
        big_object['str ' + str(i)] = make_string()

    return big_object

def generate_objs(how_many, how_big):
    return [make_big_object(how_big) for i in range(0, how_many)]

def generate_long_array_obj():
    return {str(i) : build_list() for i in range(0, 10)}

def build_list():
    return [(int)(random.random()*100000) for i in range(0, 10000)]

def randomize_10_cols(obj):
    for i in range(1, 11):
        obj[str(i)] = random.random() * 10000

# --------------------- END object generation -------------------

# ------------------- DB queries: ------------------
def insert_list(obj_list, safe, collection_name='default'):
    [insert(b, safe=safe, collection_name=collection_name) for b in obj_list]

def insert(obj, safe, collection_name='default'):
    db[collection_name].insert(obj, safe=safe)

def update(obj, safe, collection_name='default'):
    db[collection_name].update({"_id": obj["_id"]}, obj, safe=safe)

def update_so_many_fields(obj, safe, how_many=1, collection_name='default'):
    for i in range(1, how_many+1):
        db[collection_name].update({"_id": obj["_id"]}, {'$set': {'str '+str(i): obj[str(i)] }}, safe=safe)

def update_3_pushes(obj, safe, collection_name='default'):
    db[collection_name].update({"_id": obj["_id"]}, {'$push': {'1': 100000}}, safe=safe)
    db[collection_name].update({"_id": obj["_id"]}, {'$push': {'1': 100001}}, safe=safe)
    db[collection_name].update({"_id": obj["_id"]}, {'$push': {'1': 100002}}, safe=safe)

def read(_id, collection_name='default'):
    return db[collection_name].find({"_id":_id}).next()

def read_10_cols(_id, collection_name='default'):
    return db[collection_name].find({"_id":_id},
        {'1': 1, '2': 1, '3': 1, '4': 1, '5': 1,'6': 1, '7': 1, '8': 1, '9': 1, '10':1 }).next()

def read_1_col(_id, collection_name='default'):
    return db[collection_name].find({"_id":_id},
        {'1': 1 }).next()

# --------------------- End DB queries -------------------

# -------------------- Timer methods --------------
def time_it(func, dct):
    pre = datetime.datetime.now()
    result = func(**dct)
    post = datetime.datetime.now()

    s = 'Time: ' + str(post - pre) + ' Func: ' + str(func.func_name)
    if 'safe' in dct:
        s += ' Safe: ' + str(dct['safe'])
    # if result:
    #   s += str(result)
    print s


@contextmanager
def timer():
    pre = datetime.datetime.now()
    yield
    post = datetime.datetime.now()
    s = 'Time: ' + str(post - pre)
    print s

# Alternate timing method:
# http://stackoverflow.com/questions/889900/accurate-timing-of-functions-in-python

# -------------------- END Timer methods --------------

"""
Conclusion: safe=True slows you down ~ 0 -> 100%
"""
def analyze_inserts():
    bos = generate_objs(100, 100)
    bos2 = copy.deepcopy(bos)

    print ' Inserting 100 objects with 300 fields each'
    time_it(insert_list,  {'obj_list':bos2, 'safe': True, 'collection_name': 'bos1'})
    time_it(insert_list,  {'obj_list':bos, 'safe': False, 'collection_name': 'bos2'})

    massive = generate_objs(1, 10000)[0]
    massive2 = copy.deepcopy(massive)

    print ' Inserting 1 object with 30000 fields '
    time_it(insert,  {'obj': massive, 'safe': True, 'collection_name': 'mas1'})
    time_it(insert,  {'obj': massive2, 'safe': False, 'collection_name': 'mas2'})
    print '------------'

"""
Conclusion: Update the whole object - dont try and update only the rows that changed
that will take _much_ longer
"""
def analyze_updates():
    bos = generate_objs(1, 10000)[0]
    print 'Inserting 1 objects with 30000 fields each'
    print 'Next: update 10 of those fields'

    time_it(insert,  {'obj': bos, 'safe': True, 'collection_name': 'up'})
    randomize_10_cols(bos)
    time_it(update,  {'obj': bos, 'safe': True, 'collection_name': 'up'})
    randomize_10_cols(bos)
    time_it(update_so_many_fields,  {'obj': bos, 'safe': True, 'how_many': 10, 'collection_name': 'up'})
    print '------------'


"""
Conclusion: Not worth reading parts of an object - just grab the whole thing.
Object with: 30000 fields. Reading ~ 10 fields takes 25% - 50%  of the time to read the whole object
"""
def analyze_reads():
    bos = generate_objs(1, 10000)[0]

    print 'Inserting 1 objects with 30000 fields each'
    print 'Next: read entire object or just ~ 10 fields of object'

    time_it(insert, {'obj': bos, 'safe': True, 'collection_name': 'reads'})
    time_it(read,  {'_id' : bos['_id'], 'collection_name': 'reads'})
    time_it(read_10_cols, {'_id' : bos['_id'], 'collection_name': 'reads'})

    print '------------'
    # with timer():
    #   insert([bos], True, 'reads')

"""
Conclusion: Here reading 1 of the 10 giant arrays takes ~ 10%  of the time to read the whole thing.
Therefore behaviour with giant arrays is quite different to object with many small arrays or single values

Digging:
    arrays of length < 100  ~ reading 1 col is similar to reading whole 10 col object
    arrays of length ~ 1000 ~ reading 1 col is faster (2-4X faster) than reading whole 10 col object
    arrays of length ~ 10000 ~ reading 1 col is faster (8-10X faster) than reading whole 10 col object
    as array length grows this becomes more pronounced
"""
def analyze_reads_long_array():
    bos = generate_long_array_obj()

    print 'Inserting 1 objects with a few fields each field a very long array'
    print 'Next: read entire object or just ~ 1 fields of object'

    time_it(insert,     {'obj': bos, 'safe': True, 'collection_name': 'read_l'})
    time_it(read,       {'_id': bos['_id'], 'collection_name': 'read_l'})
    time_it(read_1_col, {'_id' : bos['_id'], 'collection_name': 'read_l'})

    print '------------'

"""
Conclusion: Updating only the column that changed can save time if arrays length > 1000.
If you save() the whole object then changing 1 field takes as long as rewriting all of the fields with different values.
Updating 1 of the 10 giant arrays saves time compared to saving the whole object - On SSD ubuntu. Not on macbook pro.
"""
def analyze_updates_long_array():
    bos = generate_long_array_obj()

    print 'Inserting 1 objects with a few fields each one a very long array'
    print 'Next: push 3 elements on to the end of one of the arrays'

    time_it(insert,  {'obj': [bos], 'safe': True, 'collection_name': 'up_l'})

    completly_new = generate_long_array_obj()
    completly_new['_id'] = bos['_id']

    bos['1'].append('99999')
    bos['1'].append('999999')
    bos['1'].append('9999999')

    print 'Append 3 items to 1 array and full update'
    time_it(update,  {'obj': bos, 'safe': True, 'collection_name': 'up_l'})

    print 'Pushing 3 items at the DB level:'
    time_it(update_3_pushes,  {'obj': bos, 'safe': True, 'collection_name': 'up_l'})

    #bos = read(bos['_id'], 'up_l')

    print 'Change 1 column: full update'
    bos['1'] = build_list()
    time_it(update,  {'obj': bos, 'safe': True, 'collection_name': 'up_l'})

    print 'Change all columns: full update'
    time_it(update,  {'obj': completly_new, 'safe': True, 'collection_name': 'up_l'})

    print 'Add single column: full update'
    completly_new['new_field'] = 'hello'
    time_it(update,  {'obj': completly_new, 'safe': True, 'collection_name': 'up_l'})

    print 'Update 1 column at the DB level'
    completly_new['1'] = build_list()
    time_it(update_so_many_fields,  {'obj': completly_new, 'safe': True, 'how_many': 1, 'collection_name': 'up_l'})

    print '------------'

"""
(As expected) Doing this really will wipe out the old object - you will replace it with the
small sub_object that you read out of the database
"""
def analyze_partial_update():
    bos = generate_objs(1, 1000)[0]

    db['reads'].insert(bos, safe=True)
    new_bos = db['reads'].find({"_id": bos['_id']}, {'str 1': 1, 'str 2': 1 } ).next()
    new_bos['str 1'] = 'EDITED'
    db['reads'].update({"_id": new_bos["_id"]}, new_bos, safe=True)


if __name__ == '__main__':
    analyze_inserts()
    analyze_updates()
    analyze_reads()

    analyze_reads_long_array()
    analyze_updates_long_array()

    #analyze_partial_update()
    #cProfile.runctx('analyze_reads()', globals(), locals())
