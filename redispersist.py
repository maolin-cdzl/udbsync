import redis
import json
from udb import UDB
from udbtype import *

def PersistRedis(udb):
    r = redis.StrictRedis(host='localhost')
    
    # clear all keys with prefix 'db:'
    idx,keys = r.scan(0,'db:',100)
    while idx != 0:
        with r.pipeline() as pipe:
            for k in keys:
                pipe.delete(k)
        idx,keys = r.scan(idx,'db:',100)

    cache = []

    # store users
    for (k,v) in udb.users.items():
        cache.append(('db:user:' + str(k) + ':info', json.dumps(v))) 
        if len(cache) >= 100:
            print('Pipelined store %d users' % len(cache))
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.set(pair[0],pair[1])
            cache = []

    if len(cache) > 0:
        print('Pipelined store last %d users' % len(cache))
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.set(pair[0],pair[1])
        cache = []


    # store groups
    for (k,v) in udb.groups.items():
        cache.append(('db:group:' + str(k) + ':info', json.dumps(v))) 
        if len(cache) >= 100:
            print('Pipelined store %d groups' % len(cache))
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.set(pair[0],pair[1])
            cache = []

    if len(cache) > 0:
        print('Pipelined store last %d groups' % len(cache))
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.set(pair[0],pair[1])
        cache = []

    # store companys
    for (k,v) in udb.companys.items():
        cache.append(('db:company:' + str(k) + ':info', json.dumps(v))) 
        if len(cache) >= 100:
            print('Pipelined store %d companys' % len(cache) )
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.set(pair[0],pair[1])
            cache = []

    if len(cache) > 0:
        print('Pipelined store last %d companys' % len(cache))
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.set(pair[0],pair[1])
        cache = []

    # store agents 
    for (k,v) in udb.agents.items():
        cache.append(('db:agent:' + str(k) + ':info', json.dumps(v))) 
        if len(cache) >= 100:
            print('Pipelined store %d agents' % len(cache))
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.set(pair[0],pair[1])
            cache = []

    if len(cache) > 0:
        print('Pipelined store last %d agents' % len(cache))
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.set(pair[0],pair[1])
        cache = []

    # store user and group relationship
    for (k,groups) in udb.user_group_index.items():
        print('Pipelined store %d group of user %d' % (len(groups), k))
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd('db:user:' + str(k) + ':group',g)

    for (k,users) in udb.group_user_index.items():
        print('Pipelined store %d user of group %d' % (len(users), k))
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd('db:group:' + str(k) + ':user',u)

    # store company subs
    print('store %d companys with subs' % len(udb.company_sub_index))
    for (k,subs) in udb.company_sub_index.items():
        with r.pipeline() as pipe:
            for s in subs:
                pipe.sadd('db:company:' + str(k) + ':subs',s)

    # store company groups
    for (k,groups) in udb.company_group_index.items():
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd('db:company:' + str(k) + ':group',g)

    # store company users
    for (k,users) in udb.company_user_index.items():
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd('db:company:' + str(k) + ':user',u)

    # store agent subs
    for (k,subs) in udb.agent_sub_index.items():
        with r.pipeline() as pipe:
            for s in subs:
                pipe.sadd('db:agent:' + str(k) + ':subs',s)

    # store agent companys
    for (k,companys) in udb.agent_sub_index.items():
        with r.pipeline() as pipe:
            for c in subs:
                pipe.sadd('db:agent:' + str(k) + ':company',c)

    # store agent groups
    for (k,groups) in udb.agent_group_index.items():
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd('db:agent:' + str(k) + ':group',g)

    # store agent users
    for (k,users) in udb.agent_user_index.items():
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd('db:agent:' + str(k) + ':user',u)
