import redis
import json
from udb import UDB
from udbtype import *

def PersistRedis(udb):
    r = redis.StrictRedis(host='localhost',port=6379, db=0)
    
    count = 0
    # clear all keys with prefix 'db:'
    idx,keys = r.scan(0,'db:*',100)
    while idx != 0:
        count += len(keys)
        with r.pipeline() as pipe:
            for k in keys:
                pipe.delete(k)
            pipe.execute()
        idx,keys = r.scan(idx,'db:*',100)
    logging.info('Clean %d db keys' % count)

    cache = []
    # store users
    for (k,v) in udb.users.items():
        cache.append((str(k), v.to_JSON())) 
        if len(cache) >= 100:
            count += len(cache)
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.sadd('db:user',pair[0])
                    pipe.set('db:user:' + pair[0] + ':info',pair[1])
                pipe.execute()
            cache = []

    if len(cache) > 0:
        count += len(cache)
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.sadd('db:user',pair[0])
                pipe.set('db:user:' + pair[0] + ':info',pair[1])
            pipe.execute()
        cache = []
    logging.info('Store %d users' % count)


    # store groups
    count = 0
    for (k,v) in udb.groups.items():
        cache.append((str(k), v.to_JSON())) 
        if len(cache) >= 100:
            count += len(cache)
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.set('db:group:' + pair[0] + ':info',pair[1])
                    pipe.sadd('db:group',pair[0])
                pipe.execute()
            cache = []

    if len(cache) > 0:
        count += len(cache)
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.set('db:group:' + pair[0] + ':info',pair[1])
                pipe.sadd('db:group',pair[0])
            pipe.execute()
        cache = []
    logging.info('Store %d groups' % count)

    # store companys
    count = 0
    for (k,v) in udb.companys.items():
        cache.append((str(k), v.to_JSON())) 
        if len(cache) >= 100:
            count += len(cache)
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.sadd('db:company',pair[0])
                    pipe.set('db:company:' + pair[0] + ':info',pair[1])
                pipe.execute()
            cache = []

    if len(cache) > 0:
        count += len(cache)
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.sadd('db:company',pair[0])
                pipe.set('db:company:' + pair[0] + ':info',pair[1])
            pipe.execute()
        cache = []
    logging.info('Store %d companys' % count)

    # store agents 
    count = 0
    for (k,v) in udb.agents.items():
        cache.append((str(k), v.to_JSON())) 
        if len(cache) >= 100:
            count += len(cache)
            with r.pipeline() as pipe:
                for pair in cache:
                    pipe.sadd('db:agent',pair[0])
                    pipe.set('db:agent:' + pair[0] + ':info',pair[1])
                pipe.execute()
            cache = []

    if len(cache) > 0:
        count += len(cache)
        with r.pipeline() as pipe:
            for pair in cache:
                pipe.sadd('db:agent',pair[0])
                pipe.set('db:agent:' + pair[0] + ':info',pair[1])
            pipe.execute()
        cache = []
    logging.info('Store %d agents' % count)

    # store user and group relationship
    count = 0
    for (k,groups) in udb.user_group_index.items():
        count += len(groups)
        key = 'db:user:' + str(k) + ':group'
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd(key,g)
            pipe.execute()
    logging.info('Store %d groups of user relationship' % count)

    count = 0
    for (k,users) in udb.group_user_index.items():
        count += len(users)
        key = 'db:group:' + str(k) + ':user'
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd(key,u)
            pipe.execute()
    logging.info('Store %d user of group relationship' % count)

    # store company subs
    count = 0
    for (k,subs) in udb.company_sub_index.items():
        count += len(subs)
        key = 'db:company:' + str(k) + ':subs'
        with r.pipeline() as pipe:
            for s in subs:
                pipe.sadd(key,s)
            pipe.execute()
    logging.info('store %d companys with subs, total subs %d' % (len(udb.company_sub_index),count))

    # store company groups
    count = 0
    for (k,groups) in udb.company_group_index.items():
        count += len(groups)
        key = 'db:company:' + str(k) + ':group'
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd(key,g)
            pipe.execute()
    logging.info('store %d companys with groups, total groups %d' % (len(udb.company_group_index),count))

    # store company users
    count = 0
    for (k,users) in udb.company_user_index.items():
        count += len(users)
        key = 'db:company:' + str(k) + ':user'
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd(key,u)
            pipe.execute()
    logging.info('store %d companys with users, total users %d' % (len(udb.company_user_index),count))

    # store agent subs
    count = 0
    for (k,subs) in udb.agent_sub_index.items():
        count += len(subs)
        key = 'db:agent:' + str(k) + ':subs'
        with r.pipeline() as pipe:
            for s in subs:
                pipe.sadd(key,s)
            pipe.execute()
    logging.info('store %d agents with subs, total subs %d' % (len(udb.agent_sub_index),count))

    # store agent companys
    count = 0
    for (k,companys) in udb.agent_sub_index.items():
        count += len(companys)
        key = 'db:agent:' + str(k) + ':company'
        with r.pipeline() as pipe:
            for c in subs:
                pipe.sadd(key,c)
            pipe.execute()
    logging.info('store %d agents with companys, total companys %d' % (len(udb.agent_company_index),count))

    # store agent groups
    count = 0
    for (k,groups) in udb.agent_group_index.items():
        count += len(groups)
        key = 'db:agent:' + str(k) + ':group'
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd(key,g)
            pipe.execute()
    logging.info('store %d agents with groups, total groups %d' % (len(udb.agent_group_index),count))

    # store agent users
    count = 0
    for (k,users) in udb.agent_user_index.items():
        count += len(users)
        key = 'db:agent:' + str(k) + ':user'
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd(key,u)
            pipe.execute()
    logging.info('store %d agents with users, total users %d' % (len(udb.agent_user_index),count))

    # store user group relationship
    count = 0
    for (k,groups) in udb.user_group_index.items():
        count += len(groups)
        key = 'db:user:' + str(k) + ':group'
        with r.pipeline() as pipe:
            for g in groups:
                pipe.sadd(key,g)
            pipe.execute()
    logging.info('store %d users has group, total groups %d' % (len(udb.user_group_index),count))

    count = 0
    for (k,users) in udb.group_user_index.items():
        count += len(users)
        key = 'db:group:' + str(k) + ':user'
        with r.pipeline() as pipe:
            for u in users:
                pipe.sadd(key,u)
            pipe.execute()
    logging.info('store %d groups has user, total users %d' % (len(udb.group_user_index),count))
