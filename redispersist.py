import logging
import redis
import json
from udb import UDB
from udbtype import *

def PersistRedis(udb):
    r = redis.StrictRedis(host='localhost',port=6379, db=0)
    
    count = 0
    pipecount = 0

    logging.info('Start persist to redis')
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


    # store users
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,v) in udb.users.items():
            key = str(k)
            pipe.sadd('db:user',key)
            pipe.set('db:user:' + key + ':company',v.company)
            pipe.set('db:user:' + key + ':info',v.to_JSON())
            pipecount += 3
            count += 1
            if pipecount >= 100:
                pipe.execute()
                pipecount = 0
        if pipecount > 0:
            pipe.execute()

    logging.info('Store %d users' % count)


    # store groups
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,v) in udb.groups.items():
            key = str(k)
            pipe.sadd('db:group',key)
            pipe.set('db:group:' + key + ':info',v.to_JSON())
            pipecount += 2
            count += 1
            if pipecount >= 100:
                pipe.execute()
                pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('Store %d groups' % count)

    # store companys
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,v) in udb.companys.items():
            key = str(k)
            pipe.sadd('db:company',key)
            pipe.set('db:company:' + key + ':agent', v.agent)
            pipe.set('db:company:' + key + ':info',v.to_JSON())
            pipecount += 3
            count += 1
            if pipecount >= 100:
                pipe.execute()
                pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('Store %d companys' % count)

    # store agents 
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,v) in udb.agents.items():
            key = str(k)
            pipe.sadd('db:agent',key)
            pipe.set('db:agent:' + key + ':info',v.to_JSON())
            pipecount += 2
            count += 1
            if pipecount >= 100:
                pipe.execute()
                pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('Store %d agents' % count)

    # store user and group relationship
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,groups) in udb.user_group_index.items():
            count += len(groups)
            key = 'db:user:' + str(k) + ':group'
            for g in groups:
                pipe.sadd(key,g)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('Store %d groups of user relationship' % count)

    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,users) in udb.group_user_index.items():
            count += len(users)
            key = 'db:group:' + str(k) + ':user'
            for u in users:
                pipe.sadd(key,u)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0 :
            pipe.execute()
    logging.info('Store %d user of group relationship' % count)

    # store company subs
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,subs) in udb.company_sub_index.items():
            count += len(subs)
            key = 'db:company:' + str(k) + ':subs'
            for s in subs:
                pipe.sadd(key,s)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d companys with subs, total subs %d' % (len(udb.company_sub_index),count))

    # store company groups
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,groups) in udb.company_group_index.items():
            count += len(groups)
            key = 'db:company:' + str(k) + ':group'
            for g in groups:
                pipe.sadd(key,g)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d companys with groups, total groups %d' % (len(udb.company_group_index),count))

    # store company users
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,users) in udb.company_user_index.items():
            count += len(users)
            key = 'db:company:' + str(k) + ':user'
            for u in users:
                pipe.sadd(key,u)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d companys with users, total users %d' % (len(udb.company_user_index),count))

    # store agent subs
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,subs) in udb.agent_sub_index.items():
            count += len(subs)
            key = 'db:agent:' + str(k) + ':subs'
            for s in subs:
                pipe.sadd(key,s)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d agents with subs, total subs %d' % (len(udb.agent_sub_index),count))

    # store agent companys
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,companys) in udb.agent_sub_index.items():
            count += len(companys)
            key = 'db:agent:' + str(k) + ':company'
            for c in subs:
                pipe.sadd(key,c)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d agents with companys, total companys %d' % (len(udb.agent_company_index),count))

    # store agent groups
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,groups) in udb.agent_group_index.items():
            count += len(groups)
            key = 'db:agent:' + str(k) + ':group'
            for g in groups:
                pipe.sadd(key,g)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0 :
            pipe.execute()
    logging.info('store %d agents with groups, total groups %d' % (len(udb.agent_group_index),count))

    # store agent users
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,users) in udb.agent_user_index.items():
            count += len(users)
            key = 'db:agent:' + str(k) + ':user'
            for u in users:
                pipe.sadd(key,u)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d agents with users, total users %d' % (len(udb.agent_user_index),count))

    # store user group relationship
    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,groups) in udb.user_group_index.items():
            count += len(groups)
            key = 'db:user:' + str(k) + ':group'
            for g in groups:
                pipe.sadd(key,g)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d users has group, total groups %d' % (len(udb.user_group_index),count))

    count = 0
    pipecount = 0
    with r.pipeline() as pipe:
        for (k,users) in udb.group_user_index.items():
            count += len(users)
            key = 'db:group:' + str(k) + ':user'
            for u in users:
                pipe.sadd(key,u)
                pipecount += 1
                if pipecount >= 100:
                    pipe.execute()
                    pipecount = 0
        if pipecount > 0:
            pipe.execute()
    logging.info('store %d groups has user, total users %d' % (len(udb.group_user_index),count))

    logging.info('Persist to redis done')
