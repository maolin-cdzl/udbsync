import logging

class User:
    def __init__(self):
        self.uid = None
        self.account = None
        self.name = None
        self.company = None
        self.role = None
        self.create = None
        self.service_begin = None
        self.service_end = None

def CreateUser(obj):
    user = User()
    user.uid = obj['uid']
    user.account = obj['account']
    user.name = obj['name']
    user.company = obj['company']
    user.role = obj['role']
    user.create = obj['create']
    user.service_begin = obj['service_begin']
    user.service_end = obj['service_end']

    if user.uid is None or user.uid == 0:
        logging.error('bad uid')
        return None
    if user.account is None:
        logging.error('User %d account is None' % user.uid)
        return None
    if user.company is None:
        logging.error('User %d company is None' % user.uid)
        return None
    if user.name is None:
        user.name = str(user.account)
    if user.role is None:
        user.role = 0
    #if user.create is None:
    #    user.create = ''
    #if user.service_begin is None:
    #    user.service_begin = ''
    #if user.service_end is None:
    #    user.service_end = ''

    return user

class Group:
    def __init__(self):
        self.gid = None
        self.name = None
        self.company = None


def CreateGroup(obj):
    group.gid = obj['gid']
    group.name = obj['name']
    group.company = obj['company']

    if group.gid is None or group.gid == 0:
        logging.error('bad gid')
        return None
    if group.company is None:
        logging.error('Group %d company is None' % group.gid)
        return None
    if group.name is None:
        group.name = str(group.gid)
    return group

class Company:
    def __init__(self):
        self.cid = None
        self.name = None
        self.agent = None
        self.parent = None


def CreateCompany(obj):
    company = Company()
    company.cid = obj['cid']
    company.name = obj['name']
    company.agent = obj['agent']
    company.parent = obj['parent']

    if company.cid is None:
        logging.error('bad company id')
        return None
    if company.agent is None:
        logging.error('Company %d agent is None' % company.cid)
        return None
    if company.parent is None:
        company.parent = 0
    if company.name is None:
        company.name = str(company.cid)

    return company

class Agent:
    def __init__(self):
        self.aid = None
        self.name = None
        self.parent = None

def CreateAgent(obj):
    agent = Agent()
    agent.cid = obj['aid']
    agent.name = obj['name']
    agent.parent = obj['parent']

    if agent.cid is None:
        logging.error('bad agent id')
        return None
    if agent.parent is None:
        agent.parent = 0;
    if agent.name is None:
        agent.name = str(agent.aid)
    return agent





