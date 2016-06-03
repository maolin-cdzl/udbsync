from sets import Set
import logging
from udbtype import *

class UDB :
    def __init__(self):
        self.users = {}
        self.groups = {}
        self.companys = {}
        self.agents = {}

        self.user_group_index = {}
        self.group_user_index = {}

        self.company_sub_index = {}
        self.company_group_index = {}
        self.company_user_index = {}

        self.agent_sub_index = {}
        self.agent_company_index = {}
        self.agent_group_index = {}
        self.agent_user_index = {}

    def addAgent(self,agent):
        if agent is None:
            return
        self.agents[agent.aid] = agent
        if agent.parent is not None:
            index = self.agent_sub_index.get(agent.parent,None)
            if index is None:
                index = Set()
                self.agent_sub_index[agent.parent] = index
            index.add(agent.aid)


    def addCompany(self,company):
        if company is None:
            return
        self.companys[company.cid] = company
        if company.parent is not None:
            index = self.company_sub_index.get(company.parent,None)
            if index is None:
                index = Set()
                self.company_sub_index[company.parent] = index
            index.add(company.cid)

        if company.agent is None:
            logging.error('Company %d has no agent' % company.cid )
            return

        index = self.agent_company_index.get(company.agent,None)
        if index is None:
            index = Set()
            self.agent_company_index[company.agent] = index
        index.add(company.agent)

    def addGroup(self,group):
        if group is None:
            return
        self.groups[group.gid] = group
        if group.company is None:
            logging.error('Group %d has no company' % group.gid)
            return

        index = self.company_group_index.get(group.company,None)
        if index is None:
            index = Set()
            self.company_group_index[group.company] = index
        index.add(group.gid)

        company = self.companys.get(group.company,None)
        if company is None:
            logging.error('Group %d \'s company %d is not exists' % (group.gid,group.company))
            return

        if company.agent is not None:
            index = self.agent_group_index.get(company.agent,None)
            if index is None:
                index = Set()
                self.agent_group_index[company.agent] = index
            index.add(group.gid)
            
                

    def addUser(self,user):
        if user is None:
            return
        self.users[user.uid] = user
        if user.company is None:
            logging.error('User %d has no company' % user.uid)
            return

        index = self.company_user_index.get(user.company,None)
        if index is None:
            index = Set()
            self.company_user_index[user.company] = index
        index.add(user.uid)

        company = self.companys.get(user.company,None)
        if company is None:
            logging.error('User %d \'s company %d is not exists' % (user.uid,user.company))
            return
        if company.agent is None:
            return
        index = self.agent_user_index.get(company.agent,None)
        if index is None:
            index = Set()
            self.agent_user_index[company.agent] = index
        index.add(user.uid)

    def addUserToGroup(self,uid,gid):
        index = self.user_group_index.get(uid,None)
        if index is None:
            index = Set()
            self.user_group_index[uid] = index
        index.add(gid)

        index = self.group_user_index.get(gid,None)
        if index is None:
            index = Set()
            self.group_user_index[gid] = index
        index.add(uid)

