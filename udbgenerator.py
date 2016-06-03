import pymssql
from udbtype import *
from udb import UDB


def generateUDB():
    server = '222.222.46.204:9033'
    user = 'test'
    password = 'echat_test'
    database = 'test'

    udb = UDB()

    with pymssql.connect(server,user,password,database) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute('SELECT Aorg_ID as aid, Aorg_Name as name, Aorg_Parent as parent FROM tb_AgentOrg')
            for row in cursor:
                udb.addAgent( CreateAgent(row) )

            cursor.execute('SELECT Corg_ID as cid,Corg_Name as name,Corg_Parent as parent, Corg_Agent as agent FROM tb_ComOrg')
            for row in cursor:
                udb.addCompany( CreateCompany(row) )

            cursor.execute('SELECT Cg_ID as gid,Cg_Name as name,Cg_ComID as company FROM tb_ChatGroup')
            for row in cursor:
                udb.addGroup( CreateGroup(row) )

            cursor.execute('SELECT User_ID as uid,User_Account as account,User_CompanyID as company,User_Name as name,User_Type as role,User_CreateTime as create,User_ServiceBeginTime as service_begin,User_ServiceEndTime as service_end as service_end FROM tb_User WHERE User_Enable <> 0')
            for row in cursor:
                udb.addUser( CreateUser(row) )

    return udb

