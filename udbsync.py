from udbtype import *
from udb import *
from udbgenerator import *
from redispersist import *

if __name__ == '__main__':
    udb = generateUDB()

    PersistRedis(udb)
