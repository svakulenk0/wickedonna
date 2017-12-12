import random
import sqlite3
import string

def isValidName(s):
    if type(s) != str:
        return False
    for x in s:
        if not( x in string.ascii_letters or
                x in string.digits or
                x in ('_', ) ):
            return False
    return True

def genOutput(path, n, tableName):
    assert type(n) == int
    assert isValidName(tableName)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("SELECT * FROM %s ORDER BY RANDOM() LIMIT %s"%(tableName,n))
    sample = cur.fetchall()
    con.close()
    return sample

output = genOutput("./Weibo_2015_separated.db", 75000, "segmented2015")
#print(output)

