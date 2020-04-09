import pymysql.cursors
from urllib import parse
from urllib.request import urlopen
import requests
import re
import json
connection = pymysql.connect(host="localhost", user='root', password='wq65929102', db='lian_jia', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
try:
    with connection.cursor() as cursor:
        sql_select="select l.lng,l.lat,ceil(avg(Unit_price)) as 'count' from suzhou_real s join lat_lng l on s.address=l.addr_parse group by s.address;"
        cursor.execute(sql_select)
        result=cursor.fetchall()
        print(result)


except:
    connection.rollback()

finally:
    connection.close()