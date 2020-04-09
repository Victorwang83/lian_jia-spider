# -*-coding:utf-8-*-
# getlonlat.py
# from: mamq
# run: python3 getlonlat.py
import pymysql.cursors
from urllib import parse
from urllib.request import urlopen
import requests
import re

connection = pymysql.connect(host="localhost", user='root', password='wq65929102', db='lian_jia', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def parse_address(address):
    url = 'http://api.map.baidu.com/geocoding/v3/'
    output = 'json'
    ak = 'Sj9CsksGB3dyurIao0wDFCvPXwBbC4ZK'  # 浏览器端密钥
    address = parse.quote(address)  # 由于本文地址变量为中文，为防止乱码，先用quote进行编码
    uri = url + '?' + 'address=' + address + '&output=' + output + '&ak=' + ak + "&callback=showLocation"
    req = urlopen(uri)
    res = req.read().decode()
    pattern_lng = "lng\":(.*?),"
    pattern_lat = "lat\":(.*?)}"
    lng = re.findall(pattern_lng, res)
    lat = re.findall(pattern_lat, res)
    if lng or lat:
        data = (float(lng[0]), float(lat[0]))
    return data


def get_address():
    try:
        with connection.cursor() as cursor:
            sql_select = "select District,sub_district,`position` from suzhou_real"
            cursor.execute(sql_select)
            connection.commit()
            result = cursor.fetchall()
    except:
        connection.rollback()
    address_list = []
    for i in result:
        address = '苏州' + i['District'] + i['sub_district'] + i['position']
        address_list.append(address)
    return address_list


try:
    with connection.cursor() as cursor:
        address_list = set(get_address())
        sql_create="create table if not exists lat_lng ( id int primary key auto_increment, addr_parse varchar(255),lng double, lat double);"
        cursor.execute(sql_create)
        connection.commit()
        for addr in address_list:
            lng_lat = parse_address(addr)
            lng = lng_lat[0]
            lat = lng_lat[1]
            sql_insert = "insert into lat_lng(addr_parse,lng,lat) values(%s,%s,%s);"

            cursor.execute(sql_insert, [addr,lng,lat])

            connection.commit()
            print("正在插入经纬度")

finally:

    connection.close()
