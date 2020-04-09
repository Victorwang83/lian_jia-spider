import ast
import re
import time
from datetime import datetime
from urllib import parse

import requests
from scrapy import Selector
from selenium import webdriver

from threading import Thread
import pymysql.cursors

domain = "https://su.lianjia.com"



headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"}

#定义全局变量用作多线程
district_urls=["https://su.lianjia.com/ershoufang/gusu/","https://su.lianjia.com/ershoufang/gaoxin1/","https://su.lianjia.com/ershoufang/wujiang/"]
page_urls=[]
class Parseoriginalthread(Thread):
    def run(self):
        while 1:
            res_text_all = requests.get(url="https://su.lianjia.com/ershoufang/", headers=headers,
                                        cookies=cookie_dict).text
            sel_all = Selector(text=res_text_all)
            for n in sel_all.xpath("//div[@data-role='ershoufang']/div[1]//a"):
                url = n.xpath("./@href").extract()[0]
                District_url = parse.urljoin(domain, url)
                district_urls.append(District_url)


class ParseDistrictthread(Thread):
    def run(self):
        while 1:
            try:
                district_url=district_urls.pop()
            except:
                time.sleep(1)
                continue
            res_text_district = requests.get(district_url, headers=headers).text
            sel = Selector(text=res_text_district)
            for sub_district_a in sel.xpath("//div[@data-role='ershoufang']/div[2]//a"):
                sub_district_url = parse.urljoin(domain, sub_district_a.xpath("./@href").extract()[0])
                # sub_district_name = sub_district_a.xpath("./text()").extract()[0]

                # 解析区下面的各个县的链接地址，获得总共的页数，遍历每一页
                res_text_subdistrict = requests.get(sub_district_url, headers=headers, cookies=cookie_dict).text
                sel_3 = Selector(text=res_text_subdistrict)
                page_info = sel_3.xpath("//@page-data").extract()[0]
                page_num = re.match(".*?:(\d+).*", page_info).group(1)
                for i in range(1, int(page_num) + 1):
                    if i == 1:
                        sub_district_url_i = sub_district_url

                    else:
                        sub_district_url_i = sub_district_url + 'pg' + str(i) + '/'

                        page_urls.append(sub_district_url_i)


class ParseTpagethread(Thread):
    def run(self):
        while 1:
            try:
                sub_district_url_i=page_urls.pop()
            except:
                time.sleep(1)
                continue
            time.sleep(2)
            res_text_subdistrict = requests.get(sub_district_url_i, headers=headers, cookies=cookie_dict).text
            sel_i = Selector(text=res_text_subdistrict)
            all_divs = sel_i.xpath("//div[@class='info clear']")
            District_name = sel_i.xpath("//div[@data-role='ershoufang']/div[1]/a[@class='selected']/text()").extract()[0]
            if District_name in ["园区","吴中"]:
                continue

            sub_district_name = sel_i.xpath("//div[@data-role='ershoufang']/div[2]/a[@class='selected']/text()").extract()[0]
            connection = pymysql.connect(host="localhost", user='root', password='wq65929102', db='lian_jia',
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
            for div in all_divs:
                # 获取title
                title = div.xpath(".//div[@class='title']/a/text()").extract()[0]
                # 获取单个房子的url
                title_url = div.xpath(".//div[@class='title']//@href").extract()[0]
                res_text_title = requests.get(title_url, headers=headers, cookies=cookie_dict).text
                sel_4 = Selector(text=res_text_title)
                # Des_district=sel_4.xpath("//div[@class='baseattribute clear']//div[@class='content'][1]/text()").extract()[1]
                position = div.xpath("..//div[@class='flood']//a[1]/text()").extract()[0]
                try:
                    follow_info = div.xpath(".//div[@class='followInfo']/text()").extract()[0].split("/")[1]
                    publish_time = follow_info
                except:
                    print("Error happened")
                try:
                    address = div.xpath(".//div[@class='houseInfo']/text()").extract()[0]
                    house_info = address.split("|")
                    house_shape = house_info[0]
                    Square = house_info[1]
                    direction = house_info[2]
                    decoration = house_info[3]
                    building_info = house_info[4]
                    total_price = float(div.xpath(".//div[@class='totalPrice']/span/text()").extract()[0])
                    unit_Price_info = div.xpath(".//div[@class='unitPrice']/span/text()").extract()[0]
                    unit_Price = re.match("单价(\d+\D)", unit_Price_info).group(1)
                except:
                    print("Error happened")

                try:
                    with connection.cursor() as cursor:
                            sql_select="select id from suzhou_real where title=%s"
                            cursor.execute(sql_select,(title,))
                            result = cursor.fetchone()
                            if not result:
                                try:
                                    sql_insert = "insert into suzhou_real(district,sub_district,title,position,house_shape,Total_Price,building_info,decoration,direction,Square,Unit_price,Publish_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                    cursor.execute(sql_insert, (
                                        District_name, sub_district_name, title, position, house_shape, total_price,
                                        building_info,
                                        decoration,
                                        direction, Square, unit_Price, publish_time))
                                    # connection is not autocommit by default. so you must commit to save
                                    connection.commit()
                                    print("正在插入%s" % title)
                                except:
                                    connection.rollback()
                            # sql_update = "update suzhou_real set district=%s,sub_district=%s,position=%s,house_shape=%s,Total_Price=%s,building_info=%s,decoration=%s,direction=%s,Square=%s,Unit_price=%s, Publish_time=%s where `title`=%s"
                            # cursor.execute(sql_update, (
                            # District_name, sub_district_name, position, house_shape, total_price, building_info, decoration,
                            # direction, Square, unit_Price, publish_time, title))
                            # connection.commit()
                            # print("正在更新%s" % title)


                except:
                    print("error happened")
                    continue
        connection.close()
def parse_subdistrict_url(district_url):
    res_text_district = requests.get(district_url, headers=headers).text
    sel = Selector(text=res_text_district)
    for sub_district_a in sel.xpath("//div[@data-role='ershoufang']/div[2]//a"):
        sub_district_url = parse.urljoin(domain, sub_district_a.xpath("./@href").extract()[0])
        # sub_district_name = sub_district_a.xpath("./text()").extract()[0]

        # 解析区下面的各个县的链接地址，获得总共的页数，遍历每一页
        res_text_subdistrict = requests.get(sub_district_url, headers=headers, cookies=cookie_dict).text
        sel_3 = Selector(text=res_text_subdistrict)
        page_info = sel_3.xpath("//@page-data").extract()[0]
        page_num = re.match(".*?:(\d+).*", page_info).group(1)
        for i in range(1, int(page_num) + 1):
            if i == 1:
                sub_district_url_i = sub_district_url

            else:
                sub_district_url_i = sub_district_url + 'pg' + str(i) + '/'
                page_urls.append(sub_district_url_i)

# 解析每一页，并保存到mysql
# def parse_single_page(sub_district_url_i):
#     res_text_subdistrict = requests.get(sub_district_url_i, headers=headers, cookies=cookie_dict).text
#     time.sleep(3)
#     sel_i = Selector(text=res_text_subdistrict)
#     all_divs = sel_i.xpath("//div[@class='info clear']")
#     District_name=sel_i.xpath("//div[@data-role='ershoufang']/div[1]/a[@class='selected']/text()").extract()[0]
#     sub_district_name=sel_i.xpath("//div[@data-role='ershoufang']/div[2]/a[@class='selected']/text()").extract[0]
#     for div in all_divs:
#         # 获取title
#         title = div.xpath(".//div[@class='title']/a/text()").extract()[0]
#         # 获取单个房子的url
#         title_url = div.xpath(".//div[@class='title']//@href").extract()[0]
#         res_text_title = requests.get(title_url, headers=headers, cookies=cookie_dict).text
#         sel_4 = Selector(text=res_text_title)
#         # Des_district=sel_4.xpath("//div[@class='baseattribute clear']//div[@class='content'][1]/text()").extract()[1]
#         position = div.xpath("..//div[@class='flood']//a[1]/text()").extract()[0]
#         try:
#             follow_info = div.xpath(".//div[@class='followInfo']/text()").extract()[0].split("/")[1]
#             publish_time = follow_info
#         except:
#             print("Error happened")
#         try:
#             address = div.xpath(".//div[@class='houseInfo']/text()").extract()[0]
#             house_info = address.split("|")
#             house_shape = house_info[0]
#             Square = house_info[1]
#             direction = house_info[2]
#             decoration = house_info[3]
#             building_info = house_info[4]
#             total_price = float(div.xpath(".//div[@class='totalPrice']/span/text()").extract()[0])
#             unit_Price_info = div.xpath(".//div[@class='unitPrice']/span/text()").extract()[0]
#             unit_Price = re.match("单价(\d+\D)", unit_Price_info).group(1)
#         except:
#             print("Error happened")
#         try:
#             sql_update = "update suzhou_real set district=%s,sub_district=%s,position=%s,house_shape=%s,Total_Price=%s,building_info=%s,decoration=%s,direction=%s,Square=%s,Unit_price=%s, Publish_time=%s where `title`=%s"
#             cursor.execute(sql_update, (District_name, sub_district_name,position, house_shape, total_price, building_info, decoration,direction, Square, unit_Price, publish_time,title))
#             connection.commit()
#             print("正在更新%s" % title)
#         except:
#             try:
#                 sql_insert = "insert into suzhou_real(district,sub_district,title,position,house_shape,Total_Price,building_info,decoration,direction,Square,Unit_price,Publish_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#                 cursor.execute(sql_insert, (
#                 District_name, sub_district_name, title, position, house_shape, total_price, building_info, decoration,
#                 direction, Square, unit_Price, publish_time))
#                 # connection is not autocommit by default. so you must commit to save
#                 connection.commit()
#                 print("正在插入%s" % title)
#             except:
#                 connection.rollback()

#解析各个大区和下面县级的内容，调用解析每一页
def parse_lianjia():
    # def parse_list()
    pass


                # parse_subdistrict_url(District_name,District_url)

if __name__=="__main__":
    browser = webdriver.Chrome(executable_path="D:\软件\chrome driver\chromedriver.exe")
    browser.get(domain)
    time.sleep(5)
    cookies = browser.get_cookies()
    cookie_dict = {}
    for item in cookies:
        cookie_dict[item['name']] = item['value']

             # District_name = n.xpath("./text()").extract()[0]
    # District_get_thread=Parseoriginalthread()
    Pageurl_get_thread=ParseDistrictthread()
    Page_anayl_thread= ParseTpagethread()
    # District_get_thread.start()
    Pageurl_get_thread.start()
    Page_anayl_thread.start()
