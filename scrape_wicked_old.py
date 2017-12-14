cd #!/usr/bin/python
# -*- coding: utf8 -*-


from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from lxml import html #using lxml for parsing
from urllib.request import urlopen
import sqlite3
#from html.parser import HTMLParser
from bs4 import BeautifulSoup
import time
from datetime import date, datetime, timedelta as td
import datetime as dat
import re
import csv
import os
import multiprocessing as mp
import logging
from time import sleep
import jieba
from collections import Counter
from pycnnum import cn2num
import random
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

logging.basicConfig(format='%(asctime) s : %(levelname)s : %(message)s', level=logging.INFO)

#########################################################################################
############						DATABASE STUFF						############
#########################################################################################

###################    			 NEW DATABASE	 		   ###########################

def new_database (name):
	conn = sqlite3.connect (name)
	print("connected %s"%name)
	cur = conn.cursor()
	cur.execute("""CREATE TABLE CASES
    	   (ID integer primary key autoincrement,
    	   	url    			TEXT    NOT NULL,
     	  	date    		TEXT    NOT NULL,
     	  	city    		TEXT    NOT NULL,
     	  	county			TEXT	NOT NULL,
       		headline    	TEXT    NOT NULL,
       		text   			TEXT    NOT NULL,
       		keyword1 		TEXT	NOT NULL,
       		keyword2		Text	NOT NULL,
       		people			Text	NOT NULL,
       		UNIQUE (url));""")
	print("after execute")
	conn.close


###################    			 MERGE DATABASES 		   ###########################


def finddate (row):
	date = re.search (r'201\d{1}(\.|\s|年)+\d{1,2}(\.|\s|月)*\d{1,2}', row).group(0)
	date = date.replace ("年", ".");
	date = date.replace ("月", ".");
	if " " in date:
		date = re.sub (" ", "", date)
	if ".." in date:
		date = re.sub ("..", ".", date)
	try:
		return (datetime.strptime(date,"%Y.%m.%d").date())
	except Exception:
		return date
	
	
def get_place (infile):
	with open(infile, "rU") as file:
		entries = ""
		for row in csv.reader(file):
			row = row [0]
			row = re.sub (" ", "", row)
			if infile == cities and len (re.sub ("市|县|区", "", row)) >= 2:
				row = re.sub ("市|县|区", "", row)
				entries = entries + row + " "
			else:
				entries = entries + row + " "
		keywdlist = set (entries.split())
		for entry in keywdlist:
			if len (entry) < 2:
				print (entry)
		return keywdlist
				

def extract_content (soup,url):
	title = headline = content = date = keyword1 = keyword2 = city_w = county_w = "none"
	title = soup.find ('title')
	headline = re.sub('<[^<]+?>', '', str (title))
	#print ("headline", headline)
	#content = soup.find ('div', {'class': 'article-content entry-content'})
	content = soup.find ('div', {'class': 'article-content entry-content'})
	if content == None:
		content = soup.find ('div', {'class': 'content'})
	
	content = re.sub('<[^<]+?>', '', str (content))
	try:
		date = finddate (str (headline))
	except Exception as e:
		#print (e)
		date = "None"
	keyword1 = re.match(r"^.*\［(.*)\］.*$", str(headline))
	if keyword1:
		keyword1 = keyword1.group(1)
	keyword2 = re.match(r"^.*\（(.*)\）.*$", str(headline))
	if keyword2:
		keyword2 = keyword2.group(1)
	if not keyword1: 
		keyword1 = "none"
	if not keyword2:
		keyword2 = "none"
	for city in citylist:
		if city in headline or city in content:
			city_w = city
						
	for county in countylist:
		if county in headline or county in content:
			county_w = county
	people = extract_people (content)
	#print ((url, date, city_w, county_w, headline, content, keyword1, keyword2, people))
	return (url, date, city_w, county_w, headline, content, keyword1, keyword2, people)
	

def extract_people (content):

	dictionary = {}
	valuesnewold =(["上百","100"], ["上千", "1000"],["上万", "10000"])
	listo = ("上百","上千", "上万")
	
	for value in valuesnewold:
		dictionary [value[0]]=value [1]

	term = []
	text11 = None
	word = None
	try:
		text = re.sub ("\n", "", content)
		#text  = re.search(".{6}十|百|千|万[.{0,2}业主|.{0,2}司机|.{0,2}名|.{0,2}开发商|.{0,2}投资者|\
		#.{0,2}朋友|.{0,2}民众|.{0,2}父母|.{0,2}老|.{0,3}学生|.{0,3}家|.{0,3}工[友|人|民]|.{0,3}师|\
		#.{0,3}户|.{0,4}人|.{0,4}民|多家]", text)
		
		text1  = re.search("[1|2|3|4|5|6|7|8|9]\d{0,1}0{0,6}[民|名|户|人|业主|司机|开发商|投资者|朋友|民众|父母|老|学生|家|工友|工人|工民|师].{5}", text)
		text1 = re.sub ("\d{1,10}人民警察", "", text1.group(0))
		text1 = re.sub (".{0,2}岁|.{0,4}人口|.{0,4}元|.{0,4}人民币|.{0,4}金|.{0,4}工程|.{0,4}块|.{0,4}钱|.{0,4}工资", "", text1)
		text11 = re.search ("\d{2,8}", text1)
		text11 = text11.group(0)
	except Exception:
		pass		
		
	try:
		text2  = re.search("[一|二|两|三|四|五|六|七|八|九|上][十|百|千|万].{0,4}[民|名|户|人|业主|司机|开发商|投资者|朋友|民众|父母|老|学生|家|工友|工人|工民|师]", text)
		text2 = re.sub (".{0,2}岁|.{0,4}人口|.{0,4}元|.{0,4}人民币|.{0,4}金|.{0,4}工程|.{0,4}块|.{0,4}钱|.{0,4}工资", "", text2.group(0))
		
		word = cn2num(text2)
		for element in listo:
			if element in text2:
				word = dictionary [element]
				
		word = re.search ("\d{2,8}", word)
		word = word.group(0)
		
	except Exception:
		pass
		
		
	if word != None and int (word) <= 999999:
		term.append (int(word))
	if text11 != None and int (text11) <= 999999:
		term.append (int(text11))
	if term != []:
		term = max (term)
	else: term = "NA"

	return term
	
##########################################################################################
#####							THE SELENIUM SCRAPER							##########
##########################################################################################


def linkscraper (rangemin, rangemax, inurls):
	print (rangemin, rangemax)
	conn = sqlite3.connect (target_db)
	
	#webdriver.PhantomJS(service_args=['--load-images=no'])
	#driver =  webdriver.PhantomJS('phantomjs')
	print("BEFORE OPENING DRIVER")
	driver =  webdriver.Chrome('/usr/bin/google-chrome') #CHANGED added path to chromedriver
	
	print("RANGE ", rangemin, " ", rangemax)
	
	for i in range (rangemin, rangemax):
		url = inurls [i]
		driver.get(url)
		time.sleep(1000)
		soup = BeautifulSoup(driver.page_source, "lxml")
		print("BEFORE SOUP")
		print (soup.prettify())

		try:
			content = extract_content (soup, url)
		except Exception as e:
			print (e)

		#print (content [4])
		
		
		try:
			#if "Service" not in content [4] and "Hotspot" not in content [4] and content [5] != None:
			conn.execute("INSERT INTO Cases (url, date, city, county, headline, text, keyword1, \
				keyword2, people) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", \
				(content [0], content [1], content [2], content [3], content [4], soup.prettify(),\
				content [6], content [7], content [8]));
			conn.commit()
				#print ("inserted entry", content [0])
						#print (content)
		except Exception as e:
			print (e)
			if "Service" not in content [4] and "Hotspot" not in content [4] and content [5] != None and "None" not in content [5]:
				conn.execute ("UPDATE Cases SET date = ? where url == ?;",(content [1], url))
				conn.execute ("UPDATE Cases SET city = ? where url == ?;",(content [2], url))
				conn.execute ("UPDATE Cases SET county = ? where url == ?;",(content [3], url))
				conn.execute ("UPDATE Cases SET headline = ? where url == ?;",(content [4], url))
				conn.execute ("UPDATE Cases SET text = ? where url == ?;",(content [5], url))
				conn.execute ("UPDATE Cases SET keyword1 = ? where url == ?;",(content [6], url))
				conn.execute ("UPDATE Cases SET keyword2 = ? where url == ?;",(content [7], url))
				conn.execute ("UPDATE Cases SET people = ? where url == ?;",(content [8], url))
				#print (content [0], "UPDATED")
			else:
				print (content [0], "no update because of", e)
				pass
		conn.commit()
		print ("database committed", content [0])
							
	driver.quit ()



def collectMyResult(result):
    print("Got result {}".format(result))


def abortable_worker(func, *args, **kwargs):
	#print ("hjh")
	timeout = kwargs.get('timeout', None)
	#print (timeout)
	p = ThreadPool(1)
	#print ("p", p)
	res = p.apply_async(func, args=args)
	#print ("res", res)
	try:
		out = res.get(timeout)  # Wait timeout seconds for func to complete.
		return out
	except multiprocessing.TimeoutError:
		print("Aborting due to timeout")
		p.terminate()
		raise


def apply_async_with_callback (inurls):
	try:
		pool = mp.Pool ()	
		for i in range (0, len (inurls), interval):
			print (i+interval, len (inurls))
			if i+interval > len (inurls):
				
				print ("Now scrapings: ", i, len (inurls))
				abortable_func = partial (abortable_worker, linkscraper, timeout=10000)
				#print (abortable_func)
				pool.apply_async(abortable_func, args=(i, len (inurls), inurls))
			else:
				abortable_func = partial(abortable_worker, linkscraper, timeout=10000)
				pool.apply_async(abortable_func, args=(i, i+interval, inurls))#,callback=collectMyResult)
				print ("Now scraping: ", i, i+interval)
		pool.close()
		pool.join ()
	except Exception as e:
		print ("Exception", e)
	
	
def get_inurls (wickedlinks, wickedbig):
	matchlist = []
	conn = sqlite3.connect (wickedbig)
	cur = conn.cursor()
	cur.execute ("select url from cases")
	content = cur.fetchall()
	for cont in content:
		cont = re.sub ("\n", "", cont [0])
		matchlist.append(cont)
	inurls = []
	con = sqlite3.connect (wickedlinks)
	cur = con.cursor ()
	cur.execute ("Select url from cases")
	urls = [re.sub ("\n","", url[0]) for url in cur.fetchall()]
	#print (urls)

	inurls = list(set(urls) - set(matchlist))

	print ("we still have", len (inurls), "elements to scrape")
	
	return inurls

	
##########################################################################################
###########						SPEFCIFICATIONS								##########
##########################################################################################

####	---FILES---datadir = "dropbox/bigdata/Wicked"

workdir = "Responsiveness/protests/workdir" #CHANGED 
#datadir = "dropbox/bigdata/Wicked"
datadir = "Responsiveness/protests/wickedonna" #CHANGED

#target_db = "WickedonnaFULLNew_Unique.db" 33608 ´9223
target_db = "Wickedonna_html.db"
target_db = os.path.join (os.environ ['HOME'], datadir, target_db)

wicked_big = "Wickedonna.db" #%str (date.today())
wicked_big = os.path.join (os.environ ['HOME'], datadir, wicked_big)

cities = "Cities.csv"
cities = os.path.join (os.environ ['HOME'], workdir, cities)
counties = "Counties.csv"
counties = os.path.join (os.environ ['HOME'], workdir, counties)

interval = None

		
if __name__ == "__main__":
	
	try:
		new_database (target_db)
	except Exception:
		#print("error: db creation failed")
		pass

	citylist = set (get_place (cities))
	countylist = set (get_place (counties))

	inurls = get_inurls (wicked_big, target_db)
	interval = int(len (inurls)/12)

	apply_async_with_callback (inurls)
