import time
from datetime import date, datetime#, timedelta
import re
import csv
import os

import sqlite3

import scrapy
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from bs4 import BeautifulSoup



#########################################################################################
############						DATABASE STUFF						############
#########################################################################################

###################    			 NEW DATABASE	 		   ###########################

def new_database (name):
	conn = sqlite3.connect (name)
	#print("connected %s"%name)
	cur = conn.cursor()
	cur.execute("""CREATE TABLE CASES
    	   (ID integer primary key autoincrement,
    	   	url    			TEXT    NOT NULL,
     	  	date    		TEXT    NOT NULL,
     	  	city    		TEXT    NOT NULL,
     	  	county			TEXT	NOT NULL,
       		headline		TEXT    NOT NULL,
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
	
	#TODO: content	
	
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
#####							THE SCRAPY SCRAPER							##########
##########################################################################################	


class Wicked_Spider(scrapy.Spider):
	name = "wicked_spider"

	def start_requests (self):
		urls = inurls
		#for i in range (0, 5):
		for url in urls:
			yield scrapy.Request(url=url, callback=self.parse)

	def parse (self, response):
		url = response.url		
		#print("IN PARSE NOW")
		conn = sqlite3.connect (target_db)
		pageSource = response.body
		soup = BeautifulSoup(pageSource, "lxml")
		try:
			content = extract_content (soup, url)
		except Exception as e:
			print (e)
			
		try:
			conn.execute("INSERT INTO Cases (url, date, city, county, headline, text, keyword1, \
				keyword2, people) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", \
				(content[0], content [1], content [2], content [3], content [4], soup.prettify(),\
				content [6], content [7], content [8]));
			conn.commit()
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

	#print ("we still have", len (inurls), "elements to scrape")	
	return inurls

			
##########################################################################################
###########						SPEFCIFICATIONS							##########
##########################################################################################
	
workdir = "Responsiveness/protests/workdir" #CHANGED 
datadir = "Responsiveness/protests/wickedonna" #CHANGED
target_db = "Wickedonna_html.db"
target_db = os.path.join (os.environ ['HOME'], datadir, target_db)

wicked_big = "Wickedonna.db" #%str (date.today())
wicked_big = os.path.join (os.environ ['HOME'], datadir, wicked_big)

cities = "Cities.csv"
cities = os.path.join (os.environ ['HOME'], workdir, cities)
counties = "Counties.csv"
counties = os.path.join (os.environ ['HOME'], workdir, counties)


if __name__ == "__main__":
	
	try:
		new_database (target_db)
	except Exception:
		#print("error: db creation failed")
		pass

	citylist = set (get_place (cities))
	countylist = set (get_place (counties))

	inurls = get_inurls (wicked_big, target_db)

	process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

	process.crawl(Wicked_Spider)
	process.start() # the script will block here until the crawling is finished


