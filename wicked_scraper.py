import time
from datetime import date, datetime#, timedelta
import re
import csv
import os

import sqlite3

import scrapy
#from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
#from scrapy.utils.log import configure_logging
from scrapy.utils.response import open_in_browser

from bs4 import BeautifulSoup

####################################################################
entryCount = 0
workdir = "Responsiveness/protests/workdir" #SPECIFY 
datadir = "Responsiveness/protests/wickedonna" #SPECIFY
target_db = "Wickedonna_html.db"
target_db = os.path.join (os.environ ['HOME'], datadir, target_db)
tweet_db = "Tweets.db"
tweet_db = os.path.join (os.environ ['HOME'], datadir, tweet_db)

wicked_big = "Wickedonna.db"
wicked_big = os.path.join (os.environ ['HOME'], datadir, wicked_big)

cities = "Cities.csv"
cities = os.path.join (os.environ ['HOME'], workdir, cities)
counties = "Counties.csv"
counties = os.path.join (os.environ ['HOME'], workdir, counties)


####################################################################

def new_big_database (name):
	conn = sqlite3.connect (name)
	#print("connected %s"%name)
	cur = conn.cursor()
	cur.execute("""CREATE TABLE Cases
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
	conn.close


def new_tweet_database (name):
	conn = sqlite3.connect (name)
	#print("connected %s"%name)
	cur = conn.cursor()
	cur.execute("""CREATE TABLE Tweets
    	   (ID integer primary key autoincrement,
    	   	url    			TEXT    NOT NULL,
       		userId			integer NOT NULL,
       		nickname		TEXT	NOT NULL,
            tweet			TEXT    NOT NULL,
            images			TEXT)""")
	conn.close
	
def get_inurls (wickedlinks, scraped):
	con1 = sqlite3.connect (wickedlinks)
	cur = con1.cursor()
	cur.execute ("select url from cases")
	allUrls = cur.fetchall()
	con1.close()
	con2 = sqlite3.connect (scraped)
	cur = con2.cursor()
	cur.execute ("select url from cases")
	scrapedUrls = cur.fetchall()
	con2.close()
	entryCount = len(scrapedUrls)
	inurls = []
	tupel = list(set(allUrls) - set(scrapedUrls))
	for url in tupel:
		#print(url)
		url = str(url).strip("('',)")
		url = url.strip("\n")
		#print(url)
		inurls.append(url)	
	print ("we still have", len (inurls), "elements to scrape")	
	return inurls


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

def extract_content (soup, url):
	title = headline = content = date = keyword1 = keyword2 = city_w = county_w = "none"
	title = soup.find ('title')
	headline = re.sub('<[^<]+?>', '', str (title))	
	
	#print ("headline", headline)
	#content = soup.find ('div', {'class': 'article-content entry-content'})
	
	content = soup.find ('div', {'class': 'article-content entry-content'})
	if content == None:
		content = soup.find ('div', {'class': 'content'})
	
	content = re.sub('<[^<]+?>', '', str (content))
	
	#print("CONTENT %s\n"%content)
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


class Wicked_Spider(scrapy.Spider):
	name = "wicked_spider"
	debug_mode = False
	entryCount = entryCount
	
	def start_requests (self):
		urls = inurls
		for url in urls:
			#url = str(url)
			yield scrapy.Request(url=url, callback=self.parse_details)

	def parse_details (self, response):
		url = response.url		
		pageSource = response.body
		soup = BeautifulSoup(pageSource, "lxml")
		
		try:
			content = extract_content (soup, url) 
		except Exception as e:
			print ("EXCEPTION extracting content: ", e)	

		
		try:
			conn = sqlite3.connect (target_db)
			conn.execute("INSERT INTO Cases (url, date, city, county, headline, text, keyword1, \
				keyword2, people) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", \
				(content[0], content [1], content [2], content [3], content [4], soup.prettify(),\
				content [6], content [7], content [8]))
			conn.commit()
			conn.close()
		except Exception as e:
			print ("EXCEPTION: ", e)
			if "Service" not in content [4] and "Hotspot" not in content[4] and content[5] is not None and "None" not in content [5]:
				conn.execute ("UPDATE Cases SET date = ?, city = ?, county = ?, headline = ?, text = ?, keyword1 = ?, keyword2 = ?, people = ? WHERE url == ?;",(content[1], content[2], content[3], content[4], content[5], content[6], content[7], content[8], url))
				conn.commit()
			else:
				#print ("NO UPDATE for ", content [0], " because of ", e)
				pass		
		
		if ("weibo" in str(pageSource)):
			self.parse_tweets(response, url)

	def parse_tweets (self, response, url):
		body = response.body
		url = url
		print("URL ", url)
		userId = 0
		if "post" not in url:
			
		else:
			userId = int(url.split("/")[4])
		nickname = " "
			tmpuid = url.split("/")[-1]
			userId = int(tmpuid.split("_")[0])
		tweet = " "
		images = " "

		content = response.xpath("//div[@class='content']")
		if len(content) > 1:
			for i in range (0, len(content)):
				if not content[i].xpath(".//script"):
					content = content[i]
					break
		if len(content) != 1:
			print("ERROR content len = ", len(content))
		
		if content.xpath("//div[@class='WB_info']"):
			#first nickname on page
			nickname = content.xpath("//div[@class='WB_info']/a[@class='WB_name S_func1']/text()").extract_first()
			if nickname is None:
				nickname = content.xpath("//div[@class='WB_info']/a/text()").extract_first()
			if nickname is None:
				nickname = content.xpath("//div[@class='WB_text']/a/text()").extract_first()		
			if nickname is None:
				nickname = content.xpath("//div[@class='WB_info']/.//div[@class='userName']/.//text()").extract_first()
			if nickname is None:
				print("NICKNAME NONE AGAIN - OPEN URL")
				open_in_browser(response)
				nickname = "unknown"
				
			#all tweets on page
			tweets = content.xpath("//div[@class='WB_text']")
			tweet = " "
			for i in range (0, len(tweets)):		
				parts = tweets[i].xpath("./text()").extract()
				for p in parts: 
					tweet = tweet + p
				if tweets[i].xpath("./span"):
					parts = tweets[i].xpath("./span/text()").extract()
					for p in parts: 
						tweet = tweet + p
				if tweets[i].xpath("./div[@class='msgCnt']"):
					parts = tweets[i].xpath("./div[@class='msgCnt']/text()").extract()
					for p in parts: 
						tweet = tweet + p
				if tweets[i].xpath("./img"):
					img_list = tweets[i].xpath("./img[@src]").extract()
					tmp_imgs = ",".join(img_list)
					images = images + tmp_imgs
				if tweets[i].xpath("./div[@class='msgCnt']/img"):
					img_list = tweets[i].xpath("./div[@class='msgCnt']/img[@src]").extract()
					tmp_imgs = ",".join(img_list)
					images = images + tmp_imgs
				
				#username before next tweet
				if tweets[i].xpath("./div[@class='WB_info']/a[@class='WB_name S_func1']"):
					if tweet != " ":
						#before parsing next tweet, write current tweet to db	
						self.tweet2db (url, userId, nickname, tweet, images)
						tweet = " "
						images = " "
						time.sleep(2)
						
					#next nickname	
					nickname = tweets[i].xpath("./div[@class='WB_info']/a[@class='WB_name S_func1']/text()").extract_first()
					if nickname is None:
						nickname = ("./div[@class='username']/.//text()").extract_first()
						if nickname is None:
							print("NICKNAME NONE (SECOND WB_INFO)- OPEN URL")
							open_in_browser(response)
							nickname = "unknown"
					tweet = " "
			#finally, write last tweet on page to db
			if tweet != " ":
				self.tweet2db (url, userId, nickname, tweet, images)
				tweet = " "
				images = " "
			
					
		
		else: 
			#p tags
			nickname = " " 
			tweet = " "
			ps = content.xpath(".//p") #[@class='S_txt1']")
			for p in ps:
				isWeibo = False
				href = p.xpath("./a[@href]").extract_first()
				if href is not None and "weibo" in href:
					isWeibo = True
					if nickname == " ":
						nickname = p.xpath("./a/.//text()").extract_first()
						if nickname is None:
							print("NICKAME NONE; SECOND OPTION")
							open_in_browser(response)
							nickname = "unknown"
					else:
						if tweet == " ":
							#open_in_browser(response)
							print("POSSIBLY WEIRD: ")
							tweet = tweet + p.xpath("./a/.//text()").extract_first()
							print(tweet)
					
						else:
							#does entire tweet consist of @tags? if yes, then add another one
							t_check = tweet.split(" ")
							for t in t_check:
								
								if len(t) > 0 and not t[0] == "@":
									print("NO @")
									self.tweet2db (url, userId, nickname, tweet, images)
									time.sleep(2)
									tweet = " "
									images = " "
									#next tweet
									nickname = p.xpath("./a/.//text()").extract_first()	
							#add tag to same tweet				
							if tweet != " ":
								tweet = tweet + p.xpath("./a/.//text()").extract_first()
								
				else:
					if isWeibo == True:
						parts = p.xpath(".//text()").extract()
						for part in parts:
							tweet = tweet + part
						if p.xpath(".//img"):
							img_list = p.xpath(".//img[@src]").extract()
							tmp_imgs = ",".join(img_list)
							images = images + tmp_imgs
		print("#############")		


	def tweet2db (self, url, userId, nickname, tweet, images):
		name = nickname
		if (name is None and tweet is None):
			print("ALL NONE - PASS")
			return
		if (name == " " and tweet == " "):
			print("ALL EMPTY - PASS")
			return
		if name is None and tweet == " ":
			if tweet == " ":
				print("ALL NONE OR EMPTY; PASS")
				return
		if name is not None:
			name = name.strip()
		tweet = tweet
		imgs = images
		self.entryCount += 1
		if imgs  == " ":
			imgs = None
		else:
			imgs = imgs.strip()
		conn = sqlite3.connect (tweet_db)
		conn.execute("INSERT INTO Tweets (url, userId, nickname, tweet, images) VALUES (?, ?, ?, ?, ?)", (url, userId, name, tweet, images))
		conn.commit()
		conn.close()
		print("WROTE TO DB ", self.entryCount, ": ", name, tweet, images)
		

if __name__ == "__main__":
	
	try:
		new_big_database (target_db)
	except Exception:
		pass
		
	try:
		new_tweet_database (tweet_db)
	except Exception:
		pass

	citylist = set (get_place (cities))
	countylist = set (get_place (counties))

	inurls = get_inurls (wicked_big, target_db)

	process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
	process.crawl(Wicked_Spider)	
	process.start() # the script will block here until the crawling is finished
	

