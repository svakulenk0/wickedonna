import time
from datetime import date, datetime#, timedelta
import re
import csv
import os

import sqlite3

from bs4 import BeautifulSoup


#parse id or url (reference to Wickedonna_html.db), content, username, userId

datadir = "Responsiveness/protests/wickedonna" #SPECIFY
source_db = "Wickedonna_html.db"
source_db = os.path.join (os.environ ['HOME'], datadir, source_db)
target_db = "Wickedonna_content.db"
target_db = os.path.join (os.environ ['HOME'], datadir, target_db)


def new_database (name):
	conn = sqlite3.connect (name)
	#print("connected %s"%name)
	cur = conn.cursor()
	cur.execute("""CREATE TABLE Tweets
    	   (ID integer primary key autoincrement,
    	   	url    			TEXT    NOT NULL,
       		userId			integer NOT NULL,
       		nickname		TEXT	NOT NULL,
            tweet			TEXT    NOT NULL)""")
	conn.close
	print("table created")

def get_urls_with_content (source_db):
	conn = sqlite3.connect (source_db)
	cur = conn.cursor()
	cur.execute("SELECT url, text FROM cases WHERE text LIKE '%weibo%'");
	urls_with_content = cur.fetchall()
	print("fetched")
	conn.close
	print("will return urls")
	return urls_with_content

def parse_content (urls_with_content):
	
	for i in range (12, 100):#len(urls_with_content)):
		entry = urls_with_content[i]
		#for entry in urls_with_content:
		#if ("weibo") in entry[1]:
		url = entry[0]
		text = entry[1]
		userId = int(url.split("/")[4])
		nickname = "none"
		tweet = "none"
	
		print ("UserID : ", userId);
			
		soup = BeautifulSoup(text, "lxml")
		content = soup.find('div', {'class': 'content'})

					
		list = []
		try: 	
			for element in content:
				list.append(element)
		except:
			pass
			
		wbFound = 0
		for i in range(0, len(list)):
			e = str(list[i])
		#for e in content:
			if ("WB_info" in e or "WB_text" in e):
				wbFound = 1
				print("WB FOUND SET TRUE!!!")
				if ("WB_name" in e):
					info = BeautifulSoup(e, "lxml")
					nickname = info.findAll(True, {'class':['WB_name', 'S_func1']})
					
					try:
						nickname = nickname[0].text.strip()
						#print("NAME: ", nickname)
					except:
						#print("NO NAME") 
						pass

					if ("WB_text" in str(list[i+2])):
						wb_text = str(list[i+2])
						wb_text = BeautifulSoup(wb_text, "lxml")
						tweet = wb_text.text.strip()
						#print("TWEET: ", tweet)
		
		#other site structure (mixed sources)
		if (wbFound == 0):
			for j in range(0, len(list)):
				e = str(list[j])				
				if "weibo" in e:
					weibo_nn_el = BeautifulSoup(e, "lxml")
					nickname = weibo_nn_el.find('a')
					nickname = nickname.text.strip()
					print("NICKNAME ", nickname)
					print(url, "\n\n")
					k = 0

					text = str(list[j+2])
					text = BeautifulSoup(text, "lxml")
					spans = text.findAll('span')
					tweet = ""
					for span in spans:
						#TODO: join		
						tweet = tweet + span.text.strip()
					print("TWEET OTHER ", tweet)
					
					images = text.findAll(True, 'img')
					#TODO: get images
			
		print("#############")		
		write_to_db (target_db, url, userId, nickname, tweet)


def write_to_db (target_db, url, userId, nickname, tweet):
	conn = sqlite3.connect (target_db)
	conn.execute("INSERT INTO Tweets (url, userId, nickname, tweet) VALUES (?, ?, ?, ?)", (url, userId, nickname, tweet))
	conn.commit()
	conn.close()


if __name__ == "__main__":
	try:
		new_database (target_db)
	except Exception:
		print("error: db creation failed")
		pass
	urls_with_content = get_urls_with_content(source_db)
	parse_content (urls_with_content)


	

	
	
