import sys
import time
from time import sleep
import locale
import multiprocessing
import json
import datetime
import logging
import os
import requests
import random
import re
import csv
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import math
from random import randint
from multiprocessing import Pool
from multiprocessing import cpu_count


user_agents = [
	"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
	"Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
	"Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0",
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0",
	"Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0",
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0",
	"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0",
	"Mozilla/5.0 (X11; OpenBSD amd64; rv:28.0) Gecko/20100101 Firefox/28.0",
	"Mozilla/5.0 (X11; Linux x86_64; rv:28.0) Gecko/20100101 Firefox/28.0",
	"Mozilla/5.0 (Windows NT 6.1; rv:27.3) Gecko/20130101 Firefox/27.3",
	"Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:27.0) Gecko/20121011 Firefox/27.0",
	"Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16",
	"Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14",
	"Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14",
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0) Opera 12.14",
	"Opera/12.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.02",
	"Opera/9.80 (Windows NT 6.1; U; es-ES) Presto/2.9.181 Version/12.00",
	"Opera/9.80 (Windows NT 5.1; U; zh-sg) Presto/2.9.181 Version/12.00",
	"Opera/12.0(Windows NT 5.2;U;en)Presto/22.9.168 Version/12.00",
	"Opera/12.0(Windows NT 5.1;U;en)Presto/22.9.168 Version/12.00",
	"Mozilla/5.0 (Windows NT 5.1) Gecko/20100101 Firefox/14.0 Opera/12.0",
	"Opera/9.80 (Windows NT 6.1; WOW64; U; pt) Presto/2.10.229 Version/11.62",
	"Opera/9.80 (Windows NT 6.0; U; pl) Presto/2.10.229 Version/11.62",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
	"Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10",
	"Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko ) Version/5.1 Mobile/9B176 Safari/7534.48.3",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; da-dk) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1",
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; tr-TR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; ko-KR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; fr-FR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; cs-CZ) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Windows; U; Windows NT 6.0; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; sv-se) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ko-kr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-fr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; es-es) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-us) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-gb) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; de-de) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27"
]
counter = []
def requests_retry_session(
	retries=5,
	backoff_factor=2,
	status_forcelist=(500, 502, 503, 504),
	session=None,
):
	session = session or requests.Session()
	retry = Retry(
		total=retries,
		read=retries,
		connect=retries,
		backoff_factor=backoff_factor,
		status_forcelist=status_forcelist,
	)
	adapter = HTTPAdapter(max_retries=retry)
	session.mount('http://', adapter)
	session.mount('https://', adapter)
	return session

def resolve_text(tag, tag_name, text):
	text = ''
	for tag in tags:
		if tag.name == tag_name and text_piece in tag.text:
			text = tag.next_sibling.get_text()
	return text

def get_pages():
	pages = []
	timeout = round(random.uniform(3.0, 5.0),1)
	url = 'https://www.gala-global.org/company-directory?f%5B0%5D=field_target_languages%3A119'
	headers = {}
	headers['User-Agent'] = user_agents[random.randint(0,len(user_agents)-1)]
	# headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
	r = requests_retry_session().get(url, headers=headers, timeout=timeout)
	print url
	htmltext = r.text.encode("utf-8")
	#print htmltext
	soup = BeautifulSoup(htmltext, 'html.parser')
	last_page = int(re.findall(re.compile('page=(\d+)'), soup.select("li.pager-last.last a")[0].get('href'))[0])
	for i in range(0, last_page + 1):
		page = 'https://www.gala-global.org/company-directory?f%5B0%5D=field_company_type%3A221&f%5B1%5D=field_target_languages%3A119&page={}'.format(i)
		pages.append(page)
	print pages
	return pages

def get_links():
	pages = get_pages()
	with open("gala_links.csv", "wb") as infile:
		writer = csv.writer(infile)
		for url in pages:
			timeout = round(random.uniform(3.0, 5.0),1)
			headers = {}
			headers['User-Agent'] = user_agents[random.randint(0,len(user_agents)-1)]
			# headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
			
			r = requests_retry_session().get(url, headers=headers, timeout=timeout)
			print url
			htmltext = r.text.encode("utf-8")
			#print htmltext
			soup = BeautifulSoup(htmltext, 'html.parser')
			links = soup.select("h2 a[href*='company-directory']")
			for link in links:
				href = 'https://www.gala-global.org' + link.get('href')
				writer.writerow([href])

def dedup():
	with open("gala_links.csv", "rb") as infile:
		with open("gala_links_dedup.csv", "wb") as outfile:

			#configure columns, which should be included in deduplication. We do not include lng and lat, as they are always different
			columns_to_use_list = [0]
			reader = csv.reader(infile)
			writer = csv.writer(outfile)
			row_ids = set([])  # This set serve as unique identifier of rows
			for row in reader:
				row_id = []  # Initiating the id for this row
				for j in columns_to_use_list:
					row_id.append(row[j])
				row_id = tuple(row_id)
				if row_id not in row_ids:  # De-duplicating
					row_ids.add(row_id)
					writer.writerow(row)

def parse_data(url, line_num):
	
	timeout = round(random.uniform(3.0, 5.0),1)
	headers = {}
	headers['User-Agent'] = user_agents[random.randint(0,len(user_agents)-1)]
	r = requests_retry_session().get(url, headers=headers, timeout=timeout)
	print line_num, url
	htmltext = r.text.encode("utf-8")
	soup = BeautifulSoup(htmltext, 'html.parser')
																								
	try:
		company = soup.select("h1.page-title")[0].get_text().encode("utf-8")
	except:
		company = ''
	try:
		profile_link = url
	except:
		profile_link = ''
	try:
		country = soup.select(".company-main-location")[0].get_text().split("<br>")[3].encode("utf-8")
	except:
		country = ''
	try:
		headquaters = ",".join(soup.select(".company-main-location")[0].get_text().split("<br>")).encode("utf-8")
	except:
		headquaters = '' 
	try:
		other_loc = ",".join(soup.select(".company-other-locations li")[0].get_text().split("<br>")).encode("utf-8")
	except:
		other_loc = ''
	try:
		email = soup.select('span.email a')[0].get("href").replace("mailto:", "")
	except:
		email = ''
	try:
		tel = soup.select('span.phone')[0].get_text().replace("Telephone:", "").strip()
	except:
		tel = ''
	try:
		web = soup.select('span.website a')[0].get("href")
	except:
		web = ''
	try:
		overview = resolve_text(soup.select("h5"), "h5", 'Organization Overview').encode("utf-8")
	except:
		overview = ''
	try:
		services = resolve_text(soup.select('h4'), "h4", "Products and Services").encode("utf-8")
	except:
		services = ''
	try:
		other_products = resolve_text(soup.select('h4'), "h4", "Other Products").encode("utf-8")
	except:
		other_products = ''
	try:
		sector = resolve_text(soup.select('h4'), "h4", "Sector").encode("utf-8")
	except:
		sector = ''
	try:
		s_lang =resolve_text(soup.select('h4'), "h4", "Source Languages").encode("utf-8")
	except:
		s_lang = ''
	try:
		t_lang = resolve_text(soup.select('h4'), "h4", "Target Languages").encode("utf-8")
	except:
		t_lang = ''
	try:
		o_type = resolve_text(soup.select('h4'), "h4", "Organization Type").encode("utf-8")
	except:
		o_type = ''
	try:
		media = soup.select('#block-views-company-feeds-block-3')[0].get_text().encode("utf-8")
	except:
		media = ''
	try:
		users = soup.select('#block-gala-user-gala-user-company-block')[0].get_text().encode("utf-8")
	except:
		users = ''
	write_data(company, profile_link, url, country, headquaters, other_loc, email, tel, web, overview, services, other_products, sector, s_lang, t_lang, o_type, media, users)

def contact_text(tag):
	if tag.name == 'a' and 'ontact' in tag.text:
		print tag.text, tag.get("href")
		result = tag.get("href")
		return result
	else:
		return False

def write_data(company, profile_link, url, country, headquaters, other_loc, email, tel, web, overview, services, other_products, sector, s_lang, t_lang, o_type, media, users):
	with open('gala_details.csv', 'ab') as resfile:
		writer = csv.writer(resfile)
		writer.writerow([company, profile_link, url, country, headquaters, other_loc, email, tel, web, overview, services, other_products, sector, s_lang, t_lang, o_type, media, users])

def save_faield_links(link, index):
	with open('6_failed_links.csv', 'ab') as resfile:
		writer = csv.writer(resfile)
		writer.writerow([index, link])
	resfile.close()

def main():
	#get_links()
	#dedup()

	with open("gala_links_dedup.csv", "rb") as f:
		reader = csv.reader(f)
		for line_num, line in enumerate(reader):
			if line_num < 0:
				continue
			else:
				url = 'https://www.gala-global.org/company-directory/2m-language-services'#line[0]
				parse_data(url, line_num)
	

if __name__ == "__main__":
	main()