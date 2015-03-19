#! /usr/bin/env python
# -*- coding: utf-8 -*-
#########################
#
# Purpose: Parse history files into elasticsearch.
#
#########################
import json
from subprocess import call

URL = 'http://localhost:9200/epl/history'

def split_history(h_string):
	pass

if __name__ == "__main__":
	import doctest
	doctest.testmod()
	with open('201407.hist') as f:
	# with open('test.hst') as f:
		for my_line in f:
			# Each line in the file is a document onto itself.
			doc = {}
			line_list = my_line.split('^')
			# print my_line
			# Time stamp is [0] element, command code is [1]
			
			# Keep the date and time
			ansiDate  = line_list[0][1:9]
			doc['date'] = ansiDate
			ansiTime = line_list[0][9:15]
			doc['time'] = ansiTime
			stationNum = line_list[0][15:19]
			doc['station_number'] = stationNum
			
			seq = line_list[1][0:3]
			# Make the index unique to avoid elasticsearch loading repeats as versions.
			# Time stamp is [0] element, command code is [1]
			index = str.rstrip( line_list[0] ) + seq
			doc['seq'] = seq
			ccode = line_list[1][3:5]
			doc['ccode'] = ccode
			station = line_list[1][5:]
			doc['station'] = station
			userAccess = line_list[2][0:2]
			doc['user_access'] = userAccess
			library = line_list[2][2:]
			doc['library'] = library
			# TODO parse out the timestamp in index
			dataCodes = {}
			for dindex in range(1,len(line_list)):
				# TODO parse out the data code from the data value to make a tuple for store.
				# id {"time_stamp":, station_id, seq, c_code, login, d_code:{d1:"value1", d2:"value2", ...}}
				dataCode = line_list[dindex][0:2]
				dataValue = line_list[dindex][2:]
				if dataCode > "" and str.rstrip( dataCode ) != "O":
					# dataCodes[dataCode] = unicode( dataValue, 'iso-8859-1' ) # TODO Test this conversion.
					dataCodes[dataCode] = dataValue.encode( 'utf-8' ) # TODO Test this conversion.
					# print "  c:'" + dataCode + "'v:" + dataValue,
			doc['dcode'] = dataCodes
			# print doc
			callList = ["curl", "-XPUT", URL + '/' + index, "-d", json.dumps(doc)]
			print callList
			# result = call(callList)
			print '.', # Show all the elements you are printing.

# curl -XGET 'http://localhost:9200/_count?pretty' -d'{"query":{"match_all":{}}}'
# curl 'http://localhost:9200/?pretty'
# curl -XPUT 'http://localhost:9200/megacorp/employee/1' -d'{"first_name":"John", "last_name":"Smith", "age":25, "about":"I love to go rock climbing.", "interests":["sports","music"]}'
# curl -XGET 'http://localhost:9200/megacorp/employee/_count?pretty'
# curl -XGET 'http://localhost:9200/megacorp/employee/1'
# curl -XGET 'http://localhost:9200/megacorp/employee/1'
# curl -XGET 'http://localhost:9200/megacorp/employee/1?pretty'
# curl -XGET 'http://localhost:9200/megacorp/employee/_search?q=smith&pretty'
# curl -XGET 'http://localhost:9200/megacorp/employee/_search?q=nisbet&pretty'
# curl -XGET 'http://localhost:9200/megacorp/employee/1'
# curl -XGET 'http://localhost:9200/megacorp/employee/1?pretty'
# curl -XGET 'http://localhost:9200/megacorp/employee/2?pretty'
# curl -XPUT 'http://localhost:9200/megacorp/employee/2' -d'{"first_name":"Jane", "last_name":"Smith", "age":32, "about":"I like to collect rock albums", "interests":["music"]}'
# curl -XPUT 'http://localhost:9200/megacorp/employee/2' -d'{"first_name":"Douglas", "last_name":"Fir", "age":45, "about":"I like to build cabinets.", "interests":["forestry"]}'
# curl -XPUT 'http://localhost:9200/megacorp/employee/2' -d'{"first_name":"Jane", "last_name":"Smith", "age":32, "about":"I like to collect rock albums", "interests":["music"]}'
# curl -XGET 'http://localhost:9200/megacorp/employee/2?pretty'
# curl -XPUT 'http://localhost:9200/megacorp/employee/3' -d'{"first_name":"Douglas", "last_name":"Fir", "age":45, "about":"I like to build cabinets.", "interests":["forestry"]}'
# curl -XGET 'http://localhost:9200/megacorp/employee/3?pretty'
# curl -XGET 'http://localhost:9200/_count?pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?size=5'
# curl -XGET 'http://localhost:9200/epl/history/_search?size=5;pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?size=5&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?ccode=hF&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=hF&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=31221090692521&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=31221&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=IY&S05&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=IY&seq=S05&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=IY+seq=S05&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2Bseq%3AS05&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2Bseq%3AS05+%2BUO%3A21221020341936&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2Bseq%3AS05+%2BUO%3A21221020341936'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2Bseq%3AS05+%2BUO%3A21221020341936&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2BNQ%3A31221108792701&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2BNQ%3A31221108792701'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2Bseq%3AS05&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2Bseq%3AS05+%2BUO%3A21221020341936&pretty'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2BNQ%3A31221108792701'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2BNQ%3A31221108792701'
# curl -XGET 'http://localhost:9200/epl/history/_search?q=%2BNQ%3A31221108792701&pretty'
# curl -XDELETE 'http://localhost:9200/epl/history'
# curl -XPUT 'http://localhost:9200/epl/history' -d '{"settings":{"analysis":{"filter":{"nfkc_normalizer":{"type":"icu_normalizer", "name":"nfkc"}}, "analyzer": {"my_normalizer":{"tokenizer":"icu_tokenizer", "filter": ["nfkc_normalizer"]}}}}}'
# curl -XPUT 'http://localhost:9200/epl/history' -d '{"settings":{"analysis":{"filter":{"nfkc_nr":{"type":"icu_normalizer", "name":"nfkc"}}, "analyzer": {"my_normalizer":{"tokenizer":"icu_tokenizer", "filter": ["nfkc_normalizer"]}}}}}'
# curl -XPUT 'http://localhost:9200/epl' -d '{"settings":{"analysis":{"filter":{"nfkc_nr":{"type":"icu_normalizer", "name":"nfkc"}}, "analyzer": {"my_normalizer":{"tokenizer":"icu_tokenizer", "filter": ["nfkc_normalizer"]}}}}}'
# curl -XDELETE 'http://localhost:9200/epl'
# curl -XPUT 'http://localhost:9200/epl' -d '{"settings":{"analysis":{"filter":{"nfkc_nr":{"type":"icu_normalizer", "name":"nfkc"}}, "analyzer": {"my_normalizer":{"tokenizer":"icu_tokenizer", "filter": ["nfkc_normalizer"]}}}}}'
# curl -XPUT 'http://localhost:9200/epl' -d '{"settings":{"analysis":{"filter":{"nfkc_normalizer":{"type":"icu_normalizer", "name":"nfkc"}}, "analyzer": {"my_normalizer":{"tokenizer":"icu_tokenizer", "filter": ["nfkc_normalizer"]}}}}}'