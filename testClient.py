#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import requests

uid='l2'
key='ntnu'
sent='以目前鼓本估算'

if len(sys.argv) > 1:
    sent = sys.argv[1]

resp = requests.post('http://localhost:5050/query', json={'uid':uid, 'key':key, 'sent':sent})

print("Resp status={}".format(resp.status_code))
if resp.status_code == 200:
    resp_json = json.loads(resp.text)
    r'''
    {
        "rst_list": [
            {
                "tokenized": "+\u4ee5 +\u76ee\u524d *\u9f13\u672c +\u4f30\u7b97", 
                "collsug": [
                    {
                        "sug": ["\u80a1\u672c"], 
                        "org": "\u9f13\u672c", 
                        "sp": 3, 
                        "ep": 5
                    }], 
                "score": 4.9, 
                "source_sent": "\u4ee5 \u76ee\u524d \u80a1\u672c \u4f30\u7b97"
            }], 
        "sentence": "\u4ee5\u76ee\u524d\u9f13\u672c\u4f30\u7b97"
    }
    '''
    rst = resp_json['rst_list'][0]
    print("\tTokenized result: {}".format(rst['tokenized'].encode('utf-8')))
    if len(rst['collsug']) > 0:
        print("\tSuggested Correction ({}):".format(len(rst['collsug'])))
        for s in rst['collsug']:
            print("\t\t{} ({},{}) -> {}".format(s['org'].encode('utf-8'), s['sp'], s['ep'], s['sug'][0].encode('utf-8')))

print("")
