#!/usr/bin/env python
import json
import os
import time
from flask import Flask, redirect, url_for, request
from subprocess import call


app = Flask(__name__)
task_id = 0

@app.route("/error/<reason>")
def error(reason):
    return "Please contact lunghaolee@gmail.com for registering the usage of this service! ({})".format(reason)

@app.route("/")
def hello():
    return redirect(url_for('error',reason = 'Hello'))

@app.route("/done")
def done(msg):
    return msg

@app.route("/query", methods = ['POST', 'GET'])
def query():
    global task_id
    if request.method == 'POST':
        try:
            rj = request.json  # Request json
            if rj['uid'] == 'l2' and rj['key'] == 'ntnu':                
                print("Handle sent={}...".format(rj['sent'].encode('utf-8')))
                task_id = (task_id + 1) % 10000
                call(['spark-submit', 'demo.py', rj['sent'], str(task_id)])
                task_id_rst = "/tmp/{}.json".format(task_id)
                wc = 0
                while True:
                    if os.path.isfile(task_id_rst):
                        with open(task_id_rst, 'r') as fh:
                            return fh.read()
                    wc += 1
                    time.sleep(1)
                    if wc > 10:
                        return redirect(url_for('error',reason = 'Timeout')) 
            else:
                return redirect(url_for('error',reason = 'InvalidKey'))
            #return redirect(url_for('done', msg='Success!'))
            return json.dumps({"Num":0})
        except:
            raise
            return redirect(url_for('error',reason = 'UnknownData'))        
    else:
        return redirect(url_for('error',reason = 'UnsupportMethodInQuery'))

if __name__ == "__main__":
    app.run('0.0.0.0', 5050, True)
