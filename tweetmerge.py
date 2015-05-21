import json
import os.path
from operator import itemgetter

twtpath = "V:\\twt"
dirs = [d for d in os.listdir(twtpath) if os.path.isdir(os.path.join(twtpath, d))]

jsdic = {}

for d in dirs:
    jspath = twtpath + "\\" + d + "\\" + "data\\js\\tweets"
    for js in os.listdir(jspath):
        if js not in jsdic:
            jsdic[js] = [os.path.join(jspath,js)]
        else:
            jsdic[js] = jsdic[js] + [os.path.join(jspath,js)]

for js in jsdic.keys():
    json_merge = []
    for fp in jsdic[js]:
        with open(fp,"r") as f:
            content = f.read()
        header = content.splitlines(1)[0]
        content = content[len(header):]
        json_merge = json_merge + json.loads(content)
    print js, len(json_merge)
    json_merge = {v['id']:v for v in json_merge}.values()
    json_merge = sorted(json_merge,key=itemgetter('id'))

    dumppath = os.path.join(twtpath,js)
    dump = json.dumps(json_merge,indent=4)
    w = open(dumppath,"w")
    w.write(header)
    print >> w, dump
    w.close()
    #print dumppath
    
    #for json_item in json_merge:
    #    print json_item['id']