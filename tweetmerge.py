import zipfile, zlib
import csv, json
import os.path
import sys
from operator import itemgetter

if len(sys.argv) == 1:
    print ("Tweetmerge merges twitter zip archives.")
    print ("Usage: tweetmerge dir")
    exit(1)
if len(sys.argv) >= 2:
    twtpath = sys.argv[1]
    if os.path.isdir(sys.argv[1]):
        twtpath = sys.argv[1]
    else:
        print ("Invalid path:", sys.argv[1])
        exit(1)

print (twtpath)

zips = [f for f in os.listdir(twtpath) if not os.path.isdir(os.path.join(twtpath,f))]
zips = [z for z in zips if zipfile.is_zipfile(os.path.join(twtpath,z))]
for z in zips:
    zfile = zipfile.ZipFile(os.path.join(twtpath,z))
    zname = z.replace('.zip','')
    print (z)
    for name in zfile.namelist():
        (dirname, filename) = os.path.split(name)
        dir_fullpath = os.path.join(twtpath,zname,dirname)
        if not os.path.exists(dir_fullpath):
           os.makedirs(dir_fullpath)
        zfile.extract(name,os.path.join(twtpath,zname))

dirs = [d for d in os.listdir(twtpath) if os.path.isdir(os.path.join(twtpath, d))]

print ("parsing tweets.csv..")
csvmerge = []
for d in dirs:
    csvpath = twtpath + "\\" + d + "\\tweets.csv"
    with open(csvpath, 'rt', encoding= 'utf-8') as csvfile:
        csvreader = csv.reader(csvfile,delimiter=',',quotechar='"')
        for row in csvreader:
            if len(row) > 0:
                csvmerge.append(row)

csvmerge = list({v[0]:v for v in csvmerge}.values())
csvmerge = sorted(csvmerge,key=itemgetter(0))
header = csvmerge[len(csvmerge)-1]
csvmerge = [header] + csvmerge[0:len(csvmerge)-1]

with open(csvpath, 'wt', encoding= 'utf-8') as csvfile:
    csvwriter = csv.writer(csvfile,delimiter=',',quotechar='"',quoting=csv.QUOTE_ALL)
    for row in csvmerge:
        csvwriter.writerow(row)

print (str(len(csvmerge)-1) + " tweets parsed.")
print ("parsing json..")

jsdic = {}
for d in dirs:
    jspath = twtpath + "\\" + d + "\\" + "data\\js\\tweets"
    for js in os.listdir(jspath):
        if js not in jsdic:
            jsdic[js] = [os.path.join(jspath,js)]
        else:
            jsdic[js] = jsdic[js] + [os.path.join(jspath,js)]

twtcount = 0
idx_merge = []
for js in list(jsdic.keys()):
    json_merge = []
    for fp in jsdic[js]:
        with open(fp,"r") as f:
            content = f.read()
        header = content.splitlines(1)[0]
        content = content[len(header):]
        json_merge = json_merge + json.loads(content)
    json_merge = list({v['id']:v for v in json_merge}.values())
    json_merge = sorted(json_merge,key=itemgetter('id'))

    twtsize = len(json_merge)
    twtcount += twtsize

    dumppath = os.path.join(jspath,js)
    dump = json.dumps(json_merge,indent=4)
    with open(dumppath,'w') as w:
        w.write(header)
        print(dump, file=w)

    idx = {}
    idx['month'] = int(js.split('_')[1].split('.')[0])
    idx['file_name'] = 'data/js/tweets/'+js
    idx['var_name'] = 'tweets_' + js.split('.')[0]
    idx['tweet_count'] = twtsize
    idx['year'] = int(js.split('_')[0])
    idx_merge.append(idx)
print (str(twtcount) + " tweets indexed.")

payloadpath = twtpath + "\\" + d + "\\" + "data\\js\\payload_details.js"
with open(payloadpath,'r') as f:
    content = f.read()
    header = content.splitlines(1)[0]
    content = "[{" + content[len(header):] + "]"
    payload = json.loads(content)
    payload[0]['tweets'] = twtcount
    dump = json.dumps(payload,indent=0)
    dump = header + dump[4:-2]

with open(payloadpath,'w') as w:
    w.write(dump)

idxpath = twtpath + "\\" + d + "\\" + "data\\js\\tweet_index.js"
with open(idxpath,'w') as f:
    idx_merge = sorted(idx_merge,key=itemgetter('year','month'),reverse=True)
    dump = "var tweet_index = " + json.dumps(idx_merge,indent=4)
    f.write(dump)

print ("writing: output.zip")
file_paths = []
with zipfile.ZipFile(os.path.join(twtpath,"output.zip"),mode='w') as z:
    for root, directories, files in os.walk(os.path.join(twtpath,d)):
        for fn in files:
            fp = os.path.join(root, fn)
            file_paths.append(fp)
    for fp in file_paths:
        bfp = fp.replace(os.path.join(twtpath,d),"")
        z.write(fp,bfp,compress_type=zipfile.ZIP_DEFLATED)

print ("merge ok.")
