'''
Created on Jun 8, 2013

@author: root
'''

from cStringIO import StringIO
import sys, os
import types
import urllib
import threading
import time
import urllib2
from cookielib import MozillaCookieJar, DefaultCookiePolicy

def dict_add(*args):
    c = {}
    for v in args:
        if type(v) == type({}):
            c.update(v) 
    return c

user_agent = {'User-agent': "Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36" }
cookie_file = "/dev/shm/urllib2_cookies.txt"

policy = DefaultCookiePolicy()

global _cookieJar

_cookieJar = MozillaCookieJar(cookie_file, policy)
if os.path.exists(cookie_file ):
    _cookieJar.load()

DEBUG_LEVEL = 2

_http = urllib2.HTTPHandler()
_http.set_http_debuglevel(DEBUG_LEVEL)

_https = urllib2.HTTPSHandler()
_https.set_http_debuglevel(DEBUG_LEVEL)

_cookies = urllib2.HTTPCookieProcessor( _cookieJar )

urllib2.install_opener(urllib2.build_opener(_http, _https, _cookies))

ajax_header = {"X-Requested-With": "XMLHttpRequest" }
json_header = {"Accept": "application/json, text/javascript, */*; q=0.01"}

ajax_json_header = dict_add(ajax_header, json_header)

def  toFixed(v):
    return str(int(v * 100.0) / 100.0)


class stats(threading.Thread):

    def __init__(self, clength):
        self.clength = clength
        self.fstop = False
        self.ncount = 0

        threading.Thread.__init__(self)
        self.setDaemon(True)

    def run(self):
        st_start = time.time()

        slength = "unknown"
        if self.clength > 0 : slength = str(self.clength)

        oldt = time.time()
        while not self.fstop:
            if time.time() - oldt >= 1:
                pct = "unknown";
                if self.clength > 0:
                    pct = toFixed(float(self.ncount) / self.clength * 100) + "%"

                us_time = time.time() - st_start
                speed = "NA";
                if (us_time > 0)  :
                    fspeed = self.ncount / us_time / 1024;  # kb/s
                    speed = toFixed(fspeed) + " KB/S";

                stime = toFixed(us_time) + " seconds"
                ltime = "NA"

                if self.clength > 0 and self.ncount > 0 and us_time > 0:
                    cltm = (self.clength - self.ncount) / (self.ncount / us_time)
                    ltime = toFixed(cltm) + " seconds"

                sys.stderr.write("\r" + str(self.ncount) + " of " + slength + " bytes, " + pct + ", " + speed + ", elapsed: " + stime + ", remaining: " + ltime + "        ")
                sys.stderr.flush()

                oldt = time.time()
            time.sleep(0.1)

    def update(self, ncount):
        self.ncount = ncount

    def stop(self):
        self.fstop = True

def response_read(resp, callback, clength=0):
    if resp is None: return None

    if resp.code != 200: 
        return None
    
    dls = None
    if DEBUG_LEVEL >= 2:
        dls = stats(clength)
        dls.start()            
     
    try:
        ncount = 0       
        while True:
            data = resp.fp.read(8192)

            if data is None   : break
            if len(data) == 0 : break

            ncount += len(data)
            if dls is not None:
                dls.update(ncount)

            callback(data)

        resp.fp.close()
        return ncount 
    except Exception, e: 
        print >> sys.stderr, str(e)
        return None
    finally:
        if dls is not None:
            dls.stop()

TIMEOUT = 90

def  urlopen(url, data=None, headers={}):
    try:
        if data is not None:
            if isinstance(data, types.DictType):
                data = urllib.urlencode(data)
                print(data)

        r= urllib2.urlparse.urlparse(url)
        headers = dict_add(headers, {
            "Host": r[1],
            "Origin": r[0]+"://" + r[1]
        }) 
        headers = dict_add(headers,ajax_json_header)
        req = urllib2.Request(url, data, dict_add(headers, user_agent))
        resp = urllib2.urlopen(req, data, timeout=TIMEOUT)

        if resp.code >= 400:
            print >> sys.stderr, "ERROR:", url , "CODE:", resp.code , ", MESSAGE:" , resp.msg

        #print >>sys.stderr, _cookieJar
        _cookieJar.save()

        return resp
    except Exception, e:
        print >> sys.stderr, str(e)
        return None

def urlread(url, data=None, headers={}):
    resp = urlopen(url, data, headers)
    
    if resp is None:
        return None
         
    _s = StringIO()
    if response_read(resp, callback=_s.write) is None :
        return None
    return _s.getvalue()

