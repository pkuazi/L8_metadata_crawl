#
import os, sys
import time, datetime
import zlib, json

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    from urllib.parse import unquote
else:
    from urllib import unquote

from urllib3.connectionpool import HTTPConnectionPool

DEFAULT_TTL = 86400

def print_error(e):
    sys.stderr.write(str(e) + "\n")

def encode_key(s):
    if s is None: return ""
    return unquote(s)

class KeysIter(object):
    
    def __init__(self, c, namespace, node=None):
        self.c = c 
        self.namespace = namespace
        self.node = node 
        self.__iter__()    
 
    def __iter__(self):
        self.last_key = "" 
        self.cached_keys = None
        self.cached_len = 0
        self.last_index = 0
        return self
    
    def __next__(self): 
        if self.cached_keys is None:
            self.__get_keys()  
        
        if self.last_index >= self.cached_len :
            self.__get_keys()
        
        val = self.cached_keys[ self.last_index ]
        self.last_index = self.last_index + 1
        return val.split(":")
    
    def next(self):  # for py2
        return self.__next__()
    
    def __get_keys(self): 
        try:
            namespace = self.namespace 
            key = self.last_key 
            node = self.node 
            
            url = "/ht/stat/keys?ns=%s&key=%s&size=100&node=%s" % (encode_key(namespace), encode_key(key), encode_key(node));
            resp = self.c.urlopen("GET", url)
            
            self.cached_keys = json.loads(resp.data.decode("utf-8"), encoding='utf-8') 
            self.cached_len = len(self.cached_keys)
            if self.cached_len > 0:
                self.last_key = self.cached_keys[-1] 
            self.last_index = 0
            
        except Exception as e:
            print(e)
            self.cached_keys = None
            
        if self.cached_keys is None:    
            raise StopIteration

        if self.cached_len == 0:
            raise StopIteration

class HtClient(object):
    def __init__(self, hostname, port=8060):
        self.c = HTTPConnectionPool(hostname, port) 

    def erase_data(self, namespace, key):
        try:
            url = "/ht/del?ns=%s&key=%s" % (encode_key(namespace), encode_key(key),)
            resp = self.c.urlopen("DELETE", url)
            jdata = json.loads(resp.data.decode("utf-8"), encoding="utf-8")
            return jdata.get("code", None) == 1
        except Exception as e:
            print(e)
        return False
    
    def erase_nss(self, namespace,):
        try:
            url = "/ht/del?ns=%s" % (encode_key(namespace),)
            resp = self.c.urlopen("DELETE", url)
            jdata = json.loads(resp.data.decode("utf-8"), encoding="utf-8")
            return jdata.get("code", None) == 1
        except Exception as e:
            print(e)
        return False
    
    def put_data(self, namespace, key, content, ctype=None, ttl=DEFAULT_TTL, overwrite="yes", **kwargs):
        try:
            ctype = "" if ctype is None else ctype
            url = "/ht/put?ns=%s&key=%s&ctype=%s&ttl=%s&overwrite=%s" % (
                encode_key(namespace), encode_key(key), ctype, ttl, overwrite);

            cdata = kwargs 
            headers = {"cdata": json.dumps(cdata, ensure_ascii=False)}

            resp = self.c.urlopen("PUT", str(url), body=content, headers=headers)
            jdata = json.loads(resp.data.decode("utf-8"), encoding="utf-8")
            return jdata["code"] == 1        
        except Exception as e:
            print_error(e)            
        return False

    put = put_data

    def has_data(self, namespace, key):
        try:
            url = "/ht/get?ns=%s&key=%s&action=has" % (encode_key(namespace), encode_key(key))
            resp = self.c.urlopen("GET", url)
            jdata = json.loads(resp.data.decode("utf-8"), encoding="utf-8")
            return jdata.get("code", None) == 1
        except Exception as e:
            print(e)
        return False

    has = has_data

    def get_stat(self, node=None):
        try:
            url = "/ht/stat?node=%s" % (encode_key(node),)
            resp = self.c.urlopen("GET", url)
            jdata = json.loads(resp.data.decode("utf-8"), encoding='utf-8')
            return jdata
        except Exception as e:
            print(e)
        return None

    stat = get_stat

    def get_nss(self, node=None):
        try:
            url = "/ht/stat/nss?node=%s" % (encode_key(node),)
            resp = self.c.urlopen("GET", url)
            jdata = json.loads(resp.data.decode("utf-8"), encoding='utf-8')
            return jdata
        except Exception as e:
            print(e)
        return None

    nss = get_nss
    
    def get_nodes(self):
        try:
            url = "/ht/stat/nodes"
            resp = self.c.urlopen("GET", url)
            jdata = json.loads(resp.data.decode("utf-8"), encoding='utf-8')
            return jdata
        except Exception as e:
            print(e)
        return None 
    
    nodes = get_nodes

    def get_keys(self, namespace, node=None):
        return KeysIter(self.c, encode_key(namespace), node)
    
    keys = get_keys

    def get_data(self, namespace, key):
        try:
            url = "/ht/get?ns=%s&key=%s" % (encode_key(namespace), encode_key(key))
            resp = self.c.urlopen("GET", url)
            try:
                cdata = json.loads(resp.headers.get("cdata", "{}"), encoding="utf-8")
            except:
                cdata = {}
            options = { "cdata" : cdata, "ctype" : resp.headers.get("ctype", "") }
            return resp.data, cdata, options, resp.status
        except Exception as e:
            print(e)
        return None, {}, {}, 500

    get = get_data

if __name__ == "__main__":
    # htc = HtClient("127.0.0.1", 8065)
    htc = HtClient("10.0.138.151", 8065)

    imgdata = '''open("/root/Pictures/m.jpg", "rb") .read()'''
    
    print(htc.stat())
    print(htc.nss())

    # modis_products_cn_browse
    # modis_products_cn_scale
    browse_dir = "/mnt/gscloud/MODIS_PRODUCT_CN/browse"
    for root, dirs, files in os.walk(browse_dir):
        for name in files:
            file = os.path.join(root, name)
            if file.endswith('V2.png'):
                imgdata = open(file, "rb").read()
                name = name.lower()
                htc.put('modis_products_cn_browse', name[:-4], imgdata,ctype="image/jpeg", overwrite="no",ttl=0)
            sys.exit(0)

    scale_dir = "/mnt/gscloud/MODIS_PRODUCT_CN/scale"
    for root, dir, files in os.walk(scale_dir):
        for name in files:
            file = os.path.join(root, name)
            if file.endswith("V2.png"):
                imgdata=open(file, "rb").read()
                name = name.lower()
                htc.put('modis_products_cn_scale', name[:-4], imgdata, ctype="image/jpeg", overwrite="no", ttl=0)



    # for i in range(10):
    print(htc.put('test', 'lc80010712013141lgn01',  imgdata, ctype="image/jpeg", overwrite="no", ttl=0 ))
 
    print(htc.has("test", "d_1"))
    print(htc.get("test", "d_1"))
   
    keys = htc.keys("test")
    for akey in keys :
        print(akey)
 
    # print(htc.has("test", "d_1"))
    # print(htc.erase_data("test", "d_1"))
 
    # print(htc.has("test", "d_1"))
 
    # print(htc.erase_nss("test",))
     
    # print(htc.has("test", "d_1"))
# 
#     print(htc.has("test", "e_1"))
#     print(htc.get("test", "e_1"))
