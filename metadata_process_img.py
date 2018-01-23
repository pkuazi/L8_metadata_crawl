'''
Created on Nov 16, 2015

@author: root
'''

from cStringIO import StringIO

from PIL import Image

import myhttplib
from pyHtClient import *

# htc = HtClient("127.0.0.1", 8065)
htc = HtClient("10.0.138.151", 8065)

dataset_browse = "metadata_%s_browse"
dataset_scale = "metadata_%s_scale"
dataset_fgdc = "metadata_%s_fgdc"

options = {"ns": "gscloud"}


def check_browse(dataid, dataset):
    # if not dataset in ["landsat", "eo1"]:
    #     raise Exception()
    # return not tileslite.get_data(dataset_browse % (dataset,), dataid.lower(), options) is None
    return htc.has(dataset_browse % dataset, dataid.lower())
    # return False


def read_browse(dataid, dataset):
    data, cdata, options, status = htc.get(dataset_browse % dataset, dataid.lower())
    return data


def write_browse(dataid, imgdata, dataset):
    if not htc.put(dataset_browse % dataset, dataid.lower(), imgdata, ctype="image/jpeg", overwrite="no", ttl=0):
        raise Exception()


def check_scale(dataid, dataset):
    if not dataset in ["landsat", "eo1"]:
        raise Exception()
    return htc.has(dataset_scale % (dataset), dataid.lower())


def write_scale(dataid, metadata, dataset):
    if not dataset in ["landsat", "eo1"]:
        raise Exception()
    if not htc.put(dataset_scale % (dataset), dataid.lower(), metadata, ctype="image/jpeg", overwrite="no", ttl=0):
        raise Exception()


def make_scale(data, size=(128, 128), crop_size=1024):
    imgdata = StringIO()
    imgdata.write(data)
    imgdata.seek(0, 0)

    try:
        im = Image.open(imgdata)
        if crop_size > 0:
            w, h = im.size
            ls = min(w, h, crop_size)
            l = (w - ls) / 2
            r = w - l
            t = (h - ls) / 2
            b = h - t
            im = im.crop((l, t, r, b))
        im.thumbnail(size, Image.ANTIALIAS)
        outbuf = StringIO()
        im.save(outbuf, "JPEG")
        return outbuf.getvalue()
    except Exception, e:
        print e
        return None


def make_browse(data):
    imgdata = StringIO()
    imgdata.write(data)
    imgdata.seek(0, 0)

    try:
        im = Image.open(imgdata)
        w, h = im.size
        print "\nImage size:", w, h
        outbuf = StringIO()
        im.save(outbuf, "JPEG")
        return outbuf.getvalue()
    except Exception, e:
        print e
        return None


def process_image(url, dataid, dataset):
    print "process image:", url, dataid

    if check_browse(dataid, dataset):
        print "browse exist:", dataid
    else:
        print "get browse:", url
        resp = myhttplib.urlread(url)
        if resp:
            resp = make_browse(resp)
            if resp:
                print "store browse:", dataid
                write_browse(dataid, resp, dataset)

    if check_scale(dataid, dataset):
        print "scale exist:", dataid
    else:
        resp = read_browse(dataid, dataset)
        if resp:
            resp = make_scale(resp)
            if resp:
                print "store scale", dataid
                write_scale(dataid, resp, dataset)
