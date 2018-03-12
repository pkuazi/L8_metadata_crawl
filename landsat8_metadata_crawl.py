#! /usr/bin/env python
# encoding=utf8

import myhttplib
import re, time, os
import urllib
import urllib2
from bs4 import BeautifulSoup
from metadata_process_img import *

from db_api import metadata_insertDB_Landsat8, dataid_exists
from pyHtClient import *

LANDSAT_COLLECTION = {"3120": "mss",
                      "12266": "tm",
                      "12267": "etm",
                      "12864": "landsat_8",
                      "13400": "landsat_8"}


def _mk_area(paths, rows):
    _areas = []
    for p in range(paths[0], paths[1]):
        for r in range(rows[0], rows[1]):
            _areas.append([p, r])
    return _areas


# CHINA_AREA = _mk_area([113, 114], [37, 56])
CHINA_AREA = _mk_area([113, 151], [23, 56])
GLOBAL_AREA = _mk_area([1, 233], [1, 248])

AGT_AREA = [[230, 94], [230, 95], [230, 96], [230, 97], [230, 98], [4, 62], [4, 63], [4, 66], [4, 67], [4, 68], [4, 69],
            [4, 70], [4, 71], [4, 72], [11, 62], [11, 63], [11, 64], [228, 96], [228, 97], [228, 98], [2, 68], [2, 69],
            [2, 70], [2, 71], [2, 72], [2, 73], [2, 74], [2, 75], [2, 76], [2, 77], [9, 62], [9, 63], [9, 64], [9, 65],
            [9, 66], [9, 67], [226, 97], [226, 98], [226, 99], [233, 74], [233, 75], [233, 76], [233, 77], [233, 78],
            [233, 79], [233, 80], [233, 81], [233, 82], [233, 83], [233, 84], [233, 85], [233, 86], [233, 87],
            [233, 88], [233, 89], [233, 90], [233, 91], [233, 92], [233, 93], [233, 94], [233, 95], [7, 60], [7, 61],
            [7, 62], [7, 63], [7, 64], [7, 65], [7, 66], [7, 67], [7, 68], [7, 69], [7, 70], [231, 90], [231, 91],
            [231, 92], [231, 93], [231, 94], [231, 95], [231, 96], [231, 97], [5, 61], [5, 62], [5, 63], [5, 64],
            [5, 66], [5, 67], [5, 68], [5, 69], [5, 70], [5, 71], [229, 96], [229, 97], [229, 98], [3, 67], [3, 68],
            [3, 69], [3, 70], [3, 71], [3, 72], [10, 63], [10, 64], [10, 65], [26, 79], [227, 96], [227, 97], [227, 98],
            [227, 99], [1, 71], [1, 72], [1, 73], [1, 74], [1, 75], [1, 76], [1, 77], [1, 78], [1, 79], [1, 80],
            [1, 81], [1, 82], [1, 83], [1, 84], [1, 85], [1, 86], [1, 87], [1, 88], [1, 89], [1, 90], [1, 91], [1, 92],
            [1, 93], [8, 60], [8, 61], [8, 62], [8, 63], [8, 64], [8, 65], [8, 66], [8, 67], [8, 68], [225, 98],
            [225, 99], [232, 76], [232, 77], [232, 79], [232, 83], [232, 84], [232, 85], [232, 86], [232, 87],
            [232, 88], [232, 89], [232, 90], [232, 91], [232, 92], [232, 93], [232, 94], [232, 95], [232, 96],
            [232, 97], [6, 61], [6, 62], [6, 63], [6, 64], [6, 65], [6, 66], [6, 67], [6, 68], [6, 69], [6, 70],
            [6, 71]]


def post_search_filter(collectionid, pathStart, pathEnd, rowStart, rowEnd, dStart, dEnd):
    url = "https://earthexplorer.usgs.gov/"
    resp = myhttplib.urlopen(url)

    if resp.code != 200:
        print("error", url)

    print(resp.code)
    url = "https://earthexplorer.usgs.gov/tabs/save"
    # 12864

    tab1_param = {"data": '''{"tab": 1, "destination": 2, "coordinates": [], "format": "dms", "dStart": "%s", "dEnd": "%s",
         "searchType": "Std", "num": "25000", "includeUnknownCC": "1", "maxCC": 100, "minCC": 0,
         "months": ["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
         "pType": "polygon"}''' % (dStart, dEnd)}

    resp = myhttplib.urlopen(url, tab1_param, headers={})
    print(resp.code, resp.read())

    # tab2_param = {"data": '''{"tab": 2, "destination": 3, "cList": ["12864"], "selected": 12864}'''}
    tab2_param = {
        "data": '''{"tab": 2, "destination": 3, "cList": ["%s"], "selected": %s}''' % (collectionid, collectionid)}

    resp = myhttplib.urlopen(url, tab2_param, headers={})
    print(resp.code, resp.read())

    # tab3_param = {"data": '''{"tab": 3, "destination": 4, "criteria": {
    #     "13400": {"between_20514_1": "123", "between_20514_2": "123", "between_20516_1": "32", "between_20516_2": "32",
    #               "select_20522_5": [""], "select_20515_5": [""], "select_20510_4": [""], "select_20517_4": [""],
    #               "select_20518_4": [""], "select_20513_3": [""], "select_20519_3": [""]}}, "selected": "12864"}'''}
    tab3_param = {"data": '''{"tab": 3, "destination": 4, "criteria": {
                "%s": {"between_20514_1": "%s", "between_20514_2": "%s", "between_20516_1": "%s", "between_20516_2": "%s",
                          "select_20522_5": [""], "select_20515_5": [""], "select_20510_4": [""], "select_20517_4": [""],
                          "select_20518_4": [""], "select_20513_3": [""], "select_20519_3": [""]}}, "selected": "%s"}''' % (
        collectionid, pathStart, pathEnd, rowStart, rowEnd, collectionid)}

    resp = myhttplib.urlopen(url, tab3_param, headers={})
    print(resp.code, resp.read())


def image_ruku(namespace, key, content, ctype=None, overwrite="yes", **kwargs):
    htc = HtClient("10.0.138.151", 8065)
    dataid = key.lower()
    imgdata = content
    # imgdata = '''open("/root/Pictures/m.jpg", "rb") .read()'''
    htc.put(namespace, dataid, imgdata, ctype="image/jpeg", overwrite="no", ttl=0)


def usgspage_parse(resp_content):
    dataset = "landsat"
    soup = BeautifulSoup(resp_content, "lxml")
    datacontent = soup.find_all('td', 'resultRowBrowse')
    result_num = len(datacontent)
    for i in range(result_num):
        data = BeautifulSoup(str(datacontent[i]))
        '''<html><body><a class="metadata" id="/form/metadatalookup/?collection_id=13400&amp;entity_id=LC81230322013356LGN01">\n<img alt="Browse Image" class="LC81230322013356LGN01" src="https://earthexplorer.usgs.gov/browse/thumbnails/landsat_8/2013/123/032/LC08_L1TP_123032_20131222_20170427_01_T1.jpg" title="Show Browse and Metadata"/>\n</a></body></html>'''

        # parse dataid
        dataid = data.a['id'][-21:]

        # determine weather the dataid already exists
        if dataid_exists(dataid):
            print "parsed,", dataid
            continue

        # parse img
        img_url = data.img['src']
        img_url = img_url.replace('/thumbnails','')
        process_image(img_url, dataid, dataset)

        # parse metadata
        meta_url = 'https://earthexplorer.usgs.gov' + data.a['id']
        resp = myhttplib.urlread(meta_url)

        print "process metadata:", meta_url, dataid

        soup = BeautifulSoup(resp,"lxml")
        metadata_content = soup.find_all('tr')
        column_name = BeautifulSoup(str(metadata_content[0])).find_all('th')
        attribute_name = [column_name[0].text, column_name[1].text]
        print(attribute_name)

        metadata = {}
        for i in range(1, len(metadata_content)):
            att = metadata_content[i]
            key = att.find_all('td')[0].text.strip('\n').strip()
            value = att.find_all('td')[1].text
            metadata[key.lower()] = value
        print(metadata)
        # metadata_insertDB_Landsat8(metadata)
        db_metadata = {"entityid": "", "day_night": "", "lines": "", "samples": "", "station_id": "", "path": "",
                       "row": "", "date_acquired": "", "start_time": "", "stop_time": "", "image_quality": "",
                       "cloud_cover": "", "sun_elevation": "", "sun_azimuth": "", "file_size": "",
                       "scene_center_lon": "", "scene_center_lat": "", "corner_ul_lon": "", "corner_ul_lat": "",
                       "corner_ur_lon": "", "corner_ur_lat": "", "corner_lr_lon": "", "corner_lr_lat": "",
                       "corner_ll_lon": "", "corner_ll_lat": ""}
        db_metadata["entityid"] = metadata['landsat scene identifier']
        db_metadata["day_night"] = metadata['day/night indicator']
        db_metadata["lines"] = float(metadata["reflective lines"])
        '''Panchromatic Lines
        Reflective Lines
        Thermal Lines'''
        db_metadata["samples"] = float(metadata["reflective samples"])
        '''Panchromatic Samples
        Reflective Samples
        Thermal Samples'''
        db_metadata["station_id"] = metadata["station identifier"]
        db_metadata["path"] = int(metadata["wrs path"])
        db_metadata["row"] = int(metadata["wrs row"])
        db_metadata["date_acquired"] = metadata["acquisition date"]
        db_metadata["start_time"] = metadata["start time"]
        db_metadata["stop_time"] = metadata["stop time"]
        db_metadata["image_quality"] = metadata["image quality"]
        db_metadata["cloud_cover"] = float(metadata["scene cloud cover"])
        db_metadata["sun_elevation"] = float(metadata["sun elevation"])
        db_metadata["sun_azimuth"] = float(metadata["sun azimuth"])
        db_metadata["file_size"] = 0

        db_metadata["scene_center_lon"] = float(metadata["center longitude dec"])
        db_metadata["scene_center_lat"] = float(metadata["center latitude dec"])
        db_metadata["corner_ul_lon"] = float(metadata["nw corner long dec"])
        db_metadata["corner_ul_lat"] = float(metadata["nw corner lat dec"])

        db_metadata["corner_ur_lon"] = float(metadata["ne corner long dec"])
        db_metadata["corner_ur_lat"] = float(metadata["ne corner lat dec"])
        db_metadata["corner_lr_lon"] = float(metadata["se corner long dec"])
        db_metadata["corner_lr_lat"] = float(metadata["se corner lat dec"])

        db_metadata["corner_ll_lon"] = float(metadata["sw corner long dec"])
        db_metadata["corner_ll_lat"] = float(metadata["sw corner lat dec"])

        print(db_metadata)
        metadata_insertDB_Landsat8(db_metadata)

def area_landsat8_metadata_fetch(collectionid, dStart, dataArea=CHINA_AREA):
    try:
        for area in dataArea:
            pathStart = area[0]
            pathEnd = area[0]
            rowStart = area[1]
            rowEnd  =area[1]
            # dStart = ""#"03/01/2018" 2018年3月1日
            dEnd = ""

            try:
                print("metadata for path: %s and row: %s")%(pathStart, rowStart)
                post_search_filter(collectionid, pathStart, pathEnd, rowStart, rowEnd, dStart, dEnd)
                url = "https://earthexplorer.usgs.gov/result/index"
                tab4_param = {"collectionId": collectionid}

                # parse the first page of the results
                resp = myhttplib.urlopen(url, tab4_param, headers={})
                resp_content = resp.read()

                # if there are more than one page, parse the other pages of the results
                soup = BeautifulSoup(resp_content, "lxml")

                if soup.div.text == "No Results Found":
                    print("No results Found")
                    continue
                result_num = int(soup.th.text.split()[5])
                # result_num = int(soup.find_all('th','ui-state-icons')[0].text.split()[5])

                print('total records:', result_num)

                # if there are no results.


                usgspage_parse(resp_content)

                if result_num > 10:
                    page_num = result_num / 10 + 1
                    for i in range(1, page_num):
                        # page number, from page 2
                        page_param = {"collectionId": collectionid, "pageNum": i+1}
                        resp = myhttplib.urlopen(url, page_param, headers={})
                        resp_content = resp.read()
                        usgspage_parse(resp_content)

                        print('sleep 3 seconds')
                        time.sleep(3)
            except Exception, e:
                print e
                str = "Oops! Fail to crawl metadata of l8 with path: %s and row: %s.\n"%(pathStart,rowStart)
                print(str)
                log_file.write(str)
                continue

            # sys.exit(0)

    except Exception, e:
        print e
        return

if __name__ == '__main__':
    # fetch_metadata_run(dataCol="landsat_8", dataArea=CHINA_AREA, year_range=[])

    # explorurl = "https://earthexplorer.usgs.gov/tabs/save"
    # tab4_param = {"tab": 4, "destination": 1, "selected": "12864"}
    # tab1_param = {"tab": 1, "destination": 2, "coordinates": [], "format": "dms", "dStart": "", "dEnd": "",
    #               "searchType": "Std",
    #               "num": "100", "includeUnknownCC": "1", "maxCC": 100, "minCC": 0,
    #               "months": ["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"], "pType": "polygon"}
    # resp = myhttplib.urlopen(explorurl, tab1_param, headers={})
    # tab2_param = {"tab": 2, "destination": 3, "cList": ["12864"], "selected": 12864}
    # resp = myhttplib.urlopen(explorurl, tab2_param, headers={})
    #
    # geturl = "https://earthexplorer.usgs.gov/form/addcriteria?collection_id=12864"
    # data = {"collection_id":12864}
    # resp1 = myhttplib.urlread(geturl, headers=data)

    # tab3_param = {"tab": 3, "destination": 4, "criteria": {
    #     "12864": {"between_20514_1": "123", "between_20514_2": "123", "between_20516_1": "32", "between_20516_2": "32",
    #               "select_20522_5": [""], "select_20515_5": [""], "select_20510_4": [""], "select_20517_4": [""],
    #               "select_20518_4": [""], "select_20513_3": [""], "select_20519_3": [""]}}, "selected": "12864"}
    # # resp = myhttplib.urlopen(explorurl, data=tab3_param, headers={})
    #
    # #cookie
    # import requests
    # s = requests.session()
    #
    # explorurl2 = "https://earthexplorer.usgs.gov/result/index"
    # data = {"collectionId": 12864}
    # resp = myhttplib.urlopen(explorurl2, data=data, headers={})
    # sceneids = re.findall('data-entityId[\w\W]+?data-displayId', resp.read())
    # print(sceneids)
    log_file = open("/tmp/landsat8_log.txt", 'w')
    dataCol = "landsat_8"
    collectionid = "12864"
    dStart = "01/01/2018"  # "03/01/2018" 2018年3月1日

    area_landsat8_metadata_fetch(collectionid, dStart, dataArea=CHINA_AREA)
    log_file.close()
