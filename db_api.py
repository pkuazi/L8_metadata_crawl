'''
Created on Nov 16, 2015

@author: root
'''

import datetime, re

from pyMapCloud import pgsql

pg_src = pgsql.Pgsql("10.0.138.20", "postgres", "", "gscloud_metadata")
# pg_src = pgsql.Pgsql("127.0.0.1", "postgres", "", "gscloud_metadata")

def dataid_exists(dataId):
    sql = "select * from metadata_landsat where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)
    return len(datas)>0


def trim_empty(v, d):
    if v:
        return v
    return d


def try_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception, e:
        return None


def _parse_jdate(datadate):
    year, days, hour, minute, second = map(lambda a: int(float(a)), datadate.split(":"))
    tdate = datetime.datetime(year, 1, 1, hour, minute, second)
    return tdate + datetime.timedelta(days=days)

    # s = datadate + "000000"
    # v = s.split(".")
    # v[-1] = v[-1][:6]
    # datadate = ".".join(v)
    # return datetime.datetime.strptime(datadate, "%Y:%j:%X.%f")

def _parse_l8time(datadate):
    #datadate = u'2017:009:01:48:03.4275340'
    year = int(datadate[0:4])
    day_of_year = int(datadate[5:8])
    d = datetime.date(year,1,1)+datetime.timedelta(day_of_year-1)
    times = str(d.year)+'-'+str(d.month)+'-'+str(d.day) +" " + datadate[9:]
    return d

def _parsedate(datadate):
    fmts = ("%Y%m%d", "%Y/%m/%d",)
    for fmt in fmts:
        r = try_call(lambda: datetime.datetime.strptime(datadate, fmt))
        if r is not None:
            return r
    return None


def _parse_degree(dval):
    ll = re.findall("(\d+)&deg;(\d+)'(\d+)&quot;(\w)", dval)
    if len(ll) != 2:
        return None

    def comps(l):
        if len(l) != 4:
            return None, None

        a, b, c, d = l
        val = int(a) + int(b) / 60.0 + int(c) / 3600.0
        if d.lower() in ["n", "e"]:
            return val, d.lower()
        return 0 - val, d.lower()

    v0, a0 = comps(ll[0])
    v1, a1 = comps(ll[1])

    if v0 is None or v1 is None:
        return None
    return v0, a0, v1, a1


# http://glovis.usgs.gov/ImgViewer/showEO1Metadata.cgi?scene_id=EO1A1210302002320110PZ_LGS_01#
def metadata_insertDB_ALI(metadata):
    print metadata

    dataId = metadata["entity id"]
    datadate = _parsedate(metadata["acquisition date"])
    cloudCover = metadata['image cloud cover']

    path = metadata["orbit path"]
    row = metadata["orbit row"]

    stationId = metadata['receiving station']

    startTime = metadata["scene start time"]
    # if not startTime.endswith("0"):
    # 	startTime = startTime + "0"

    stopTime = metadata["scene stop time"]
    # if not stopTime.endswith("0"):
    # 	stopTime = stopTime + "0"

    startTime_f = _parse_jdate(startTime)  # datetime.datetime.strptime(startTime, "%Y:%j:%X.%f")
    stopTime_f = _parse_jdate(stopTime)  # datetime.datetime.strptime(stopTime,  "%Y:%j:%X.%f")

    sunAzimuth = metadata["sun azimuth"]
    sunElevation = metadata["sun elevation"]

    sensor = "ali"

    # _parse_degree TODO

    # 'nw corner'
    # 'se corner'
    # 'ne corner'
    # 'sw corner'


    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    '''
  id serial NOT NULL,
        
  
  lt_lat double precision,
  lt_long double precision,
  rt_lat double precision,
  rt_long double precision,
  lb_lat double precision,
  lb_long double precision,
  rb_lat double precision,
  rb_long double precision,
  
  ct_lat double precision,
  ct_long double precision,
  
  datadate_day bigint DEFAULT 0,
  datadate_year bigint DEFAULT 0,
  datadate_month bigint DEFAULT 0,
  layerexists bigint DEFAULT 0,
  dataexists bigint DEFAULT 0,
  browsenum bigint DEFAULT 0,
  the_geom geometry,
  iconexists integer DEFAULT 0,
  filesize bigint DEFAULT 0,	
    '''
    dayNight = metadata["day_night"]

    satellite = "LANDSAT5"
    dataType = "L45TM"

    lines = metadata["lines"]
    samples = metadata["samples"]

    imageQualityVCID1 = metadata["acquisition_quality"]
    imageQualityVCID2 = metadata["acquisition_quality"]

    cloudCoverUpperLeft = trim_empty(metadata["cloud_cover_quad_ul"], None)
    cloudCoverUpperRight = trim_empty(metadata["cloud_cover_quad_ur"], None)
    cloudCoverLowerLeft = trim_empty(metadata["cloud_cover_quad_ll"], None)
    cloudCoverLowerRight = trim_empty(metadata["cloud_cover_quad_lr"], None)

    filesize = metadata.get("file_size", None)
    if filesize is None:
        filesize = metadata.get("l1_file_size", "0")

    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    wkt = "POLYGON(( %s %s, %s %s, %s %s, %s %s, %s %s ))" % (
    lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, lb_lat, lt_long, lt_lat)

    insert_sql = """insert into metadata_landsat (dataID, satellite, dataType, sensor, lines, samples,
					stationId, dayNight, path, row, dataDate, dataDate_Year, 
					dataDate_Month, dataDate_Day, startTime, stopTime, imageQualityVCID1, imageQualityVCID2,
					cloudCover, cloudCoverUpperLeft, cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight, sunElevation, 
					sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, 
					rt_lat, rb_long, rb_lat, lb_long, lb_lat, filesize, the_geom ) values 
					(%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s,
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));"""

    update_sql = """update metadata_landsat set satellite=%s, dataType=%s, sensor=%s, lines=%s, samples=%s,
					stationId=%s, dayNight=%s, path=%s, row=%s, dataDate=%s, dataDate_Year=%s, 
					dataDate_Month=%s, dataDate_Day=%s, startTime=%s, stopTime=%s, imageQualityVCID1=%s, imageQualityVCID2=%s,
					cloudCover=%s, cloudCoverUpperLeft=%s, cloudCoverUpperRight=%s, cloudCoverLowerLeft=%s, cloudCoverLowerRight=%s, sunElevation=%s, \
					sunAzimuth=%s, ct_long=%s, ct_lat=%s, lt_long=%s, lt_lat=%s, rt_long=%s, 
					rt_lat=%s, rb_long=%s, rb_lat=%s, lb_long=%s, lb_lat=%s, filesize=%s, the_geom=ST_GeomFromText(%s, 4326) where dataID=%s """

    sql = "select * from metadata_eo1 where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (dataId, satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt))
        print "insert ", dataId

    else:
        pg_src.update(update_sql, (satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt, dataId))

        print "update ", dataId


def metadata_insertDB_LandsatETM7Off(metadata, ison):
    dataId = metadata["entityid"]
    dayNight = metadata["day_night"]

    satellite = "LANDSAT7"

    dataType = "L7slc-off"
    if ison == True:
        dataType = "L7slc-on"

    sensor = "ETM+"
    lines = metadata["lines"]
    samples = metadata["samples"]
    stationId = metadata["station_id"]
    path = metadata["path"]
    row = metadata["row"]

    datadate = _parsedate(metadata["date_acquired"])

    filesize = metadata.get("file_size", None)
    if filesize is None:
        filesize = metadata.get("l1_file_size", "0")

    startTime = metadata["start_time"]
    # if not startTime.endswith("0"):
    # 	startTime = startTime + "0"

    stopTime = metadata["stop_time"]
    # if not stopTime.endswith("0"):
    # 	stopTime = stopTime + "0"

    startTime_f = _parse_jdate(startTime)  # datetime.datetime.strptime(startTime, "%Y:%j:%X.%f")
    stopTime_f = _parse_jdate(stopTime)  # datetime.datetime.strptime(stopTime,  "%Y:%j:%X.%f")

    imageQualityVCID1 = metadata["image_quality_vcid_1"]
    imageQualityVCID2 = metadata["image_quality_vcid_2"]

    cloudCover = metadata["cloud_cover"].replace("N/A", "0")

    cloudCoverUpperLeft = trim_empty(metadata["cloud_cover_quad_ul"], None)
    cloudCoverUpperRight = trim_empty(metadata["cloud_cover_quad_ur"], None)
    cloudCoverLowerLeft = trim_empty(metadata["cloud_cover_quad_ll"], None)
    cloudCoverLowerRight = trim_empty(metadata["cloud_cover_quad_lr"], None)

    sunElevation = metadata["sun_elevation"]
    sunAzimuth = metadata["sun_azimuth"]

    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    wkt = "POLYGON(( %s %s, %s %s, %s %s, %s %s, %s %s ))" % (
    lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, lb_lat, lt_long, lt_lat)

    insert_sql = """insert into metadata_landsat (dataID, satellite, dataType, sensor, lines, samples,
					stationId, dayNight, path, row, dataDate, dataDate_Year, 
					dataDate_Month, dataDate_Day, startTime, stopTime, imageQualityVCID1, imageQualityVCID2,
					cloudCover, cloudCoverUpperLeft, cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight, sunElevation, 
					sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, 
					rt_lat, rb_long, rb_lat, lb_long, lb_lat, filesize, the_geom ) values 
					(%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s,
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));"""

    update_sql = """update metadata_landsat set satellite=%s, dataType=%s, sensor=%s, lines=%s, samples=%s,
					stationId=%s, dayNight=%s, path=%s, row=%s, dataDate=%s, dataDate_Year=%s, 
					dataDate_Month=%s, dataDate_Day=%s, startTime=%s, stopTime=%s, imageQualityVCID1=%s, imageQualityVCID2=%s,
					cloudCover=%s, cloudCoverUpperLeft=%s, cloudCoverUpperRight=%s, cloudCoverLowerLeft=%s, cloudCoverLowerRight=%s, sunElevation=%s, \
					sunAzimuth=%s, ct_long=%s, ct_lat=%s, lt_long=%s, lt_lat=%s, rt_long=%s, 
					rt_lat=%s, rb_long=%s, rb_lat=%s, lb_long=%s, lb_lat=%s, filesize=%s, the_geom=ST_GeomFromText(%s, 4326) where dataID=%s """

    sql = "select * from metadata_landsat where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (dataId, satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt))
        print "insert ", dataId

    else:
        pg_src.update(update_sql, (satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt, dataId))
        print "update ", dataId


def metadata_insertDB_Landsat8(metadata):
    dataId = metadata["entityid"]
    dayNight = metadata["day_night"]

    satellite = "LANDSAT8"
    dataType = "OLI_TIRS"
    sensor = "OLI_TIRS"

    lines = metadata["lines"]
    samples = metadata["samples"]

    stationId = metadata["station_id"]
    path = metadata["path"]
    row = metadata["row"]

    datadate = _parsedate(metadata["date_acquired"])

    startTime = metadata["start_time"]
    # if not startTime.endswith("0"):
    # 	startTime = startTime + "0"

    stopTime = metadata["stop_time"]
    # if not stopTime.endswith("0"):
    # 	stopTime = stopTime + "0"

    startTime_f = _parse_jdate(startTime)  # datetime.datetime.strptime(startTime, "%Y:%j:%X.%f")
    stopTime_f = _parse_jdate(stopTime)  # datetime.datetime.strptime(stopTime,  "%Y:%j:%X.%f")

    imageQualityVCID1 = metadata["image_quality"]
    imageQualityVCID2 = metadata["image_quality"]

    cloudCover = metadata["cloud_cover"]
    # cloudCover = metadata["cloud_cover"].replace("N/A", "0")

    sunElevation = metadata["sun_elevation"]
    sunAzimuth = metadata["sun_azimuth"]

    filesize = metadata["file_size"]

    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    wkt = "POLYGON(( %s %s, %s %s, %s %s, %s %s, %s %s ))" % (
    lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, lb_lat, lt_long, lt_lat)

    insert_sql = """insert into metadata_landsat (dataID, satellite, dataType, sensor, lines, samples,
					stationId, dayNight, path, row, dataDate, dataDate_Year, 
					dataDate_Month, dataDate_Day, startTime, stopTime, imageQualityVCID1, imageQualityVCID2,
					cloudCover, cloudCoverUpperLeft, cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight, sunElevation, 
					sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, 
					rt_lat, rb_long, rb_lat, lb_long, lb_lat, filesize, the_geom ) values 
					(%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s,
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));"""

    update_sql = """update metadata_landsat set satellite=%s, dataType=%s, sensor=%s, lines=%s, samples=%s,
					stationId=%s, dayNight=%s, path=%s, row=%s, dataDate=%s, dataDate_Year=%s, 
					dataDate_Month=%s, dataDate_Day=%s, startTime=%s, stopTime=%s, imageQualityVCID1=%s, imageQualityVCID2=%s,
					cloudCover=%s, cloudCoverUpperLeft=%s, cloudCoverUpperRight=%s, cloudCoverLowerLeft=%s, cloudCoverLowerRight=%s, sunElevation=%s, \
					sunAzimuth=%s, ct_long=%s, ct_lat=%s, lt_long=%s, lt_lat=%s, rt_long=%s, 
					rt_lat=%s, rb_long=%s, rb_lat=%s, lb_long=%s, lb_lat=%s, filesize=%s, the_geom=ST_GeomFromText(%s, 4326) where dataID=%s """

    sql = "select * from metadata_landsat where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (dataId, satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, None, None, None, None,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt))
        print "insert ", dataId

    else:
        pg_src.update(update_sql, (satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, None, None, None, None,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt, dataId))
        print "update ", dataId


def metadata_insertDB_L123MSS(metadata):
    dataId = metadata["entityid"]
    dayNight = metadata["day_night"]

    satellite = "LANDSAT%s" % dataId[2]

    dataType = "L123MSS"
    sensor = "MSS"

    lines = metadata["lines"]
    samples = metadata["samples"]
    stationId = metadata["station_id"]

    path = metadata["path"]
    row = metadata["row"]

    datadate = _parsedate(metadata["date_acquired"])

    startTime = metadata["start_time"]
    # if not startTime.endswith("0"):
    # 	startTime = startTime + "0"

    stopTime = trim_empty(metadata["stop_time"], startTime)
    # if not stopTime.endswith("0"):
    # 	stopTime = stopTime + "0"

    startTime_f = _parse_jdate(startTime)  # datetime.datetime.strptime(startTime, "%Y:%j:%X.%f")
    stopTime_f = _parse_jdate(stopTime)  # datetime.datetime.strptime(stopTime,  "%Y:%j:%X.%f")

    imageQualityVCID1 = metadata["acquisition_quality"]
    imageQualityVCID2 = metadata["acquisition_quality"]
    cloudCover = metadata["cloud_cover"].replace("N/A", "0")

    cloudCoverUpperLeft = trim_empty(metadata["cloud_cover_quad_ul"], None)
    cloudCoverUpperRight = trim_empty(metadata["cloud_cover_quad_ur"], None)
    cloudCoverLowerLeft = trim_empty(metadata["cloud_cover_quad_ll"], None)
    cloudCoverLowerRight = trim_empty(metadata["cloud_cover_quad_lr"], None)

    sunElevation = metadata["sun_elevation"]
    sunAzimuth = metadata["sun_azimuth"]

    filesize = metadata.get("file_size", None)
    if filesize is None:
        filesize = metadata.get("l1_file_size", "0")

    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    wkt = "POLYGON(( %s %s, %s %s, %s %s, %s %s, %s %s ))" % (
    lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, lb_lat, lt_long, lt_lat)

    insert_sql = """insert into metadata_landsat (dataID, satellite, dataType, sensor, lines, samples,
					stationId, dayNight, path, row, dataDate, dataDate_Year, 
					dataDate_Month, dataDate_Day, startTime, stopTime, imageQualityVCID1, imageQualityVCID2,
					cloudCover, cloudCoverUpperLeft, cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight, sunElevation, 
					sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, 
					rt_lat, rb_long, rb_lat, lb_long, lb_lat, filesize, the_geom ) values 
					(%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s,
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));"""

    update_sql = """update metadata_landsat set satellite=%s, dataType=%s, sensor=%s, lines=%s, samples=%s,
					stationId=%s, dayNight=%s, path=%s, row=%s, dataDate=%s, dataDate_Year=%s, 
					dataDate_Month=%s, dataDate_Day=%s, startTime=%s, stopTime=%s, imageQualityVCID1=%s, imageQualityVCID2=%s,
					cloudCover=%s, cloudCoverUpperLeft=%s, cloudCoverUpperRight=%s, cloudCoverLowerLeft=%s, cloudCoverLowerRight=%s, sunElevation=%s, \
					sunAzimuth=%s, ct_long=%s, ct_lat=%s, lt_long=%s, lt_lat=%s, rt_long=%s, 
					rt_lat=%s, rb_long=%s, rb_lat=%s, lb_long=%s, lb_lat=%s, filesize=%s, the_geom=ST_GeomFromText(%s, 4326) where dataID=%s """

    sql = "select * from metadata_landsat where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (dataId, satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt))
        print "insert ", dataId

    else:
        pg_src.update(update_sql, (satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt, dataId))

        print "update ", dataId


def metadata_insertDB_L45MSS(metadata):
    dataId = metadata["entityid"]
    dayNight = metadata["day_night"]

    satellite = "LANDSAT%s" % dataId[2]

    dataType = "L45MSS"
    sensor = "MSS"

    lines = metadata["lines"]
    samples = metadata["samples"]
    stationId = metadata["station_id"]

    path = metadata["path"]
    row = metadata["row"]

    datadate = _parsedate(metadata["date_acquired"])

    startTime = metadata["start_time"]
    # if not startTime.endswith("0"):
    # 	startTime = startTime + "0"

    stopTime = trim_empty(metadata["stop_time"], startTime)
    # if not stopTime.endswith("0"):
    # 	stopTime = stopTime + "0"

    startTime_f = _parse_jdate(startTime)  # datetime.datetime.strptime(startTime, "%Y:%j:%X.%f")
    stopTime_f = _parse_jdate(stopTime)  # datetime.datetime.strptime(stopTime,  "%Y:%j:%X.%f")

    imageQualityVCID1 = metadata["acquisition_quality"]
    imageQualityVCID2 = metadata["acquisition_quality"]
    cloudCover = metadata["cloud_cover"].replace("N/A", "0")

    cloudCoverUpperLeft = trim_empty(metadata["cloud_cover_quad_ul"], None)
    cloudCoverUpperRight = trim_empty(metadata["cloud_cover_quad_ur"], None)
    cloudCoverLowerLeft = trim_empty(metadata["cloud_cover_quad_ll"], None)
    cloudCoverLowerRight = trim_empty(metadata["cloud_cover_quad_lr"], None)

    sunElevation = metadata["sun_elevation"]
    sunAzimuth = metadata["sun_azimuth"]

    filesize = metadata.get("file_size", None)
    if filesize is None:
        filesize = metadata.get("l1_file_size", "0")

    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    wkt = "POLYGON(( %s %s, %s %s, %s %s, %s %s, %s %s ))" % (
    lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, lb_lat, lt_long, lt_lat)

    insert_sql = """insert into metadata_landsat (dataID, satellite, dataType, sensor, lines, samples,
					stationId, dayNight, path, row, dataDate, dataDate_Year, 
					dataDate_Month, dataDate_Day, startTime, stopTime, imageQualityVCID1, imageQualityVCID2,
					cloudCover, cloudCoverUpperLeft, cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight, sunElevation, 
					sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, 
					rt_lat, rb_long, rb_lat, lb_long, lb_lat, filesize, the_geom ) values 
					(%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s,
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));"""

    update_sql = """update metadata_landsat set satellite=%s, dataType=%s, sensor=%s, lines=%s, samples=%s,
					stationId=%s, dayNight=%s, path=%s, row=%s, dataDate=%s, dataDate_Year=%s, 
					dataDate_Month=%s, dataDate_Day=%s, startTime=%s, stopTime=%s, imageQualityVCID1=%s, imageQualityVCID2=%s,
					cloudCover=%s, cloudCoverUpperLeft=%s, cloudCoverUpperRight=%s, cloudCoverLowerLeft=%s, cloudCoverLowerRight=%s, sunElevation=%s, \
					sunAzimuth=%s, ct_long=%s, ct_lat=%s, lt_long=%s, lt_lat=%s, rt_long=%s, 
					rt_lat=%s, rb_long=%s, rb_lat=%s, lb_long=%s, lb_lat=%s, filesize=%s, the_geom=ST_GeomFromText(%s, 4326) where dataID=%s """

    sql = "select * from metadata_landsat where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (dataId, satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt))
        print "insert ", dataId

    else:
        pg_src.update(update_sql, (satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt, dataId))

        print "update ", dataId


def metadata_insertDB_L45TM(metadata):
    dataId = metadata["entityid"]
    dayNight = metadata["day_night"]

    satellite = "LANDSAT5"
    dataType = "L45TM"
    sensor = "TM"

    lines = metadata["lines"]
    samples = metadata["samples"]
    stationId = metadata["station_id"]

    path = metadata["path"]
    row = metadata["row"]

    datadate = _parsedate(metadata["date_acquired"])

    startTime = metadata["start_time"]
    # if not startTime.endswith("0"):
    # 	startTime = startTime + "0"

    stopTime = metadata["stop_time"]
    # if not stopTime.endswith("0"):
    # 	stopTime = stopTime + "0"

    startTime_f = _parse_jdate(startTime)  # datetime.datetime.strptime(startTime, "%Y:%j:%X.%f")
    stopTime_f = _parse_jdate(stopTime)  # datetime.datetime.strptime(stopTime,  "%Y:%j:%X.%f")

    imageQualityVCID1 = metadata["acquisition_quality"]
    imageQualityVCID2 = metadata["acquisition_quality"]
    cloudCover = metadata["cloud_cover"].replace("N/A", "0")

    cloudCoverUpperLeft = trim_empty(metadata["cloud_cover_quad_ul"], None)
    cloudCoverUpperRight = trim_empty(metadata["cloud_cover_quad_ur"], None)
    cloudCoverLowerLeft = trim_empty(metadata["cloud_cover_quad_ll"], None)
    cloudCoverLowerRight = trim_empty(metadata["cloud_cover_quad_lr"], None)

    sunElevation = metadata["sun_elevation"]
    sunAzimuth = metadata["sun_azimuth"]

    filesize = metadata.get("file_size", None)
    if filesize is None:
        filesize = metadata.get("l1_file_size", "0")

    ct_long = metadata["scene_center_lon"]
    ct_lat = metadata["scene_center_lat"]

    lt_long = metadata["corner_ul_lon"]
    lt_lat = metadata["corner_ul_lat"]

    rt_long = metadata["corner_ur_lon"]
    rt_lat = metadata["corner_ur_lat"]

    rb_long = metadata["corner_lr_lon"]
    rb_lat = metadata["corner_lr_lat"]

    lb_long = metadata["corner_ll_lon"]
    lb_lat = metadata["corner_ll_lat"]

    wkt = "POLYGON(( %s %s, %s %s, %s %s, %s %s, %s %s ))" % (
    lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, lb_lat, lt_long, lt_lat)

    insert_sql = """insert into metadata_landsat (dataID, satellite, dataType, sensor, lines, samples,
					stationId, dayNight, path, row, dataDate, dataDate_Year, 
					dataDate_Month, dataDate_Day, startTime, stopTime, imageQualityVCID1, imageQualityVCID2,
					cloudCover, cloudCoverUpperLeft, cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight, sunElevation, 
					sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, 
					rt_lat, rb_long, rb_lat, lb_long, lb_lat, filesize, the_geom ) values 
					(%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s,
					%s, %s, %s, %s, %s, %s, 
					%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));"""

    update_sql = """update metadata_landsat set satellite=%s, dataType=%s, sensor=%s, lines=%s, samples=%s,
					stationId=%s, dayNight=%s, path=%s, row=%s, dataDate=%s, dataDate_Year=%s, 
					dataDate_Month=%s, dataDate_Day=%s, startTime=%s, stopTime=%s, imageQualityVCID1=%s, imageQualityVCID2=%s,
					cloudCover=%s, cloudCoverUpperLeft=%s, cloudCoverUpperRight=%s, cloudCoverLowerLeft=%s, cloudCoverLowerRight=%s, sunElevation=%s, \
					sunAzimuth=%s, ct_long=%s, ct_lat=%s, lt_long=%s, lt_lat=%s, rt_long=%s, 
					rt_lat=%s, rb_long=%s, rb_lat=%s, lb_long=%s, lb_lat=%s, filesize=%s, the_geom=ST_GeomFromText(%s, 4326) where dataID=%s """

    sql = "select * from metadata_landsat where dataid='%s' " % (dataId)

    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (dataId, satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt))
        print "insert ", dataId

    else:
        pg_src.update(update_sql, (satellite, dataType, sensor, lines, samples, stationId, dayNight,
                                   path, row, datadate, datadate.year, datadate.month, datadate.day, startTime_f,
                                   stopTime_f,
                                   imageQualityVCID1, imageQualityVCID2, cloudCover, cloudCoverUpperLeft,
                                   cloudCoverUpperRight, cloudCoverLowerLeft, cloudCoverLowerRight,
                                   sunElevation, sunAzimuth, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long,
                                   rb_lat,
                                   lb_long, lb_lat, filesize, wkt, dataId))

        print "update ", dataId
