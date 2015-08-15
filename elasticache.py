# Name : Kabra Chetan Rajesh
#Student ID : 1001152872

"""References Used
http://aws.amazon.com/developers/getting-started/python/
http://boto.readthedocs.org/en/latest/
http://aws.amazon.com/documentation/
http://boto.readthedocs.org/en/latest/ec2_tut.html
http://boto.readthedocs.org/en/latest/rds_tut.html
http://blog.echolibre.com/2009/11/memcache-and-python-getting-started/4/ - Author David Coallier

Data Set Used :
http://www.geos.ed.ac.uk/abs/Weathercam/station/data.html

"""

import boto
from boto.s3.key import Key
import boto.rds
import MySQLdb
import csv
import timeit
import hashlib
import pa4_final

AWS_ACCESS_KEY_ID = 'AKIAJFOGRX6T3Y6LA5XA'
AWS_SECRET_ACCESS_KEY = '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'
bucket_name = 'pa4-cloud-computing'
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)

memc = pa4_final.Client(['assigment4.oxsevf.0001.usw2.cache.amazonaws.com:11211'])

def main():
    #insert_database()
    display_database_1()
    #display_database()
    #display_database_5()


def database_connection():
    try:
        db = MySQLdb.connect(host = "pa4-aws.cpvcrratghwj.us-west-2.rds.amazonaws.com", user="chetankabra8", passwd="8kabraakash", port=3306, db="pa4cloud")
        cursor = db.cursor()
        print " Connectioned sucessfully"
    except:
        print "Not able to connect to database "
    cursor.execute("""CREATE TABLE IF NOT EXISTS weather1 (
    date_time VARCHAR(14) CHARACTER SET utf8,
    atmospheric_pressure_mBar INT,
    rainfall_mm NUMERIC(2, 1),
    wind_speed_m_s NUMERIC(5, 3),
    wind_direction_degrees NUMERIC(6, 3),
    surface_temperature_C NUMERIC(5, 3),
    relative_humidity NUMERIC(3, 1),
    solar_flux_Kw_m2 NUMERIC(8, 7),
    battery_V NUMERIC(4, 2)
    );""")
    return db

def insert_database():
    db = database_connection()
    cursor = db.cursor()
    file_name = open('Book1.csv','r')
    filename= csv.reader(file_name)
    filename.next()
    count =0
    start = timeit.default_timer()
    for rows in filename:
        cursor.execute("""insert into pa4cloud.weather1 (date_time, atmospheric_pressure_mBar, rainfall_mm, wind_speed_m_s, wind_direction_degrees, surface_temperature_C, relative_humidity, solar_flux_Kw_m2, battery_V) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",rows)
        cursor.execute("commit;")
        count = count +1
    cursor.close()
    end = timeit.default_timer()
    print "Data Inserted Successfully"
    print " Total time taken to Inserted Large Dataset = "
    print end-start
    print " Total Number of Tuples "
    print count


def display_database_new():
    db = database_connection()
    cursor = db.cursor()
    print " Select the Number of Queries you need to Execute From Below Option :"
    print " 1 = 1K or More Queries  "
    print " 2 = 5K or More Queries  "
    print " 3 = 20K or More Queries  "
    value = int(raw_input())
    if value == 1:
        queries = "1k"
        sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.1"
    elif value == 2:
        queries = "5k"
        sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.5"
    elif value ==3 :
        queries = "20k"
        sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 2.5"
    print " Executing %s or more queries " %queries
    #sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.1"
    memc_cache = hashlib.md5(sql.encode())
    memc_cache = memc_cache.hexdigest()
    data = memc.get(memc_cache)
    if not data:
        cursor.execute(sql)
        row = cursor.fetchall()
        memc.set(memc_cache,row)
        start = timeit.default_timer()
        count = 0
        for rows in row:
            variable = str(rows[0])
            inner_query = "select * from weather1 where atmospheric_pressure_mBar ="+variable+""
            inner_query_data = hashlib.md5(inner_query.encode())
            inner_query_data = inner_query_data.hexdigest()
            data_new = memc.get(inner_query_data)
            if not data_new:
                cursor.execute(inner_query)
                random_query = cursor.fetchall()
                memc.set(inner_query_data,random_query)
                count = count +1
            else:
                count = count +1
        end = timeit.default_timer()
        print " Total queries executed "
        print count
        print " Total Time taken for executing %s or more  queries: " %queries
        print end-start
    else:
        start = timeit.default_timer()
        count = 0
        for rows in data:
            variable = str(rows[0])
            inner_query = "select * from weather1 where atmospheric_pressure_mBar ="+variable+""
            inner_query_data = hashlib.md5(inner_query.encode())
            inner_query_data = inner_query_data.hexdigest()
            memc.get(inner_query_data)
            count = count +1
        end = timeit.default_timer()
        print " Total queries executed "
        print count
        print " Total Time taken for executing %s or more  queries ( Memcache ): " %queries
        print end-start

def display_database_k_limit():
    db = database_connection()
    cursor = db.cursor()
    print " Executing 1k or more queries "
    print " Enter the Number of limit on Query :"
    limit = int(raw_input())
    print " Select the Number of Queries you need to Execute From Below Option :"
    print " 1 = 1K or More Queries  "
    print " 2 = 5K or More Queries  "
    print " 3 = 20K or More Queries  "
    value = int(raw_input())
    if value == 1:
        queries = "1k"
        sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.1"
    elif value == 2:
        queries = "5k"
        sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.5"
    elif value ==3 :
        queries = "20k"
        sql = "select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 2.5"
    cursor.execute(sql)
    row = cursor.fetchall()
    start = timeit.default_timer()
    count = 0
    for rows in row:
        variable = str(rows[0])
        inner_query = "select * from weather1 where atmospheric_pressure_mBar ="+variable+" LIMIT "+limit+""
        cursor.execute(inner_query)
        random_query = cursor.fetchall()
        count = count +1
    end = timeit.default_timer()
    print " Total queries executed "
    print count
    print " Total Time taken for executing 1k or more  queries: "
    print end-start

def upload_csv():
    bucket = conn.lookup(bucket_name)
    bucket = conn.get_bucket(bucket_name, validate=False)
    k = Key(bucket)
    k.key = 'firstfile.csv'
    start = timeit.default_timer()
    k.set_contents_from_filename('C:/Users/SONY/Desktop/Summer 2015/PA4/Book1.csv')
    end =timeit.default_timer()
    print " File Uploaded Successfully"
    print " Time Taken to Upload File to S3 Bucket : "
    print end-start
if __name__ == '__main__':
  main()
# [END all]
