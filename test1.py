import boto
from boto.s3.key import Key
import boto.rds
import MySQLdb
import csv
import os
import sys
import timeit
AWS_ACCESS_KEY_ID = 'AKIAJFOGRX6T3Y6LA5XA'
AWS_SECRET_ACCESS_KEY = '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'
bucket_name = 'pa4-cloud-computing'
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)


def main():
    #insert_database()
    display_database_1()
    display_database()
    display_database_5()


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


def display_database():
    db = database_connection()
    cursor = db.cursor()
    print " Executing 20 k or more queries "
    start = timeit.default_timer()
    cursor.execute("select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 2");
    count = 0 
    for rows in cursor.fetchall():
    	variable = str(rows[0])
	cursor.execute("select * from weather1 where atmospheric_pressure_mBar ="+variable+"")
	count = count +1
	print variable
    end = timeit.default_timer()
    print " Total queries executed "
    print count
    print " Total Time taken for executing 20k queries: "
    print start-end

def display_database_5():
    db = database_connection()
    cursor = db.cursor()
    print " Executing 5k  or more queries "
    start = timeit.default_timer()
    cursor.execute("select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.5");
    count = 0
    for rows in cursor.fetchall():
        variable = str(rows[0])
        cursor.execute("select * from weather1 where atmospheric_pressure_mBar ="+variable+"")
        print variable
	count = count +1
    end = timeit.default_timer()
    print " Total queries executed "
    print count
    print " Total Time taken for executing 5k or more  queries: "
    print end-start

def display_database_1():
    db = database_connection()
    cursor = db.cursor()
    print " Executing 1k or more queries "
    start = timeit.default_timer()
    cursor.execute("select surface_temperature_C from pa4cloud.weather1 where surface_temperature_C between 0 and 0.1");
    count = 0
    for rows in cursor.fetchall():
        variable = str(rows[0])
        cursor.execute("select * from weather1 where atmospheric_pressure_mBar ="+variable+"")
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
    k.set_contents_from_filename('C:/Users/SONY/Desktop/Summer 2015/PA4/myfile.csv')
    print " File Uploaded Successfully"

if __name__ == '__main__':
  main()
# [END all]
