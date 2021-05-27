import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import col
from pyspark.sql.functions import *

from load import load_temp_by_city, load_temp_state, load_temp_glob, load_temp_major_city, load_temp_by_country,\
    load_temp_by_city, load_cities_demographics, load_airport_codes, load_country, \
    load_transport_vehicle, load_state_usa, load_motivation, load_immigration, load_port,     

config = configparser.ConfigParser()
config.read('dl.cfg')

key = config['AWS']['AWS_ACCESS_KEY_ID']
secret = config['AWS']['AWS_SECRET_ACCESS_KEY']

print('AWS_ACCESS_KEY_ID={} AWS_SECRET_ACCESS_KEY={}'.format(key, secret))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    spark configuration.
    """
    print("Start the application")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5")\
        .getOrCreate()
    return spark    

    

def big_table(spark):
    
    df_temp_glob = load_temp_glob(spark).distinct()
    
    df_temp_state = load_temp_state(spark).select("t_state_state", "t_state_country").distinct()
    
    df_temp_major_city = load_temp_major_city(spark).distinct()
    
    df_temp_country = load_temp_by_country(spark).distinct()
    
    df_temp_city = load_temp_by_city(spark).select("t_city_country", "t_city_city").distinct()
    
    df_cities_demog = load_cities_demographics(spark).select("cit_demog_city").distinct()
    
    df_airport_codes = load_airport_codes(spark).distinct()
    
    df_country = load_country(spark).select("country_name", "country_code").distinct()
    
    df_transport_vehicle = load_transport_vehicle(spark).distinct()
    
    df_state_usa = load_state_usa(spark).distinct()
    
    df_motivation = load_motivation(spark).distinct()

    df_immigration = load_immigration(spark).distinct()
    
    df_port = load_port(spark).distinct()
    
    df_join = broadcast(df_state_usa)\
    .join(df_temp_state, upper(df_state_usa.state_usa_name) == upper(df_temp_state.t_state_state)) \
    .join(df_temp_city, upper(df_temp_state.t_state_country) == upper(df_temp_city.t_city_country)) \
    .join(df_cities_demog, upper(df_temp_city.t_city_city) == upper(df_cities_demog.cit_demog_city)) \
    .join(df_airport_codes, upper(df_airport_codes.airp_municipality) == upper(df_cities_demog.cit_demog_city)) \
    .join(df_immigration, upper(df_immigration.i94addr) == upper(df_state_usa.state_usa_code)) \
    .join(broadcast(df_transport_vehicle), df_immigration.i94mode == df_transport_vehicle.vehi_code) \
    .join(broadcast(df_motivation), df_motivation.motiv_code == df_immigration.i94visa) \
    .drop('cit_demog_city', 'state_usa_name', 't_state_country', 'state_usa_code', 'airp_gps_code', 'airp_local_code', \
         't_city_country', 'country_code', 'vehi_code', 'motiv_code', 'country_code', 'airp_continent', \
         'airp_iata_code')
    
    return df_join


def vehicle_motivation(result):
    return result.filter('vehi_name', 'motiv_name', 'cicid')\
        .groupBy('vehi_name', 'motiv_name')\
        .agg(count('cicid')\
        .alias('total_count_immi_by_vehicle'))\
        .sort(desc('total_count_immi_by_vehicle'))


def airport(result):
    return result.filter('airp_name', 'motiv_name', 'cicid')\
        .groupBy('airp_name', 'motiv_name')\
        .agg(count('cicid').alias('total_count_immit'))\
        .sort(desc("total_count_immit"))

def year_immi(result):
    return result.filter('i94yr', 'motiv_name', 'cicid')\
        .groupBy('i94yr', 'motiv_name')\
        .agg(count('cicid').alias('total_count_motiv_by_year'))\
        .sort(desc('i94yr'))       

def main():
    spark = create_spark_session()
    result = big_table(spark)

    vehicle_motivation(result)
    airport(result)
    year_immi(result)

