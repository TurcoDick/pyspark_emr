from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import configparser
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')


def load_temp_state(spark):
    
    global_land_temperatures_by_state_path = config['PATH']['GLOBAL_LAND_TEMPERATURES_BY_STATE_PATH']
    global_temperatures_by_state_schema = StructType([\
                                                  StructField("t_state_dt", DateType(), False),
                                                  StructField('t_state_average_temperature', DoubleType(), False),
                                                  StructField('t_state_average_temperature_uncertainty', DoubleType(), False),
                                                  StructField('t_state_state', StringType(), False),
                                                  StructField('t_state_country', StringType(), False)
                                          ]) 
    
    return spark\
    .read\
    .format('com.databricks.spark.csv')\
    .option("sep",",")\
    .option("header", "true")\
    .option("encoding", "UTF-8")\
    .schema(global_temperatures_by_state_schema)\
    .load(global_land_temperatures_by_state_path)

def load_temp_glob(spark):
    
    global_temperatures_path = config['PATH']['GLOBAL_TEMPERATURES_PATH']

    global_temperatures_schema = StructType([ \
                                  StructField("t_glob_dt", DateType(), False),           
                                  StructField("t_glob_land_average_temperature", DoubleType(), False),
                                  StructField("t_glob_land_average_temperature_uncertainty", DoubleType(), False),
                                  StructField("t_glob_land_max_temperature", DoubleType(), False), 
                                  StructField("t_glob_land_max_temperature_uncertainty", DoubleType(), False),
                                  StructField("t_glob_land_min_temperature", DoubleType(), False), 
                                  StructField("t_glob_land_min_temperature_uncertainty", DoubleType(), False),
                                  StructField("t_glob_land_and_ocean_average_temperature", DoubleType(), False),
                                  StructField("t_glob_land_and_ocean_average_temperature_uncertainty", DoubleType(), False)           
                            ])
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',',')\
            .option("header", "true")\
            .option("encoding", "UTF-8")\
            .schema(global_temperatures_schema)\
            .load(global_temperatures_path)

def load_temp_major_city(spark):

    global_land_temperatures_by_major_city_path = config['PATH']['GLOBAL_LAND_TEMPERATURES_BY_MAJOR_CITY_PATH']

    global_temp_major_city_schema = StructType([\
                                                StructField("t_m_city_dt", DateType(), False),
                                                StructField("t_m_city_average_temperature", DoubleType(), False),
                                                StructField("t_m_city_average_temperature_uncertainty", DoubleType(), False),
                                                StructField("t_m_city_city", StringType(), False),
                                                StructField("t_m_city_country", StringType(), False),
                                                StructField("t_m_city_latitude", StringType(), False),
                                                StructField("t_m_city_longitude", StringType(), False)
                                ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',',')\
            .option('header', 'true')\
            .option("encoding", "UTF-8")\
            .schema(global_temp_major_city_schema)\
            .load(global_land_temperatures_by_major_city_path)


def load_temp_by_country(spark):
    
    global_land_temperatures_by_country_path = config['PATH']['GLOBAL_LAND_TEMPERATURES_BY_COUNTRY_PATH']

    global_land_temp_by_country_schema = StructType([\
                                                      StructField("t_country_dt", DateType(), False),
                                                      StructField("t_country_average_temperature", DoubleType(), False),
                                                      StructField("t_country_average_temperature_uncertainty", DoubleType(), False),
                                                      StructField("t_country_country", StringType(), False)
                                      ])
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',',')\
            .option('header', 'true')\
            .option("encoding", "UTF-8")\
            .schema(global_land_temp_by_country_schema)\
            .load(global_land_temperatures_by_country_path)

def load_temp_by_city(spark):
    
    global_land_temperatures_by_city_path = config['PATH']['GLOBAL_LAND_TEMPERATURES_BY_CITY_PATH']

    global_land_temp_by_city_schema = StructType([\
                                                  StructField("t_city_dt", DateType(), False),
                                                  StructField("t_city_average_temperature", DoubleType(), False),
                                                  StructField("t_city_average_temperature_uncertainty", DoubleType(), False),
                                                  StructField("t_city_city", StringType(), False),
                                                  StructField("t_city_country", StringType(), False),
                                                  StructField("t_city_latitude", StringType(), False),
                                                  StructField("t_city_longitude", StringType(), False)
                                      ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',',')\
            .option('header', 'true')\
            .option("encoding", "utf-8")\
            .schema(global_land_temp_by_city_schema)\
            .load(global_land_temperatures_by_city_path)


def load_cities_demographics(spark):
    
    us_cities_demographics_path = config['PATH']['US_CITIES_DEMOGRAPHICS_PATH']

    us_cities_demog_schema = StructType([\
                                        StructField("cit_demog_city", StringType(), False),
                                        StructField("cit_demog_state", StringType(), False),
                                        StructField("cit_demog_median_age", DoubleType(), False),
                                        StructField("cit_demog_male_population", IntegerType(), False),
                                        StructField("cit_demog_female_population", IntegerType(), False),
                                        StructField("cit_demog_total_polulation", IntegerType(), False),
                                        StructField("cit_demog_number_veterans", IntegerType(), False),
                                        StructField("cit_demog_foreign_born", IntegerType(), False),
                                        StructField("cit_demog_average_household_size", DoubleType(), False),
                                        StructField("cit_demog_state_code", StringType(), False),
                                        StructField("cit_demog_race", StringType(), False),
                                        StructField("cit_demog_quant", IntegerType(), False)
                                        ])
    
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',';')\
            .option('header', 'true')\
            .option("encoding", "utf-8")\
            .schema(us_cities_demog_schema)\
            .load(us_cities_demographics_path)


def load_airport_codes(spark):
    
    airport_codes_csv_path = config['PATH']['AIRPORT_CODES_CSV_PATH']
    
    airport_codes_schema = StructType([\
                                       StructField("airp_ident", StringType(), False),
                                       StructField("airp_type", StringType(), False),
                                       StructField("airp_name", StringType(), False),
                                       StructField("airp_elevation_ft", IntegerType(), False),
                                       StructField("airp_continent", StringType(), False),
                                       StructField("airp_iso_country", StringType(), False),
                                       StructField("airp_iso_region", StringType(), False),
                                       StructField("airp_municipality", StringType(), False),
                                       StructField("airp_gps_code", StringType(), False),
                                       StructField("airp_iata_code", StringType(), False),
                                       StructField("airp_local_code", StringType(), False),
                                       StructField("airp_coordinates", StringType(), False)
                                      ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',',')\
            .option('header', 'true')\
            .option("encoding", "utf-8")\
            .schema(airport_codes_schema)\
            .load(airport_codes_csv_path)


def load_country(spark):
    
    country_path = config['PATH']['COUNTRY_PATH']

    country_schema = StructType([\
                                  StructField("country_code", IntegerType(), False),
                                  StructField("country_name", StringType(), False)
                                 ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',';')\
            .option('header', 'true')\
            .option("encoding", "utf-8")\
            .schema(country_schema)\
            .load(country_path)


def load_transport_vehicle(spark):
    
    transport_vehicle_path = config['PATH']['TRANSPORT_VEHICLE_PATH']

    transport_vehicle_schema = StructType([\
                                           StructField("vehi_code", IntegerType(), False),
                                           StructField("vehi_name", StringType(), False)
                                          ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep',';')\
            .option('header', 'true')\
            .option("encoding", "utf-8")\
            .schema(transport_vehicle_schema)\
            .load(transport_vehicle_path)


def load_state_usa(spark):
    
    state_usa_path = config['PATH']['STATE_USA_PATH']

    state_usa_schema = StructType([\
                                   StructField("state_usa_code", StringType(), False),
                                   StructField("state_usa_name", StringType(), False)
                                  ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep', ';')\
            .option('header', 'true')\
            .option('encoding', 'utf-8')\
            .schema(state_usa_schema)\
            .load(state_usa_path)

def load_motivation(spark):
    
    motivation_path = config['PATH']['MOTIVATION_PATH']

    motivation_schema = StructType([\
                                    StructField("motiv_code", IntegerType(), False),
                                    StructField("motiv_name", StringType(), False)
                                   ])
    
    return spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep', ';')\
            .option('header', 'true')\
            .option('encoding', 'utf-8')\
            .schema(motivation_schema)\
            .load(motivation_path)


def load_immigration(spark):
    immi_parquet_path_2 = config['PATH']['IMMIGRATION_PATH']
    
    return spark.read.parquet(immi_parquet_path_2)

def load_port(spark):
    
    port_path = config['PATH']['PORT_PATH']
    
    port_schema = StructType([\
                               StructField("port_code", StringType(), False),
                               StructField("port_name", StringType(), False)
                             ])
    
    df_port = spark\
            .read\
            .format('com.databricks.spark.csv')\
            .option('sep', ';')\
            .option('header', 'false')\
            .option('encoding', 'utf-8')\
            .schema(port_schema)\
            .load(port_path)

    return df_port\
            .withColumn('column_drop', F.split(df_port['port_name'], ','))\
            .withColumn('port_portal', F.trim(F.col('column_drop')[0]))\
            .withColumn('port_country_acronym', F.trim(F.col('column_drop')[1]))\
            .drop('column_drop')