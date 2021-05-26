from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


def load_temp_state(spark):
    
    global_land_temperatures_by_state_path = root_path + "/temperatures_data/global_land_temperatures_by_state/GlobalLandTemperaturesByState.csv"

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
    
    global_temperatures_path = root_path + "/temperatures_data/global_temperatures/GlobalTemperatures.csv"

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

    global_land_temperatures_by_major_city_path = root_path + "/temperatures_data/global_land_temperatures_by_major_city/GlobalLandTemperaturesByMajorCity.csv"

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
    
    global_land_temperatures_by_country_path = root_path + "/temperatures_data/global_land_temperatures_by_country/GlobalLandTemperaturesByCountry.csv"

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
    
    global_land_temperatures_by_city_path = root_path + "/temperatures_data/global_land_temperatures_by_city/GlobalLandTemperaturesByCity.csv"

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
    
    us_cities_demographics_path = root_path + "/us_cities_demographics/us_cities_demographics.csv"

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
    
    airport_codes_csv_path = root_path + "/airport_codes/airport-codes_csv.csv"
    
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
    
    country_path = root_path + "/country_code_and_name/country_code_and_name.csv"

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
    
    transport_vehicle_path = root_path + "/transport_vehicle/transport_vehicle.csv"

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
    
    state_usa_path = root_path + '/state_usa/state_usa.csv'

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
    
    motivation_path = root_path + '/motivation/motivation.csv'

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
    immi_parquet_path_2 = root_path + '/immigration_data/*'
    
    return spark.read.parquet(immi_parquet_path_2)

def load_port(spark):
    
    port_path = root_path + '/port/port.csv'
    
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
            .withColumn('port_portal', trim(F.col('column_drop')[0]))\
            .withColumn('port_country_acronym', trim(F.col('column_drop')[1]))\
            .drop('column_drop')