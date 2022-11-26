from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from ..extracters.extracter import Extracter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import split,col



class Transformers:

    def __init__(self,extracter: Extracter,master: str='local[*]') -> None:

        # configure spark session and context
        conf: SparkConf = SparkConf().setAppName(
            'stocks ETL').setMaster(master)
        self.__sc: SparkContext = SparkContext(conf=conf)

        self.__spark = SparkSession.builder.appName(
            'stocks ETL').master(master).getOrCreate()

        # load data and convert into data frame for processing

        axisbank_stocks: list[tuple] = extracter.get_axisbank_postgres_db(
        )
        adaniports_stocks: list[dict] = extracter.get_adaniports_mongo_db(
        )

        axisbank_stocks_rdd: RDD = self.__sc.parallelize(axisbank_stocks)
        adaniports_stocks_rdd: RDD = self.__sc.parallelize(adaniports_stocks)

        # schema for axisbank data
        axisbank_stocks_schema: StructType = StructType([
            StructField('index',IntegerType(),False),
            StructField('Date',StringType(),False),
            StructField('Symbol',StringType(),False),
            StructField('Series',StringType(),False),
            StructField('Prev Close',StringType(),False),
            StructField('Open',FloatType(),False),
            StructField('High',FloatType(),False),
            StructField('Low',FloatType(),False),
            StructField('Last',FloatType(),False),
            StructField('Close',FloatType(),False),
            StructField('VWAP',FloatType(),False),
            StructField('Volume',IntegerType(),False),
            StructField('Turnover',FloatType(),False),
            StructField('Trades',FloatType(),True),
            StructField('Deliverable Volume',FloatType(),True),
            StructField('%Deliverble',FloatType(),True),
        ])

        self.__axisbank_stocks_df: DataFrame = self.__spark.createDataFrame(axisbank_stocks_rdd,schema=axisbank_stocks_schema)

        self.__adaniports_stocks_df: DataFrame = self.__spark.read.json(adaniports_stocks_rdd,multiLine=True)

        # transform the data
        self.__transform()
    
    def __transform(self) -> None:
        self.__axisbank_stocks_df = self.__axisbank_stocks_df.drop('index')
        
        axisbank_stocks_df: DataFrame = self.__axisbank_stocks_df.withColumn('Month',split(col('Date'),'-')[1])
        axisbank_stocks_df = axisbank_stocks_df.withColumn('Day',split(col('Date'),'-')[2])
        axisbank_stocks_df = axisbank_stocks_df.withColumn('Year',split(col('Date'),'-')[0])
        axisbank_stocks_df = axisbank_stocks_df.drop('Date')

        axisbank_stocks_df = axisbank_stocks_df.filter(axisbank_stocks_df.Year >= 2007)

        self.__axisbank_stocks_df = axisbank_stocks_df

        adaniports_stocks_df = self.__adaniports_stocks_df.withColumn('Month',split(col('Date'),'-')[1])
        adaniports_stocks_df = adaniports_stocks_df.withColumn('Day',split(col('Date'),'-')[2])
        adaniports_stocks_df = adaniports_stocks_df.withColumn('Year',split(col('Date'),'-')[0])
        adaniports_stocks_df = adaniports_stocks_df.drop('Date')

        self.__adaniports_stocks_df = adaniports_stocks_df

    

    # properties to be used for analytics
    @property
    def adaniports_stocks(self) -> DataFrame:
        return self.__adaniports_stocks_df
    
    @property
    def axisbank_stocks(self) -> DataFrame:
        return self.__axisbank_stocks_df
    
    @property
    def spark_context(self) -> SparkContext:
        return self.__sc
    
    @property
    def spark_session(self) -> SparkSession:
        return self.__spark
    

    # desctructor 
    def __del__(self):
        self.__sc.stop()
        self.__spark.stop()
