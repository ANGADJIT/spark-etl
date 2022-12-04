from ..transformers.transformers import Transformers
from pyspark.sql import DataFrame
from pyspark.sql.functions import max as ag_max
from os.path import exists
from os import mkdir, system, remove


class Loader:

    def __init__(self, transformer: Transformers) -> None:
        self.__plt = self.__load_plt()
        self.__transformer = transformer
        
        system('hdfs dfs -mkdir -p /data/stocks/insights')

        # temp folder logic
        if exists('assets/temp') == False:
            mkdir('assets/temp')
    
    def __load_plt(self):
        import matplotlib.pyplot as plt

        return plt
        
    
    def load(self) -> None:
        self.__combine_data_save_to_hdfs()

        self.__perform_queries(column='Year')
        self.__perform_queries(column='Month')


    def __combine_data_save_to_hdfs(self) -> None:
        combined: DataFrame = self.__transformer.axisbank_stocks.union(
            self.__transformer.adaniports_stocks.select(*self.__transformer.adaniports_stocks.columns))
        
        self.__to_csv(dataframe=combined,file_name='combined_stocks')

    def __perform_queries(self, column: str) -> None:

        # QUERY1 : PLot a graph stock price comparision of AXISBANK Vs ADANIPORTS yearly
        axisbank: DataFrame = self.__transformer.axisbank_stocks.groupBy(
            column).agg(ag_max('Close').alias('price'))
        axisbank: DataFrame = axisbank.orderBy(column)

        adaniports: DataFrame = self.__transformer.adaniports_stocks.groupBy(
            column).agg(ag_max('Close').alias('price'))
        adaniports: DataFrame = adaniports.orderBy(column)

        # plot graph
        self.__plt.title('Axis Bank Vs Adaniports (2007-2021)')
        self.__plt.xlabel(column)
        self.__plt.ylabel('Stock Price')
        self.__plt.grid()
        self.__plt.plot(axisbank.select(column).toPandas()[column].astype(
            'int'), axisbank.select('price').toPandas().price.astype('float'), marker='o',label='Axisbank')
        self.__plt.plot(adaniports.select(column).toPandas()[column].astype(
            'int'), adaniports.select('price').toPandas().price.astype('float'), marker='D',label='Adaniports')
        self.__plt.legend()
        
        self.__save_graph_image_hdfs(image_name=column)

    def __to_csv(self, dataframe: DataFrame, file_name: str) -> None:
        dataframe.write.csv(f'/data/stocks/data/{file_name}')

    def __save_graph_image_hdfs(self, image_name: str) -> None:
        self.__plt.savefig(f'assets/temp/{image_name}.png')
        self.__plt.clf()

        system(f'hdfs dfs -put assets/temp/{image_name}.png /data/stocks/insights/')
        remove(f'assets/temp/{image_name}.png')


