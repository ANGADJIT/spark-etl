from pymongo import MongoClient
import psycopg2

from ..utils import singleton_wrapper 

@singleton_wrapper.singleton
class Extracter:

    def __init__(self) -> None: # connect to database [MONGO-DB,POSTGRES] conatiner
        
        # connect to mongo db
        MONGO_URI:str = 'mongodb://root:root@localhost:27017/?authSource=admin&readPreference=primary&ssl=false&directConnection=true' 

        self.__adaniports_collection = MongoClient(MONGO_URI).stocks.adaniports

        # connect to postgres
        conn = psycopg2.connect(
            database="postgres", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
        )

        self.__axisbank = conn.cursor()

    
if __name__ == '__main__':
    Extracter()

    





