from pymongo import MongoClient
import psycopg2

from ..utils import singleton_wrapper 

@singleton_wrapper.singleton
class Extracter:

    def __init__(self) -> None: # connect to database [MONGO-DB,POSTGRES] container
        
        # connect to mongo db
        MONGO_URI:str = 'mongodb://root:root@localhost:27017/?authSource=admin&readPreference=primary&ssl=false&directConnection=true' 

        self.__stocks_db = MongoClient(MONGO_URI).stocks

        # connect to postgres
        conn = psycopg2.connect(
            database="postgres", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
        )

        self.__axisbank = conn.cursor()


    def get_adaniports_mongo_db(self) -> list[dict]:
        return list(self.__stocks_db.adaniports.find({},{'_id': 0}))

    def get_axisbank_postgres_db(self) -> list[tuple]:
        self.__axisbank.execute('SELECT * FROM AXISBANK')

        return [ dict(line) for line in [zip([ column[0] for column in self.__axisbank.description if column[0] != 'index'], row) for row in self.__axisbank.fetchall()] ]
