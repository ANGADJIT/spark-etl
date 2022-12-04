# spark-basic-etl
![spark-basic-etl](https://user-images.githubusercontent.com/67195682/205505330-647445a9-34e7-4900-b4fc-a4bd2b8087ad.png)
<br>

## Workflow :)
<dl>
<dt><b>Extracter</b></dt>
<dd>There are 2 data sources are used in this <b>POSTGRES</b> and <b>MONGODB</b> both contains stocks data of two diffrent companies. Extracter will connect to both data sources at same and get the data. Here I am passing extracter in <b>TRANSFORMER</b> as dependency.</dd>
<br>

<dt><b>Transformer</b></dt>
<dd>Transfomer taking <b>EXTRACTER</b> as dependency which we are using to extract raw data from data sources. Raw data will be converted into RDD and then in DataFrames. Then after converting to dataframes we are performing some transformation like filter , removing and adding the columns here I extracted Date into there columns DAY MONTH YEAR and removed Date. after this transformations I am making as class properties that <b>LOADER</b> can use this.</dd>
<br>

<dt><b>Loader</b></dt>
<dd>Loader is taking <b>TRANSFORMER</b> as dependency which we using to perform analytics and combining the data which we store.So here we are combining the AXISBANK and ADANIPORTS data then I am saving this data to hdfs under stocks/data and performing basic analytics the output i am ploting in graph and saving as .png in insights in HDFS.</dd>
</dl>
<br>

<img src="https://user-images.githubusercontent.com/67195682/205505470-cf3aeaee-96a9-45e3-a4a3-fcd271f36e29.png">

## Tool used
<ul>
<li>Vscode</li>
<li>Docker</li>
<li>Wsl</li>
</ul>
<br>

## How to setup project ?

### Setup DataSources 
```
docker run
    --name myPostgresDb
    -p 5455:5432
    -e POSTGRES_USER=postgresUser
    -e POSTGRES_PASSWORD=postgresPW
    -e POSTGRES_DB=postgresDB
    -d
    postgres
```
```
docker run -it -p 28000:27017 --name mongoContainer mongo:latest mongo
```
<br>

### Setup python environment
```
virtualenv .venv
source .venv/bin/activate
pip install -r requirments.txt --no-cache-dir
```

<br>

### Start HDFS and Spark server
```
<hadoop-path>/sbin/start-all.sh
<spark-path>/sbin/start-all.sh
jps
```
<br>

### Submit spark job 
```
<spark-path>/sbin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --py-files . \
    executer.py
```
