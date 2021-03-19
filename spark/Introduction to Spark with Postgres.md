# Intro to Programming with Spark

# References

* http://localhost:4040
* https://spark.apache.org/docs/latest/api/python/index.html
* https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions

## Install

**Set-up Instructions:**

1. Install Spark (latest)
    1. Prebuilt for version
2. Install Open JDK (8 or 11) aka Java
3. Install Python3
4. Pip3 install pyspark
5. Set up environment variables
    * PYTHONPATH
    * PYSPARK_PYTHON
    * SPARK_HOME
    * JAVA_HOME
6. Install jdbc postgres driver for version of Spark
    * Save in `/Spark/spark-3.0.1-bin-hadoop3.2/jars/`
    
**Opening Spark**

* Open PySpark in Terminal or PowerShell.
    * `pyspark`
* To open a jupyter notebook with pyspark, first set these env variables in your terminal or the equivalent commands in PowerShell, before running the pyspark command.
    * `export PYSPARK_DRIVER_PYTHON=jupyter` 
    * `export PYSPARK_DRIVER_PYTHON_OPTS='notebook'`
* Run a script with Spark.
    * `spark-submit my_filename.py`
    * pass packages to spark with `spark-submit --packages package_name`

## Two APIs to Know

The following APIs, along with a number of additional (unmentioned here) Spark APIs, provide a Python programming interface to Spark.

* **DataFrame API**
    - Spark DataFrame is not the same as a Pandas DataFrame, though it can be changed to one if needed.
    
* **Spark SQL API**
    - We create a temp view (or global view) and query the view.
    - The view will persist our changes. No need to keep creating & pulling down the data!

## Transformations versus Actions

**Transformations**
- Transformations include all:
    * selecting
    * filtering
    * grouping
    * summing
    * ordering 
    * etc.

**Actions**
- Actions include:
    * View data
    * Collect data
    * Write data

## Spark Context & Session

* **Spark Context:** entry point object to Spark
* **Spark Session:** _unified_ entry point object to Spark [2.0+]


- In Jupyter notebooks and in pyspark shell, the Session is created for you. In scripts, you must specify it.

```
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName('MyApp').getOrCreate()
```

## 1. Connect to Postgres


```python
# set up user name and password for postgres
my_username = ''
my_password = ''
```


```python
# connect to the database
# make sure all args are right & connected to db if stalling
offices = spark.read.format("jdbc") \
.option("url", "jdbc:postgresql://localhost:5433/my_database") \
.option("dbtable", "offices") \
.option("driver", "org.postgresql.Driver") \
.option("user" , my_username) \
.option("password", my_password) \
.load()

# get a feel for the dataframe
offices.printSchema()
offices.limit(3).show()

# Set up for spark sql API
offices.createOrReplaceTempView("offices")
```

    root
     |-- id: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- address: string (nullable = true)
     |-- address2: string (nullable = true)
     |-- city: string (nullable = true)
     |-- state: string (nullable = true)
     |-- zipcode: string (nullable = true)
     |-- date_added: date (nullable = true)
    
    +---+---------+---------------+--------+----------+-----+-------+----------+
    | id|     name|        address|address2|      city|state|zipcode|date_added|
    +---+---------+---------------+--------+----------+-----+-------+----------+
    |  1|Location1| 123 Place Road|    null|  New York|   NY|  10001|2021-03-18|
    |  2|Location2|555 Rock Avenue|    null|    Boston|   MA|  02101|2021-03-18|
    |  3|Location3|838 Rock Avenue|    null|Pittsburgh|   PA|  15106|2021-03-18|
    +---+---------+---------------+--------+----------+-----+-------+----------+
    



```python
employees = spark.read.format("jdbc") \
.option("url", "jdbc:postgresql://localhost:5433/my_database") \
.option("dbtable", "employees") \
.option("driver", "org.postgresql.Driver") \
.option("user" , my_username) \
.option("password", my_password) \
.load()

# get a feel for the dataframe
employees.printSchema()
employees.limit(3).show()

# Set up for spark sql API
employees.createOrReplaceTempView("employees")
```

    root
     |-- id: integer (nullable = true)
     |-- firstname: string (nullable = true)
     |-- lastname: string (nullable = true)
     |-- age: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- location_id: integer (nullable = true)
     |-- date_added: date (nullable = true)
    
    +---+---------+--------+---+---------------+-----------+----------+
    | id|firstname|lastname|age|          title|location_id|date_added|
    +---+---------+--------+---+---------------+-----------+----------+
    |  1|  Rudolph|Reindeer| 23|  Lead Reindeer|          1|2021-03-18|
    |  2|   Dasher|Reindeer| 24|Junior Reindeer|          1|2021-03-18|
    |  3|  Prancer|Reindeer| 25|Junior Reindeer|          2|2021-03-18|
    +---+---------+--------+---+---------------+-----------+----------+
    


## 2. Query

**Example Includes:**

    * select()
    * where() 
    * isin()
    * orderBy()
    * show()


```python
# dataframe api
# col gives you column manipulation
from pyspark.sql.functions import col
offices.select(col("name")).where(col("name").isin('Location1','Location2')) \
.orderBy(col("name")) \
.show()

# spark sql api
# reminder: this is actually querying the "temp view"
offices_filtered = spark.sql("""
                    select name 
                    from offices
                    where name in ('Location1','Location2')
                    order by name
                    """
                    )
offices_filtered.show()
```

    +---------+
    |     name|
    +---------+
    |Location1|
    |Location2|
    +---------+
    
    +---------+
    |     name|
    +---------+
    |Location1|
    |Location2|
    +---------+
    


## 3. Join

**Example Includes:**

* alias() ~ the DataFrame
* join()
* select()
* show() ~ with Truncate = False 


```python
# dataframe api
offices.alias("o")
employees.alias("e")
join_exp = offices["id"] == employees["location_id"]

offices.join(employees, join_exp, "inner") \
.select("*") \
.show(50, False)

# spark sql api
table_join = spark.sql("""
                    select * 
                    from offices o
                    join employees e
                    on o.id = e.location_id
                    """
                    )
table_join.show(50, False)
```

    +---+---------+---------------+--------+------------+-----+-------+----------+---+---------+--------+---+------------------+-----------+----------+
    |id |name     |address        |address2|city        |state|zipcode|date_added|id |firstname|lastname|age|title             |location_id|date_added|
    +---+---------+---------------+--------+------------+-----+-------+----------+---+---------+--------+---+------------------+-----------+----------+
    |1  |Location1|123 Place Road |null    |New York    |NY   |10001  |2021-03-18|1  |Rudolph  |Reindeer|23 |Lead Reindeer     |1          |2021-03-18|
    |1  |Location1|123 Place Road |null    |New York    |NY   |10001  |2021-03-18|2  |Dasher   |Reindeer|24 |Junior Reindeer   |1          |2021-03-18|
    |3  |Location3|838 Rock Avenue|null    |Pittsburgh  |PA   |15106  |2021-03-18|5  |Comet    |Reindeer|32 |Associate Reindeer|3          |2021-03-18|
    |3  |Location3|838 Rock Avenue|null    |Pittsburgh  |PA   |15106  |2021-03-18|6  |Cupid    |Reindeer|35 |Senior Reindeer   |3          |2021-03-18|
    |5  |Location5|210 Windy Road |null    |Minneapolis |MN   |55111  |2021-03-18|9  |Blitzen  |Reindeer|43 |Co-Captain        |5          |2021-03-18|
    |4  |Location4|321 Pop Street |null    |Philadelphia|PA   |19019  |2021-03-18|7  |Dancer   |Reindeer|37 |Senior Reindeer   |4          |2021-03-18|
    |4  |Location4|321 Pop Street |null    |Philadelphia|PA   |19019  |2021-03-18|8  |Donner   |Reindeer|38 |Co-Captain        |4          |2021-03-18|
    |2  |Location2|555 Rock Avenue|null    |Boston      |MA   |02101  |2021-03-18|3  |Prancer  |Reindeer|25 |Junior Reindeer   |2          |2021-03-18|
    |2  |Location2|555 Rock Avenue|null    |Boston      |MA   |02101  |2021-03-18|4  |Vixen    |Reindeer|28 |Associate Reindeer|2          |2021-03-18|
    +---+---------+---------------+--------+------------+-----+-------+----------+---+---------+--------+---+------------------+-----------+----------+
    
    +---+---------+---------------+--------+------------+-----+-------+----------+---+---------+--------+---+------------------+-----------+----------+
    |id |name     |address        |address2|city        |state|zipcode|date_added|id |firstname|lastname|age|title             |location_id|date_added|
    +---+---------+---------------+--------+------------+-----+-------+----------+---+---------+--------+---+------------------+-----------+----------+
    |1  |Location1|123 Place Road |null    |New York    |NY   |10001  |2021-03-18|1  |Rudolph  |Reindeer|23 |Lead Reindeer     |1          |2021-03-18|
    |1  |Location1|123 Place Road |null    |New York    |NY   |10001  |2021-03-18|2  |Dasher   |Reindeer|24 |Junior Reindeer   |1          |2021-03-18|
    |3  |Location3|838 Rock Avenue|null    |Pittsburgh  |PA   |15106  |2021-03-18|5  |Comet    |Reindeer|32 |Associate Reindeer|3          |2021-03-18|
    |3  |Location3|838 Rock Avenue|null    |Pittsburgh  |PA   |15106  |2021-03-18|6  |Cupid    |Reindeer|35 |Senior Reindeer   |3          |2021-03-18|
    |5  |Location5|210 Windy Road |null    |Minneapolis |MN   |55111  |2021-03-18|9  |Blitzen  |Reindeer|43 |Co-Captain        |5          |2021-03-18|
    |4  |Location4|321 Pop Street |null    |Philadelphia|PA   |19019  |2021-03-18|7  |Dancer   |Reindeer|37 |Senior Reindeer   |4          |2021-03-18|
    |4  |Location4|321 Pop Street |null    |Philadelphia|PA   |19019  |2021-03-18|8  |Donner   |Reindeer|38 |Co-Captain        |4          |2021-03-18|
    |2  |Location2|555 Rock Avenue|null    |Boston      |MA   |02101  |2021-03-18|3  |Prancer  |Reindeer|25 |Junior Reindeer   |2          |2021-03-18|
    |2  |Location2|555 Rock Avenue|null    |Boston      |MA   |02101  |2021-03-18|4  |Vixen    |Reindeer|28 |Associate Reindeer|2          |2021-03-18|
    +---+---------+---------------+--------+------------+-----+-------+----------+---+---------+--------+---+------------------+-----------+----------+
    


## 4. Insert

Sorry. **No way to update or delete via Spark.** That's okay, we don't need to use Spark for this. We can, however, insert new records.

**Reference** 
* https://spark.apache.org/docs/2.3.1/sql-programming-guide.html#jdbc-to-other-databases

**Example Includes:**

* Row()
* createDataFrame()
* write()
* jdbc()


```python
from pyspark.sql import Row

# dataframe api
connect = 'jdbc:postgresql://localhost:5433/my_database'
auth = {"user": my_username, "password": my_password}

# create row, set as dF, append dF to dF
new_row = [Row('Location6', '441 Rainy Road', 'Unit A', 'Albany', 'NY', '12202')]
appendDF = spark.createDataFrame(new_row, offices.schema[1:-1])
appendDF.write.jdbc(connect, 'offices', mode="append", properties = auth)

# spark sql api
# tbd ~ there may be a way
```


```python
offices.show()
```

    +---+---------+---------------+--------+------------+-----+-------+----------+
    | id|     name|        address|address2|        city|state|zipcode|date_added|
    +---+---------+---------------+--------+------------+-----+-------+----------+
    |  1|Location1| 123 Place Road|    null|    New York|   NY|  10001|2021-03-18|
    |  2|Location2|555 Rock Avenue|    null|      Boston|   MA|  02101|2021-03-18|
    |  3|Location3|838 Rock Avenue|    null|  Pittsburgh|   PA|  15106|2021-03-18|
    |  4|Location4| 321 Pop Street|    null|Philadelphia|   PA|  19019|2021-03-18|
    |  5|Location5| 210 Windy Road|    null| Minneapolis|   MN|  55111|2021-03-18|
    |  6|Location6| 441 Rainy Road|  Unit A|      Albany|   NY|  12202|2021-03-18|
    +---+---------+---------------+--------+------------+-----+-------+----------+
    


## 6. Missing Values (Null/NA)

**Example Includes:**

* filter()
* col()
* isNull()
* isnan()
* show()


```python
from pyspark.sql.functions import col, count, isnan

# dataframe api
offices.filter(col("name").isNull() | isnan(col("name"))).show()

# spark sql api
table_mv = spark.sql("""
                    select * 
                    from offices
                    where name is null
                    """
                    )
table_mv.show(50, False)
```

    +---+----+-------+--------+----+-----+-------+----------+
    | id|name|address|address2|city|state|zipcode|date_added|
    +---+----+-------+--------+----+-----+-------+----------+
    +---+----+-------+--------+----+-----+-------+----------+
    
    +---+----+-------+--------+----+-----+-------+----------+
    |id |name|address|address2|city|state|zipcode|date_added|
    +---+----+-------+--------+----+-----+-------+----------+
    +---+----+-------+--------+----+-----+-------+----------+
    


## 7. Cast & Alias

**Example Includes:**

* select()
* col()
* cast()
* alias()


```python
# dataframe api
offices.select(col('id').cast('string').alias('office_id')).show()

# spark sql api
table_select = spark.sql("""
                    select cast(id as string) as office_id
                    from offices 
                    """
                    )
table_select.show(50, False)
```

    +---------+
    |office_id|
    +---------+
    |        1|
    |        2|
    |        3|
    |        4|
    |        5|
    +---------+
    
    +---------+
    |office_id|
    +---------+
    |1        |
    |2        |
    |3        |
    |4        |
    |5        |
    +---------+
    


## Summary Statistics

Note: This isn't something particularly native to do with SQL. In postgres we can use things like crosstab, avg, or median to get something ismilar. Example here only includes usage for dataframe api.

**Example Includes**

* describe()
* show()


```python
# dataframe api
offices.describe().show()
```

    +-------+------------------+---------+---------------+--------+----------+-----+------------------+
    |summary|                id|     name|        address|address2|      city|state|           zipcode|
    +-------+------------------+---------+---------------+--------+----------+-----+------------------+
    |  count|                 5|        5|              5|       0|         5|    5|                 5|
    |   mean|               3.0|     null|           null|    null|      null| null|           20267.6|
    | stddev|1.5811388300841898|     null|           null|    null|      null| null|20479.819769714773|
    |    min|                 1|Location1| 123 Place Road|    null|    Boston|   MA|             02101|
    |    max|                 5|Location5|838 Rock Avenue|    null|Pittsburgh|   PA|             55111|
    +-------+------------------+---------+---------------+--------+----------+-----+------------------+
    

