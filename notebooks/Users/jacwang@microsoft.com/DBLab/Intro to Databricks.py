# Databricks notebook source
# MAGIC %md # Using Databricks
# MAGIC 
# MAGIC Dataframes are the standard data structure in Spark 2.0 and later. They offer a consistent way to work with data in any Spark-supported language (Python, Scala, R, Java, etc.), and also support the Spark SQL API so you can query and manipulate data using SQL syntax.
# MAGIC 
# MAGIC In this lab, exercise, you'll use Dataframes to explore some data from the United Kingdom Government Department for Transport that includes details of road traffic accidents in 2016 (you'll find more of this data and related documentation at https://data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data.)
# MAGIC 
# MAGIC ## Read a Dataframe from a File
# MAGIC After uploading the data files for this lab to your Azure storage account, adapt the code below to read the *Accidents.csv* file from your account into a Dataframe by replacing ***ACCOUNT_NAME*** with the name of your storage account:

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC help

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC unmount /mnt/spark/

# COMMAND ----------

textFile = spark.read.text('wasb://spark@sc1thornstg.blob.core.windows.net/data/Accidents.csv')
textFile.printSchema()

# COMMAND ----------

# MAGIC %md The file seems to contain comma-separated values, with the column header names in the first line.
# MAGIC 
# MAGIC You can use the **spark.read.csv** function to read a CSV file and infer the schema from its contents. Adapt the following code to use your storage account and run it to see the schema it infers:

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://spark@sc1thornstg.blob.core.windows.net/",
  mount_point = "/mnt/spark/",
  extra_configs = {"fs.azure.sas.spark.sc1thornstg.blob.core.windows.net": dbutils.secrets.get(scope = "akvsecrets", key = "blobsas")})

# COMMAND ----------

textFile = spark.read.text('/mnt/spark/data/accidents/Accidents.csv')
textFile.printSchema()

# COMMAND ----------

textFile.show(10, truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/spark/data/accidents

# COMMAND ----------

accidents = spark.read.csv('/mnt/spark/data/accidents/*.csv', header=True, inferSchema=True)
accidents.printSchema()

# COMMAND ----------

# MAGIC %md Now let's look at the first ten rows of data:

# COMMAND ----------

display(accidents)

# COMMAND ----------

print(accidents.count())

# COMMAND ----------

accidents.createOrReplaceTempView("tmp_accidents_vw")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select 
# MAGIC   Day_of_Week, 
# MAGIC   count(*) num_accidents
# MAGIC from tmp_accidents_vw
# MAGIC group by Day_of_Week

# COMMAND ----------

# MAGIC %md Inferring the schema makes it easy to read structured data files into a DataFrame containing multiple columns. However, it incurs a performance overhead; and in some cases you may want to have specific control over column names or data types. For example, use the following code to define the schema of the *Vehicles.csv* file:

# COMMAND ----------

from pyspark.sql.types import *

vehicle_schema = StructType([
  StructField("Accident_Index", StringType(), False),
  StructField("Vehicle_Reference", IntegerType(), False),
  StructField("Vehicle_Type", IntegerType(), False),
  StructField("Towing_and_Articulation", StringType(), False),
  StructField("Vehicle_Manoeuvre", IntegerType(), False),
  StructField("Vehicle_Location-Restricted_Lane", IntegerType(), False),
  StructField("Junction_Location", IntegerType(), False),
  StructField("Skidding_and_Overturning", IntegerType(), False),
  StructField("Hit_Object_in_Carriageway", IntegerType(), False),
  StructField("Vehicle_Leaving_Carriageway", IntegerType(), False),
  StructField("Hit_Object_off_Carriageway", IntegerType(), False),
  StructField("1st_Point_of_Impact", IntegerType(), False),
  StructField("Was_Vehicle_Left_Hand_Drive?", IntegerType(), False),
  StructField("Journey_Purpose_of_Driver", IntegerType(), False),
  StructField("Sex_of_Driver", IntegerType(), False),
  StructField("Age_of_Driver", IntegerType(), False),
  StructField("Age_Band_of_Driver", IntegerType(), False),
  StructField("Engine_Capacity_(CC)", IntegerType(), False),
  StructField("Propulsion_Code", IntegerType(), False),
  StructField("Age_of_Vehicle", IntegerType(), False),
  StructField("Driver_IMD_Decile", IntegerType(), False),
  StructField("Driver_Home_Area_Type", IntegerType(), False),
  StructField("Vehicle_IMD_Decile", IntegerType(), False)
])

print(vehicle_schema.simpleString())

# COMMAND ----------

# MAGIC %md Now you can use the **spark.read.csv** function with the **schema** argument to load the data from the file based on the schema you have defined.
# MAGIC 
# MAGIC Adapt the following code to read the *Vehicles.csv* file from your storage account and verify that it's schema matches the one you defined:

# COMMAND ----------

vehicles = spark.read.csv('/mnt/spark/data/Vehicles.csv', schema=vehicle_schema, header=True)


# COMMAND ----------

# MAGIC %md Once again, let's take a look at the first ten rows of data:

# COMMAND ----------

display(vehicles.head(10))

# COMMAND ----------

vehicles.createOrReplaceTempView("tmp_vehicles_vw")

# COMMAND ----------

# MAGIC %md ## Use DataFrame Methods
# MAGIC The Dataframe class provides numerous properties and methods that you can use to work with data.
# MAGIC 
# MAGIC For example, run the code in the following cell to use the **select** method. This creates a new dataframe that contains specific columns from an existing dataframe:

# COMMAND ----------

vehicle_driver = vehicles.select('Accident_Index', 'Vehicle_Reference', 'Vehicle_Type', 'Age_of_Vehicle', 'Sex_of_Driver' , 'Age_of_Driver' , 'Age_Band_of_Driver')
display(vehicle_driver)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Accident_Index, 
# MAGIC       Vehicle_Reference, 
# MAGIC       Vehicle_Type, 
# MAGIC       Age_of_Vehicle, 
# MAGIC       Sex_of_Driver , 
# MAGIC       Age_of_Driver , 
# MAGIC       Age_Band_of_Driver
# MAGIC from tmp_vehicles_vw

# COMMAND ----------

# MAGIC %md The **filter** method creates a new dataframe with rows that match a specified criteria removed from an existing dataframe:
# MAGIC > *Note: The code imports the **pyspark.sql.functions** library so we can use the **col** function to specify a particular column.*

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Accident_Index, 
# MAGIC       Vehicle_Reference, 
# MAGIC       Vehicle_Type, 
# MAGIC       Age_of_Vehicle, 
# MAGIC       Sex_of_Driver , 
# MAGIC       Age_of_Driver , 
# MAGIC       Age_Band_of_Driver
# MAGIC from tmp_vehicles_vw
# MAGIC where 
# MAGIC   Age_of_Vehicle != -1

# COMMAND ----------

# MAGIC %md You can chain multiple operations together into a single statement. For example, the following code uses the **select** method to define a subset of the **accidents** dataframe, and chains the output of that that to the **join** method, which creates a new dataframe by combining the columns from two dataframes based on a common key field:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC   av.*, 
# MAGIC   vv.*  
# MAGIC from tmp_accidents_vw av
# MAGIC join tmp_vehicles_vw vv
# MAGIC   on av.Accident_Index = vv.Accident_Index

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create temp view tmp_accident_vehicle_vw as
# MAGIC   select 
# MAGIC   av.*, 
# MAGIC   vv.*  
# MAGIC from tmp_accidents_vw av
# MAGIC join tmp_vehicles_vw vv
# MAGIC   on av.Accident_Index = vv.Accident_Index
# MAGIC   

# COMMAND ----------

# MAGIC %md ## Using the Spark SQL API
# MAGIC The Spark SQL API enables you to use SQL syntax to query dataframes that have been persisted as temporary or global tables.
# MAGIC For example, run the following cell to save the driver accident data as a temporary table, and then use the **spark.sql** function to query it using a SQL expression:

# COMMAND ----------

driver_accidents.createOrReplaceTempView('tmp_accidents')

q = spark.sql("SELECT * FROM tmp_accidents WHERE Speed_Limit > 50")
q.show()

# COMMAND ----------

# MAGIC %md When using a notebook to work with your data, you can use the **%sql** *magic* to embed SQL code directly into the notebook. For example, run the following cell to use a SQL query to filter, aggregate, and group accident data from the temporary table you created:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Vehicle_Type, Age_Band_of_Driver, COUNT(*) AS Accidents
# MAGIC FROM tmp_accident_vehicle_vw
# MAGIC WHERE 
# MAGIC   Vehicle_Type <> -1 
# MAGIC   and Speed_Limit > 50
# MAGIC GROUP BY Vehicle_Type, Age_Band_of_Driver
# MAGIC ORDER BY Vehicle_Type, Age_Band_of_Driver

# COMMAND ----------

# MAGIC %md Databricks notebooks include built-in data visualization tools that you can use to make sense of your query results. For example, perform the followng steps with the table of results returned by the query above to view the data as a bar chart:
# MAGIC 
# MAGIC View the resulting chart (you can resize it by dragging the handle at the bottom-right) and note that it clearly shows that the most accidents involve drivers in age band **6** and vehicle type **9**. The UK Department for Transport publishes a lookup table for these variables at http://data.dft.gov.uk/road-accidents-safety-data/Road-Accident-Safety-Data-Guide.xls, which indicates that these values correlate to drivers aged between *26* and *35* in *cars*.

# COMMAND ----------

# MAGIC %md Temporary tables are saved within the current session, which for interactive analytics can be a good way to explore the data and discard it automatically at the end of the session. If you want to to persist the data for future analysis, or to share with other data processing applications in different sessions, then you can save the dataframe as a global table.
# MAGIC 
# MAGIC Run the following cell to save the data as a global table and query it.

# COMMAND ----------

# MAGIC %md On the left of the screen, click the **Data** tab, and in the **default** database note that a table named **accidents** has been created.