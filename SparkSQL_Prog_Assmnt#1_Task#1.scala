val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Read CSV data files
val dsports_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Sports_data.txt")

//What are the total number of gold medal winners every year
dsports_df.filter("medal_type == 'gold'").groupBy("year").count().show()

//How many silver medals have been won by USA in each sport
dsports_df.filter("medal_type == 'silver'").filter("country == 'USA'").groupBy("sports").count().show()
