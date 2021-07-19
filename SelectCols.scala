// read data:
val country = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("s3://erica-experiment-spark/data/WDICountry.csv")
val indicators = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("s3://erica-experiment-spark/data/WDIData.csv")
val series = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("s3://erica-experiment-spark/data/WDISeries.csv")

val countryDim = country
    .select("2-alpha code", "Country Code", "Short Name", "Long Name", "Region", "Income Group")
    .withColumnRenamed("2-alpha code", "country_iso_code")
    .withColumnRenamed("Country Code", "wb_country_code")
    .withColumnRenamed("Short Name", "country_name")
    .withColumnRenamed("Long Name", "country_long_name")
    .withColumnRenamed("Region", "region")
    .withColumnRenamed("Income Group", "income_group")
    
countryDim.show(5, false)

val seriesDim = series
    .select("Series Code", "Indicator Name", "Short Definition", "Periodicity", "Aggregation Method")
    .withColumnRenamed("Series Code", "indicator_code")
    .withColumnRenamed("Indicator Name", "indicator_name")
    .withColumnRenamed("Periodicity", "periodicity")
    .withColumnRenamed("Aggregation Method", "aggregation_method")
    .filter(series("periodicity") === "Annual")

seriesDim.show(5, false)

val indicatorsData = indicators
    .withColumnRenamed("Indicator Code", "indicator_code")
    .withColumnRenamed("Country Code", "wb_country_code")
    .drop("Indicator Name")
    .drop("Country Name")
    .drop("_c62")
    
indicatorsData.show(5, false)

/**
 * 1 Select Columns
 */
countryDim.select(countryDim("country_iso_code").alias("iso_code")).show(5,false)
countryDim.select(col("country_iso_code").alias("iso_code"), col("country_name").alias("country")).show(5,false)

/**
 * 2 Select All Columns
 */
countryDim.select("*")
	// Array[org.apache.spark.sql.Column]
val columnsAll = countryDim.columns.map(m => col(m))
countryDim.select(columnsAll:_*)

	// Array[String] - List[String]
val columns = countryDim.columns
countryDim.select(columns.map(m => col(m)): _*)

/**
 * 3. Select columns from list
 */
val listCols = List("country_iso_code", "wb_country_code", "country_name", "country_long_name")
countryDim.select(listCols.map(m => col(m)): _*).show(5, false)

 /**
  * 4. Select First N Columns
  */
countryDim.select(countryDim.columns.slice(0, 3).map(m => col(m)): _*).show(5,false)

/**
 * 5. Select Column By Position or Index
 */
countryDim.select(countryDim.columns(3)).show(5, false)

/**
* 6. Select Columns by Regular expression
*/
countryDim.select(countryDim.colRegex("`^.*name*`")).show(5, false)

 /**
  * 7. Select Columns Starts or Ends With
  */
  countryDim.select(countryDim.columns.filter(f => f.startsWith("country")).map(m => col(m)):_*).show(5, false)
  countryDim.select(countryDim.columns.filter(f => f.endsWith("name")).map(m => col(m)):_*).show(5, false)

  /**
  * 8. Select Nested Struct Columns - StructType
  */










