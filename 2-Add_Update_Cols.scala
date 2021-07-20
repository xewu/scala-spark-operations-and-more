// prepare data:
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

val seriesDim = series
    .select("Series Code", "Indicator Name", "Short Definition", "Periodicity", "Aggregation Method")
    .withColumnRenamed("Series Code", "indicator_code")
    .withColumnRenamed("Indicator Name", "indicator_name")
    .withColumnRenamed("Periodicity", "periodicity")
    .withColumnRenamed("Aggregation Method", "aggregation_method")
    .filter(series("periodicity") === "Annual")

val indicatorsData = indicators
    .withColumnRenamed("Indicator Code", "indicator_code")
    .withColumnRenamed("Country Code", "wb_country_code")
    .drop("Indicator Name")
    .drop("Country Name")
    .drop("_c62")

// 1. Add a New Column to DataFrame
countryDim.withColumn("Editor", lit("Some_Value")).show(5, false)

// 2. Change Value of an Existing Column
val tmp_df = countryDim.withColumn("int_value", lit(10))
tmp_df.withColumn("int_value", col("int_value")*12).show(5,false)

// 3. Derive New Column From an Existing Column
tmp_df.withColumn("new_int_value", col("int_value")* -1).show(5, false)

// 4. Change Data Type
tmp_df.withColumn("int_value", col("int_value").cast("String")).show(5,false)

// 5. Add, Replace, or Update multiple Columns: 
/**
 * when updating multiple columns, DO NOT use "withColumn()", it leads into performance issue
 * USE select after creating a temprary view on DataFrame
 */
countryDim.createOrReplaceTempView("COUNTRYDIM_TMP_VIEW")
val query = "SELECT 'erica' as editor, CONCAT(country_iso_code, '-', wb_country_code) as country_id FROM COUNTRYDIM_TMP_VIEW"
spark.sql(query).show(5, false)

// 6. Rename Column Name
countryDim.withColumnRenamed("country_iso_code", "country_iso_2char_code").show(5, false)

// 7. Drop a Column
countryDim.drop("wb_country_code").show(5, false)

// 8. Split Column into Multiple Columns
val newDF = countryDim.map(f => {
    val reginSplit = f.getAs[String](3).split(" ")
    val incomeGroupSplit = f.getAs[String](4).split(" ")
    (reginSplit(0), reginSplit(1) incomeGroupSplit(0))
}
    )
newDF.show(5, false)

val finalDf = newDF.toDF("region_i", "region_ii", "income")
finalDf.show(5, false)
