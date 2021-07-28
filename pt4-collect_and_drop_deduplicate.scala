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

// collect() : scala.Array[T] 
// collectAsList() : java.util.List[T]
val colList = countryDim.collectAsList()
val colData = countryDim.collect()

// Retrive data from Struct Column/:
colData.foreach(row=>
{
    val country_iso = row.getString(0)
    println(country_iso)
})

// nested structure:
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val data = Seq(
    Row(Row("James ","","Smith"),"36636","M",3000),
    Row(Row("Michael ","Rose",""),"40288","M",4000),
    Row(Row("Robert ","","Williams"),"42114","M",4000),
    Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

val schema = List(
    StructField("full_name", StructType(Array(
        StructField("first_name", StringType, true),
        StructField("middle_name", StringType, true),
        StructField("last_name", StringType, true)
        ))),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
    )

val df = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

val colList = df.collectAsList()
val colData = df.collect()

colData.foreach(row =>{
    val salary = row.getInt(3)
    val fullName: Row = row.getStruct(0)
    val firstName = fullName.getString(0)
    val middleName = fullName.get(1).toString
    val lastName = fullName.getAs[String]("last_name")
    println(firstName+","+middleName+","+lastName+","+salary)
})

// Distinct and dropDuplicate()
val distinctDF = countryDim.distinct()
val dropDisDF = countryDim.dropDuplicates("region","income_group")
dropDisDF.show(false)