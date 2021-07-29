val data = Seq(
    ("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

val df = data.toDF("Product", "Amount", "Country")
df.show(false)

// Pivot:
val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show()

val countries = Seq("USA", "China", "Canada", "Mexico")
val pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
pivotDF.show()

// Pivot-performance:
val pivotDF = df.groupBy("Product", "Country")
    .sum("Amount")
    .groupBy("Product")
    .pivot("Country")
    .sum("sum(Amount)")

pivotDF.show()

//unpivot
val unPivotDF = pivotDF.select($"Product",
expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
.where("Total is not null")
unPivotDF.show()

// DataType:
// org.apache.spark.sql.types.DataType._
// StringType; ArrayType; MapType; StructType; DateType, TimestampType; BooleanType; CalendarIntervalType; BinaryType; NumericType;
// ShortType; IntegerType; LongType; FloatType; DoubleType; DecimalType; ByteType; HiveStringType; ObjectType; NullType;

// 1.1-DataType:
import org.apache.spark.sql.types._
val arrayType = ArrayType(IntegerType,false)
 println("json() : "+arrayType.json)  // Represents json string of datatype
 println("prettyJson() : "+arrayType.prettyJson) // Gets json in pretty format
 println("simpleString() : "+arrayType.simpleString) // simple string
 println("sql() : "+arrayType.sql) // SQL format
 println("typeName() : "+arrayType.typeName) // type name
 println("catalogString() : "+arrayType.catalogString) // catalog string
 println("defaultSize() : "+arrayType.defaultSize) // default size
 

 // DataType.fromJson()
val typeFromJson = DataType.fromJson(
    """{"type":"array",
      |"elementType":"string","containsNull":false}""".stripMargin)
println(typeFromJson.getClass)

val typeFromJson2 = DataType.fromJson("\"string\"")
println(typeFromJson2.getClass)

// StructType and StructField:

// case class StructType(fields: Array[StructField])

// case class StructField(
//     name: String,
//     dataType: DataType,
//     nullable: Boolean = true,
//     metadata: Metadata = Metadata.empty)

import org.apache.spark.sql.Row
val simpleData = Seq(
    Row("James ","","Smith","36636","M",3000),
    Row("Michael ","Rose","","40288","M",4000),
    Row("Robert ","","Williams","42114","M",4000),
    Row("Maria ","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1))

val simpleSchema = StructType(Array(
    StructField("firstname", StringType, true),
    StructField("middlename", StringType, true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
    ))

val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), simpleSchema)
df.printSchema()
df.show()

// Nested Schema:
val structureData = Seq(
    Row(Row("James ","","Smith"),"36636","M",3100),
    Row(Row("Michael ","Rose",""),"40288","M",4300),
    Row(Row("Robert ","","Williams"),"42114","M",1400),
    Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )
 
val structureSchema = StructType(Array(
    StructField("name", StructType(Array(
        StructField("firstname", StringType),
        StructField("middlename", StringType),
        StructField("lastname", StringType))), true),
    StructField("id", StringType),
    StructField("gender", StringType),
    StructField("salary", IntegerType)
))
val df2 = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
df2.printSchema()
df2.show()