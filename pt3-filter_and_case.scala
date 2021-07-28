// simple filters:
countryDim.filter(col("region") === "Europe & Central Asia").show(5, false)
countryDim.where(col("region") === "Europe & Central Asia").show(5, false)

// multiple filters
countryDim.filter(col("region") === "Europe & Central Asia" && col("income_group") === "High income").show(false)
countryDim.filter(countryDim("region")  === "Europe & Central Asia" && countryDim("income_group") === "High income").show(false)

// Contains
import org.apache.spark.sql.functions.array_contains
val tmp_df = countryDim.withColumn("country_region", concat(col("country_iso_code"), lit("-"), col("region")))
tmp_df.filter(array_contains(tmp_df("country_region"), "Asia")).show(5, false)

// Use "when otherwise":
val df2 = countryDim.withColumn("new_region", when(col("region") === "South Asia", "SA").when(col("region") === "Europe & Central Asia", "EU").otherwise("Others"))
df2.show(5, false)

// Use "case when":
val df3 = countryDim.withColumn("new_region",
    expr("case when region = 'South Asia' then 'SA' " + 
        "when region = 'Europe & Central Asia' then 'EU' " + 
        "else 'Others' end"))

// Use "&&" and "||" operator:
countryDim.withColumn("new_region",
    when(col("region") === "South Asia" || col("region") === "Europe & Central Asia", "SA_EU")
    .otherwise("Others"))
    .show()