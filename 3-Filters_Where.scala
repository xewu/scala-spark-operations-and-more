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