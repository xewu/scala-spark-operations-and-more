val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
df.show()

// Aggregation & group 
// Single Column:
df.groupBy("department").sum("salary")
df.groupBy("department").count()
df.groupBy("department").min("salary")
df.groupBy("department").max("salary")
df.groupBy("department").avg("salary").show(false)
df.groupBy("department").mean("salary").show(false)

// multiple Columns:

df.groupBy("department","state")
    .sum("salary", "bonus")
    .show(false)
    
// Multiple aggregates at a time:
import org.apache.spark.sql.functions._
df.groupBy("department")
    .agg(
        sum("salary").alias("sum_salary"),
        avg("salary").alias("avg_salary"),
        sum("bonus").alias("sum_bonus"),
        sum("bonus").alias("max_bonus"))
    .where(col("sum_bonus") >= 50000)
    .show(false)

// Sort by Columns:
df.sort(col("department"), col("state")).show(false)
df.orderBy(col("department"), col("state")).show(false)

// default is .asc
df.orderBy(col("department").asc, col("state").asc).show(false)

// desc
df.orderBy(col("department").asc, col("state").desc).show(false)

// Inner Join:
empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner").show(false)
empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer").show(false)
empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full").show(false)
empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter").show(false)
empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left").show(false)
empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter").show(false)

// leftsemi: returns all columns from the left DataFrame/Dataset and ignores all columns from the right dataset. 
empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi").show(false)
// leftanti join returns only columns from the left DataFrame/Dataset for non-matched records
empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti").show(false)
// leftsemi: returns all columns from the left DataFrame/Dataset and ignores all columns from the right dataset. 
empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi").show(false)
// leftanti join returns only columns from the left DataFrame/Dataset for non-matched records
empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti").show(false)

// Union vs union distinct
val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000)
  )
val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

val simpleData2 = Seq(("James","Sales","NY",90000,34,10000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
val df2 = simpleData2.toDF("employee_name","department","state","salary","age","bonus")

// union:
val df3 = df.union(df2)
df3.count()
val df4 = df.union(df2).distinct()
df4.count()