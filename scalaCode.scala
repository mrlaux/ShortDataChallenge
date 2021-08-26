
import org.apache.spark.sql.types.{LongType, 
	IntegerType,
	StringType,
	DateType, 
	StructType,
	StructField}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.regexp_replace

// set dataLocation to the csv location
val dataLocation = "c:/Users/mike_/Documents/Professional/2021/03.Transition/Ventera/US_COVID_SHORT_SAMPLE _ Data Challenge.csv"

val schema = StructType(Array(
  StructField("SUBMISSION_DATE", StringType, true),
  StructField("STATE", StringType, true),
  StructField("TOTAL_CASES", LongType, true),
  StructField("NEW_CASE", LongType, true),
  StructField("TOTAL_DEATHS", LongType, true),
  StructField("NEW_DEATH", LongType, true)
))

val df = spark.read.option("inferSchema", true).option("header", true).csv(dataLocation)
val df2 = df.withColumn("TOTAL_CASES_NUM", 
	regexp_replace(col("TOTAL_CASES"), ",", "").cast("int")).withColumn("NEW_CASE_NUM",
	regexp_replace(col("NEW_CASE"), ",", "").cast("int")).withColumn("TOTAL_DEATHS_NUM",
	regexp_replace(col("TOTAL_DEATHS"), ",", "").cast("int")).withColumn("NEW_DEATH_NUM",
	regexp_replace(col("NEW_DEATH"), ",", "").cast("int"))
		
val dfOutput = df2.withColumn("SUBMISSION_DT", to_date($"SUBMISSION_DATE", "M/d/yyyy")).withColumn(
	"COVID_CASES_RATE",
  when(df2("NEW_CASE_NUM") > 50, "HIGH")
  .when(df2("NEW_CASE_NUM") > 20, "MEDIUM")
  .otherwise("LOW")).withColumn("COVID_DEATHS_RATE",
	when(df2("NEW_DEATH_NUM") > 10, "HIGH")
	.when(df2("NEW_DEATH_NUM") > 5, "MEDIUM")
	.otherwise("LOW"))
dfOutput.repartition(1)
  .write.option("header", true).mode("overwrite").csv("c:/Users/mike_/Documents/Professional/2021/03.Transition/Ventera/US_COVID_SHORT_SAMPLE_Data_Challenge_Output.csv")

dfOutput.createOrReplaceTempView("COVID")

print("Total cases and deaths in USA")
spark.sql(
  """
	|SELECT SUM(NEW_CASE_NUM) AS TOTAL_CASES, SUM(NEW_DEATH_NUM) AS TOTAL_DEATHS
	|FROM COVID
	|""".stripMargin).show

print("Top 5 states with high COVID Cases")
spark.sql(
  """WITH CTE AS (
	|SELECT STATE, SUM(NEW_CASE_NUM) AS TOTAL_CASES
	|FROM COVID
	|GROUP BY STATE
	|)
	|SELECT STATE
	|FROM CTE
	|ORDER BY TOTAL_CASES DESC
	|LIMIT 5
	|""".stripMargin).show


print("Total cases and death in California as of 5/20/2021")
spark.sql(
  """SELECT TOTAL_CASES_NUM 
	|FROM COVID 
	|WHERE SUBMISSION_DATE = '5/20/2021'
	|AND  STATE = 'CA'
	|""".stripMargin).show

print("When percent increase of Covid cases is more than 20% compared to previous submission in California")	
spark.sql("""	
SELECT  
C1.STATE, C1.SUBMISSION_DT
FROM
(
SELECT C.STATE, MAX(C2.SUBMISSION_DT) AS PRIOR_DT, C.SUBMISSION_DT AS FIRST_DT
FROM COVID C
INNER JOIN COVID C2
ON C.STATE = C2.STATE
AND C.SUBMISSION_DT > C2.SUBMISSION_DATE
GROUP BY C.STATE, C.SUBMISSION_DT
) FS
INNER JOIN COVID C1
ON C1.STATE = FS.STATE AND C1.SUBMISSION_DT = FS.FIRST_DT
INNER JOIN COVID C2
ON C2.STATE = FS.STATE AND C2.SUBMISSION_DT = FS.PRIOR_DT
WHERE (C1.NEW_CASE_NUM - C2.NEW_CASE_NUM) / C2.NEW_CASE_NUM > 0.2
AND C1.STATE = 'CA'
""").show

print("Which is safest state comparatively")
print("Interpretaton:  safest state has the lowest death rate")
spark.sql("""
SELECT STATE
FROM
(SELECT STATE, MAX(TOTAL_CASES_NUM) AS TOTAL_CASES, MAX(TOTAL_DEATHS_NUM) AS TOTAL_DEATHS
FROM COVID
GROUP BY STATE) SM
ORDER BY TOTAL_DEATHS / TOTAL_CASES ASC
LIMIT 1
""").show