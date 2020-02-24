package com.github.mrpowers.my.cool.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object Boston extends App {
  val spark = SparkSession
    .builder()
    .master(master = "local[*]")
    .getOrCreate()
  val filenameCRIME = args(0)
  val filenameOFFENSE_CODES = args(1)
  val pathToParquet = args(2)

  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)

  val crime = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    ///.load("/home/admin/IdeaProjects/bostonsurin/data/crime.csv")
    .load(filenameCRIME)
  val offense_codes = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    ///.load("/home/admin/IdeaProjects/bostonsurin/data/offense_codes.csv")
    .load(filenameOFFENSE_CODES)
  crime
    .groupBy( $"DISTRICT",$"YEAR",$"MONTH")
    .count()
    .sort($"count")
    .createOrReplaceTempView("TMPmedian")

  //Get median
  spark.sql("SELECT DISTRICT as DISTRICT,percentile_approx(count, 0.5) as crimes_monthly FROM TMPmedian group by DISTRICT order by DISTRICT")
    .na.fill("UnknownDistrict")
  .createOrReplaceTempView("median")

  crime
    .groupBy($"DISTRICT")
    .agg(count("INCIDENT_NUMBER").name("crimes_total"),
         avg("Lat") as "lat",
         avg("Long") as "lng")
    .na.fill("UnknownDistrict")
    .createOrReplaceTempView("MAINresult")

  //Get crime_type stage 1
  crime.join(broadcast(offense_codes.dropDuplicates("CODE")), crime.col("OFFENSE_CODE") === offense_codes.col("CODE"), "left_outer")
    .na.fill("UnknownDistrict")
    .groupBy( $"DISTRICT",$"NAME")
    .count()
    .sort('DISTRICT,'count.desc)
  .createOrReplaceTempView("crime_type")
  //Get crime_type stage 2
  spark.sql("select * from (" +
      "SELECT DISTRICT, NAME, count, " +
      "row_number() OVER (partition by DISTRICT order by count desc) as country_rank " +
      "FROM crime_type) ranks " +
    "where country_rank <= 3")
     .groupBy($"DISTRICT").agg(array_join(collect_list(regexp_replace($"NAME", ".-.*$","")as "NAME"),", ")as "frequent_crime_types")
    //.show(50,false)
    .createOrReplaceTempView("frequent_crime_type")

  spark.sql("SELECT a.DISTRICT, " +
    "a.crimes_total, " +
    "b.crimes_monthly, " +
    "c.frequent_crime_types, " +
    "a.lat, " +
    "a.lng FROM MAINresult a " +
    "LEFT JOIN median b ON a.DISTRICT = b.DISTRICT " +
    "LEFT JOIN frequent_crime_type c ON a.DISTRICT = c.DISTRICT")
    //.show(false)
    .repartition(1)
    .write
    .format("parquet")
    .mode("append")
    //.save("/home/admin/Downloads/my.parquet")
    .save(pathToParquet)
}
