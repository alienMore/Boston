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
  //val filenameOFFENSE = args(1)

  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)

  val crime = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    //.load("/home/admin/IdeaProjects/bostonsurin/data/crime.csv")
    .load(filenameCRIME)
  val offense_codes = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    //.load("/home/admin/IdeaProjects/bostonsurin/data/offense_codes.csv")
    .load(filenameOFFENSE_CODES)
  val MAINcrime = crime
    .groupBy( $"DISTRICT")
    .agg(avg("Lat") as "lat",
      avg("Long") as "lng",
      count("INCIDENT_NUMBER").name("crimes_total"))
    .createOrReplaceTempView("MAINcrime")

  crime.createOrReplaceTempView("month")
  spark.sql("SELECT DISTRICT,MONTH, count(MONTH) as PER_MONTH FROM month group by DISTRICT,MONTH")
     .createOrReplaceTempView("TMPmedian")
  spark.sql("SELECT DISTRICT as mDISTRICT,percentile_approx(PER_MONTH, 0.5) as crimes_monthly FROM TMPmedian group by DISTRICT order by DISTRICT")
     .createOrReplaceTempView("MAINmedian")

  spark.sql("SELECT * FROM MAINcrime LEFT JOIN MAINmedian ON MAINcrime.DISTRICT = MAINmedian.mDISTRICT")
    .createOrReplaceTempView("MAINresult")

  case class countName(DISTRICT: String, NAME: String, CODE: Int, count: Long)
  crime.join(broadcast(offense_codes), crime.col("OFFENSE_CODE") === offense_codes.col("CODE"), "left_outer")
    .select($"DISTRICT",regexp_replace($"NAME", ".-.*$","")as "NAME",$"CODE")
    .groupBy( "DISTRICT", "NAME", "CODE")
    .count()
    .orderBy('count.desc)
    .as[countName]
    .groupByKey(_.DISTRICT)
    .mapGroups{
      case (key, iter) => iter.toList.sortBy(x => -x.count).take(3)
    }
    .toDF("arr")
    .select(array_distinct($"arr.DISTRICT").getItem(0)as "crimeDISTRICT",array_join($"arr.NAME",", ")as "frequent_crime_type")
    .createOrReplaceTempView("MAINoffense")

  val parquet = spark.sql("SELECT MAINresult.DISTRICT," +
    "MAINresult.crimes_total," +
    "MAINresult.crimes_monthly," +
    "MAINoffense.frequent_crime_type," +
    "MAINresult.lat," +
    "MAINresult.lng FROM MAINresult LEFT JOIN MAINoffense ON MAINresult.DISTRICT = MAINoffense.crimeDISTRICT")
    .repartition(1)
    .write
    .format("parquet")
    .mode("append")
    //.save("/home/admin/Downloads/my.parquet")
    .save(pathToParquet)

}
