package com.epam.hadoopcourse.homework.spark

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Application {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("spark-task1")

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.parquet(args(0))
    df.registerTempTable("train")

    val result = sqlContext.sql(query)

    result.write.json(args(1))
  }

  val query = "SELECT hotel_continent,\n" +
    "                 hotel_country,\n" +
    "                 hotel_market,\n" +
    "                 COUNT(*)\n" +
    "            FROM train\n" +
    "           WHERE srch_adults_cnt = 2\n" +
    "           GROUP BY hotel_continent,\n" +
    "                    hotel_country,\n" +
    "                    hotel_market\n" +
    "           ORDER BY COUNT(*) DESC\n" +
    "           LIMIT 3\n"


  case class TrainModel(date_time: String,
                        site_name: Int,
                        posa_continent: Int,
                        user_location_country: Int,
                        user_location_region: Int,
                        user_location_city: Int,
                        orig_destination_distance: Option[Double],
                        user_id: Long,
                        is_mobile: Option[Boolean],
                        is_package: Option[Boolean],
                        channel: Option[Int],
                        srch_ci: Option[String],
                        srch_co: Option[String],
                        srch_adults_cnt: Option[Int],
                        srch_children_cnt: Option[Int],
                        srch_rm_cnt: Option[Int],
                        srch_destination_id: Option[Long],
                        srch_destination_type_id: Option[Long],
                        is_booking: Option[Boolean],
                        cnt: Option[Int],
                        hotel_continent: Option[Int],
                        hotel_country: Option[Int],
                        hotel_market: Option[Int],
                        hotel_cluster: Option[Int])

  case class ResultOfTask1(hotel_continent: Option[Int],
                           hotel_country: Option[Int],
                           hotel_market: Option[Int],
                           count: Long)
}
