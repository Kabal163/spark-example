package com.epam.hadoopcourse.homework.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("spark-task1")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val ds: Dataset[TrainModel] = spark.read.parquet(args(0)).as[TrainModel]

    val result = ds
      .filter(_.srch_adults_cnt.get == 2)
      .groupByKey(train => (train.hotel_continent, train.hotel_country, train.hotel_market))
      .count()
      .sort($"count(1)".desc)
      .map(train => ResultOfTask1(train._1._1, train._1._2, train._1._3, train._2))
      .take(3)

    spark.createDataset(result).write.json(args(1))
  }


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
                           cnt: Long)
}
