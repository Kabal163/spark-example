package com.epam.hadoopcourse.homework.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .getOrCreate()
  }
}
