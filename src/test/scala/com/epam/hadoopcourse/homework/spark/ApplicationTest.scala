package com.epam.hadoopcourse.homework.spark

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class ApplicationTest
    extends FunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  test("Application.main") {

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sourceFile = "target/scala-2.11/test-classes/train.parquet"
    val targetFile = "target/scala-2.11/test-classes/task1.json"
    val expectedFile = "target/scala-2.11/test-classes/expected.json"
    val sparkMaster = "local"

    Application.main(Array(
      sourceFile,
      targetFile,
      sparkMaster))

    val actualDF: DataFrame = spark.read.json(targetFile)
    val expectedDF: DataFrame = spark.read.json(expectedFile)

    assertSmallDataFrameEquality(actualDF, expectedDF)
  }
}
