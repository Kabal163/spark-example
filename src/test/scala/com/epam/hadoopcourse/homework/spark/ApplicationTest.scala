package com.epam.hadoopcourse.homework.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

class ApplicationTest
  extends FunSuite
    with SharedSparkContext  {

  test("Application.main") {
    val sourceFile = "target/test-classes/train.parquet"
    val targetFile = "target/test-classes/task1.json"
    val expectedFile = "target/test-classes/expected.json"
    val sparkMaster = "local"

    Application.main(Array(
      sourceFile,
      targetFile,
      sparkMaster))

    val sqlContext = new SQLContext(sc)

    val actualDF: DataFrame = sqlContext.read.json(targetFile)
    val expectedDF: DataFrame = sqlContext.read.json(expectedFile)

    assert(actualDF.count() == 3)
  }
}
