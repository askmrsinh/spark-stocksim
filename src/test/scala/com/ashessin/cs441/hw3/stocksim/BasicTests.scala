package com.ashessin.cs441.hw3.stocksim

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class BasicTests extends FunSuite {

  val spark: SparkSession = SparkSession.builder
    .appName("Montecarlo")
    .config("spark.master", "local")
    .getOrCreate()
    .newSession()
  val companylist: Path = new Path(Thread.currentThread.getContextClassLoader
    .getResource("data/companylist.csv").getFile)
  var simulation: MontecarloSimulation = _
  var msftDF: DataFrame = _
  var zscore: Double = _

  test("Companylist file must be present and readable") {
    val fileSystem = companylist.getFileSystem(new Configuration)
    assert(fileSystem.exists(companylist))
  }

  test("Stock dataframe must be defined") {
    simulation = new MontecarloSimulation(spark,
      Thread.currentThread.getContextClassLoader.getResource("data/").getPath,
      "MSFT"
    )
    msftDF = simulation.readStockFile
    assert(msftDF.columns.length > 0)
  }

  test("Validate stock stats") {
    val (variance, deviation, mean, drift) = simulation.calculateStockColumnStats(msftDF, "log_returns")
    assert(variance == 4.511899231076514E-4)
    assert(deviation == 0.021241231675862192)
    assert(mean == 1.0144483073271711E-4)
    assert(drift == -1.241501308211086E-4)
  }

  test("Validate z-score calculation") {
    zscore = simulation.calculateStockDailyReturn(new NormalDistribution(0, 1),
      7.563548659245546E-5,
      0.0327378789637775,
      0.00626383329827962
    )
    assert(zscore == 0.9215776135302319)
  }

  test("Check array size") {
    val iterations = 100
    val dailyReturnArrayDF: DataFrame = simulation.formDailyReturnArrayDF(spark,
      10, iterations,
      new NormalDistribution(0, 1), 7.563548659245546E-5, 0.0327378789637775)
    assert(dailyReturnArrayDF.first().getSeq(0).size == iterations)
  }
}
