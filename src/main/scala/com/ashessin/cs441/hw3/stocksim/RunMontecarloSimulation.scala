package com.ashessin.cs441.hw3.stocksim

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object RunMontecarloSimulation {
  private val logger = LoggerFactory.getLogger("Montecarlo")

  def main(args: Array[String]): Unit = {

    logger.info("Starting Montecarlo")

    // Zeppelin creates and injects sc (SparkContext) and sqlContext (HiveContext or SqlContext)
    // So you don't need create them manually

    // set up environment
    val spark: SparkSession = SparkSession.builder
      .appName("Montecarlo")
      .config("spark.master", "local")
      .getOrCreate()
    val simulation = new MontecarloSimulation(spark)

    val stockSymbol: String = "MSFT"
    val stockSchema: StructType = simulation.createStockSchema(stockSymbol)

    val stockDF: DataFrame = simulation.readStockFile(stockSymbol, stockSchema)
    stockDF.createOrReplaceTempView("stockDF")
    if (logger.isDebugEnabled()) {
      stockDF.printSchema()
      stockDF.show(5)
    }

    val (variance, deviation, mean, drift) =
      simulation.calculateStockColumnStats(stockDF, "log_returns")
    logger.info("{} stock returns variance: {}", stockSymbol, variance)
    logger.info("{} stock returns deviation: {}", stockSymbol, deviation)
    logger.info("{} stock returns mean: {}", stockSymbol, mean)
    logger.info("{} stock returns drift: {}", stockSymbol, drift)

    val timeIntervals = 28
    val iterations = 100

    val normalDistribution: NormalDistribution = new NormalDistribution(0, 1)

    val dailyReturnArrayDF: DataFrame = simulation.formDailyReturnArrayDF(
      spark,
      timeIntervals,
      iterations,
      normalDistribution,
      drift,
      deviation
    )
    if (logger.isDebugEnabled()) {
      dailyReturnArrayDF.printSchema()
      dailyReturnArrayDF.show(5)
    }

    val priceListArrayDF: DataFrame = simulation.formPriceListsArrayDF(
      spark,
      stockSymbol,
      stockDF,
      timeIntervals,
      iterations,
      dailyReturnArrayDF
    )

    val priceListDF: DataFrame =
      simulation.transormArrayDataframe(spark, priceListArrayDF, iterations)
    if (logger.isDebugEnabled()) {
      priceListDF.printSchema()
      priceListDF.show(5)
    }
    priceListDF.createOrReplaceTempView("priceListDF")
    priceListDF.describe().show()
  }
}