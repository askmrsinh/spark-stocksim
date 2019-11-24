package com.ashessin.cs441.hw3.stocksim

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


object RunMontecarloSimulation {
  private val logger = LoggerFactory.getLogger("RunMontecarloSimulation")

  def main(args: Array[String]): Unit = {

    logger.info("Starting Montecarlo")
    // user's fault if they dont give a usable absolute path
    val stocksDataFolderPath: String = args(0)
    val stockSymbols: Array[String] = args(1).toUpperCase.split(",")
    val (timeIntervals, iterations) = (args(2).toInt, args(3).toInt)
    val availableFunds = args(4).toDouble

    // NOTE: Zeppelin creates and injects sc (SparkContext) and sqlContext (HiveContext or SqlContext)
    //       So you don't need create them manually

    // set up environment
    val spark: SparkSession = SparkSession.builder
      .appName("Montecarlo")
      .config("spark.master", "local")
      .getOrCreate()
      .newSession()

    var priceListDFs: scala.collection.mutable.Map[String, DataFrame] =
      scala.collection.mutable.Map[String, DataFrame]()

    stockSymbols.par.foreach(stockSymbol => {
      logger.info(f"Processing stock data for $stockSymbol symbol.")
      val simulation = new MontecarloSimulation(spark, stocksDataFolderPath, stockSymbol)
      val stockDF: DataFrame = simulation.readStockFile

      if (logger.isDebugEnabled()) {
        stockDF.createOrReplaceTempView("stockDF")
        stockDF.printSchema()
        stockDF.show(5)
      }

      val (variance, deviation, mean, drift) =
        simulation.calculateStockColumnStats(stockDF, "log_returns")
      logger.info("{} stock returns variance: {}", stockSymbol, variance)
      logger.info("{} stock returns deviation: {}", stockSymbol, deviation)
      logger.info("{} stock returns mean: {}", stockSymbol, mean)
      logger.info("{} stock returns drift: {}", stockSymbol, drift)

      val dailyReturnArrayDF: DataFrame = simulation.formDailyReturnArrayDF(spark,
        timeIntervals, iterations, new NormalDistribution(0, 1), drift, deviation
      )
      val priceListArrayDF: DataFrame = simulation.formPriceListsArrayDF(spark,
        stockDF, timeIntervals, iterations, dailyReturnArrayDF
      )

      val priceListDF: DataFrame = simulation.transormArrayDataframe(spark,
        priceListArrayDF, iterations)
      simulation.summarizeSimulationResult(stockSymbol, priceListDF)

      priceListDFs += (stockSymbol -> priceListDF)
    })

    val broker = new InvestmentBroker(spark, priceListDFs, availableFunds)
    broker.dummyStrategy()
  }
}