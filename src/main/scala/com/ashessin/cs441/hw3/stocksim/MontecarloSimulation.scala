package com.ashessin.cs441.hw3.stocksim

import java.util

import org.apache.commons.math3.distribution.AbstractRealDistribution
import org.apache.spark.mllib.random.RandomRDDs.uniformVectorRDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * Runs Monte Carlo Simulation on stock data obtained from CSV file.
 *
 * @param spark the spark session in use
 */
class MontecarloSimulation(private val spark: SparkSession)
  extends Serializable {

  import spark.implicits._

  /**
   * Reads CSV file for a stock and forms a Dataframe with computed columns.
   *
   * @param stocksDataFolderPath an absolute path to stock CSV data folder
   * @param stockSymbol          some valid NASDAQ stock symbol
   * @return a DataFrame with timestamp, close, change, pct_change, log_returns columns.
   */
  def readStockFile(stocksDataFolderPath: String, stockSymbol: String): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy().orderBy(asc("timestamp"))
    spark.read.format("csv")
      .option("sep", ",").option("header", "true")
      .schema(StructType {
        Array(
          StructField("timestamp", DateType, nullable = false),
          StructField("open", FloatType, nullable = true),
          StructField("high", FloatType, nullable = true),
          StructField("low", FloatType, nullable = true),
          StructField("close", FloatType, nullable = true),
          StructField("volume", IntegerType, nullable = true))
      })
      .load(stocksDataFolderPath + "daily_" + stockSymbol + ".csv")
      // calculate day to day change in closing stock price
      .withColumn("change", $"close" - lag("close", 1).over(windowSpec))
      // calculate day to day percentage change in closing stock price
      .withColumn("pct_change", $"change" / lag("close", 1).over(windowSpec))
      // calculate natural logarithm of percentage change in closing stock price plus one
      .withColumn("log_returns", log1p("pct_change"))
      // drop columns that are not required in this simulation
      .drop("open", "high", "low", "volume")
      .orderBy(asc("timestamp"))
  }

  /**
   * Calculates statistics for a column in a stock Dataframe.
   *
   * @param stockDF    a DataFrame with stock data
   * @param columnName some column in the stock DataFrame
   * @return calculated variance, standard deviation, mean and drift values
   */
  def calculateStockColumnStats(stockDF: DataFrame, columnName: String): (Double, Double, Double, Double) = {
    val cVariance: Double = stockDF.select(variance(columnName)).first().getDouble(0)
    val cStddev: Double = stockDF.select(stddev(columnName)).first().getDouble(0)
    val cMean: Double = stockDF.select(mean(stockDF(columnName))).first().getDouble(0)
    val cDrift: Double = cMean - (0.5 * cVariance)
    (cVariance, cStddev, cMean, cDrift)
  }

  /**
   * Estimates the expected return over a period of time based on previous stock data.
   *
   * @param sparkSession  spark session in use
   * @param timeIntervals number of days
   * @param iterations    number of random value initializations
   * @param distribution  probability distribution to use
   * @param drift         calculated drift for a stock
   * @param deviation     calculated standard deviation for a stock
   * @return a DataFrame with estimated having daily return percentages
   */
  def formDailyReturnArrayDF(sparkSession: SparkSession,
                             timeIntervals: Int, iterations: Int, distribution: AbstractRealDistribution,
                             drift: Double, deviation: Double): DataFrame = {
    uniformVectorRDD(sparkSession.sparkContext, timeIntervals, iterations)
      .map(_.toArray.toIterable.map(
        x => calculateStockDailyReturn(distribution, drift, deviation, x)))
      .map(_.toArray).toDF("valueArray")
  }

  /**
   * Calculates the expected daily return percent based on stock's deviation, drift.
   *
   * @param distribution probability distribution to use.
   * @param drift        calculated drift for the stock
   * @param deviation    calculated standard deviation for the stock
   * @param value        some random decimal number
   * @return calculated daily return for a given value
   */
  def calculateStockDailyReturn(distribution: AbstractRealDistribution,
                                drift: Double, deviation: Double, value: Double): Double = {
    Math.exp(drift + deviation * distribution.inverseCumulativeProbability(value))
  }

  /**
   * Calculates the expected daily return value based on stock's last known closing price
   *
   * @param sparkSession       spark session in use
   * @param stockDF            a DataFrame with stock data
   * @param timeIntervals      number of days
   * @param iterations         number of random value initializations
   * @param dailyReturnArrayDF a DataFrame with estimated daily return percentages
   * @return a DataFrame with estimated daily stock prices
   */
  def formPriceListsArrayDF(sparkSession: SparkSession,
                            stockDF: DataFrame, timeIntervals: Int, iterations: Int,
                            dailyReturnArrayDF: DataFrame): DataFrame = {
    val lastPrice: Float = stockDF.select("close").collect()(stockDF.count().toInt - 1).getFloat(0)
    var priceList = new ListBuffer[List[Double]]()
    for (id <- 0 until timeIntervals) {
      if (id == 0) {
        priceList += List.fill(iterations)(lastPrice)
      } else {
        val x = priceList(id - 1)
        val y = dailyReturnArrayDF.select("valueArray").collect()(id - 1).getSeq[Double](0)
        priceList += x.zip(y).map { case (x, y) => x * y }
      }
    }
    priceList.toDF("valueArray")
  }

  /**
   * Explodes a single valueArray column to multiple columns in a DataFrame.
   *
   * @param sparkSession    spark session in use
   * @param arrayDataFrame  a DataFrame with arrays containing daily return percentages
   * @param numberOfColumns size of the estimates for a day
   * @return a DataFrame with numberOfColumns columns
   */
  def transormArrayDataframe(sparkSession: SparkSession,
                             arrayDataFrame: DataFrame, numberOfColumns: Int): DataFrame = {
    (0 until numberOfColumns).foldLeft(arrayDataFrame)((arrayDataFrame, num) =>
      arrayDataFrame.withColumn("c_" + (num + 1), $"valueArray".getItem(num))
    ).drop("valueArray")
  }

  /**
   * Prints a summary of Simulation results on a single stock.
   *
   * @param priceListDF a DataFrame with estimated daily stock prices
   */
  def summarizeSimulationResult(priceListDF: DataFrame): Unit = {
    val priceListDFSummary: util.List[Row] = priceListDF
      .summary("count", "mean", "stddev", "min", "5%", "50%", "95%", "max")
      .collectAsList()

    val startingPrice: Double = priceListDF.select("c_1").first().getDouble(0)
    val simulationLength: Int = priceListDFSummary.get(0).getString(1).toInt
    val iterations: Int = priceListDF.columns.length

    // Loss/profit @ 5%
    val worstCase: Double = priceListDFSummary.get(4).toSeq.drop(1).map(_.toString.toDouble).min
    val worstCasePercentage = (worstCase - startingPrice) * 100 / startingPrice
    // Loss/profit @ 50%
    val averageCase: Double = priceListDFSummary.get(5).toSeq.drop(1).map(_.toString.toDouble).min
    val averageCasePercentage = (averageCase - startingPrice) * 100 / startingPrice
    // Loss/profit @ 95%
    val bestCase: Double = priceListDFSummary.get(6).toSeq.drop(1).map(_.toString.toDouble).max
    val bestCasePercentage = (bestCase - startingPrice) * 100 / startingPrice

    println(f"Length of Simulation: $simulationLength")
    println(f"Number of iterations: $iterations")
    println(f"Starting Price: $startingPrice%8.2f")

    println(f"Estimated Current Market Price (worst):" +
      f"\t$worstCase%8.2f," +
      f"\t$worstCasePercentage%8.2f")
    println(f"Estimated Current Market Price (average):" +
      f"\t$averageCase%8.2f," +
      f"\t$averageCasePercentage%8.2f")
    println(f"Estimated Current Market Price (best):" +
      f"\t$bestCase%8.2f," +
      f"\t$bestCasePercentage%8.2f")
  }
}