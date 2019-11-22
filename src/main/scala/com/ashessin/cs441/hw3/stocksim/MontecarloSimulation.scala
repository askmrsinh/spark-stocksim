package com.ashessin.cs441.hw3.stocksim

import org.apache.commons.math3.distribution.AbstractRealDistribution
import org.apache.spark.mllib.random.RandomRDDs.uniformVectorRDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class MontecarloSimulation(private val spark: SparkSession)
  extends Serializable {

  import spark.implicits._

  def readStockFile(stocksDataFolderPath: String, stockSymbol: String): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy().orderBy("timestamp")
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
      .withColumn("change", $"close" - lag("close", 1).over(windowSpec))
      .withColumn("pct_change", $"change" / lag("close", 1).over(windowSpec))
      .withColumn("log_returns", log1p("pct_change"))
      .drop("open", "high", "low", "volume")
  }

  def calculateStockColumnStats(df: DataFrame, c: String): (Double, Double, Double, Double) = {
    val cVariance: Double = df.select(variance(c)).first().getDouble(0)
    val cStddev: Double = df.select(stddev(c)).first().getDouble(0)
    val cMean: Double = df.select(mean(df(c))).first().getDouble(0)
    val cDrift: Double = cMean - (0.5 * cVariance)
    (cVariance, cStddev, cMean, cDrift)
  }

  def formDailyReturnArrayDF(sparkSession: SparkSession,
                             timeIntervals: Int, iterations: Int, distribution: AbstractRealDistribution,
                             drift: Double, deviation: Double): DataFrame = {
    uniformVectorRDD(sparkSession.sparkContext, timeIntervals, iterations)
      .map(_.toArray.toIterable.map(
        x => calculateStockDailyReturn(distribution, drift, deviation, x)))
      .map(_.toArray).toDF("valueArray")
  }

  def calculateStockDailyReturn(distribution: AbstractRealDistribution,
                                drift: Double, deviation: Double, value: Double): Double = {
    Math.exp(drift + deviation * distribution.inverseCumulativeProbability(value))
  }

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

  def transormArrayDataframe(sparkSession: SparkSession,
                             arrayDatafeame: DataFrame, numberOfColumns: Int): DataFrame = {
    (0 until numberOfColumns).foldLeft(arrayDatafeame)((arrayDatafeame, num) =>
      arrayDatafeame.withColumn("c_" + (num + 1), $"valueArray".getItem(num))
    ).drop("valueArray")
  }

  def interpretSimulationResults(priceListDF: DataFrame): Unit = {
    val priceListDFSummary: util.List[Row] = priceListDF
      .summary("count", "mean", "stddev", "min", "5%", "50%", "95%", "max")
      .collectAsList()

    val startingPrice: Double = priceListDF.select("c_1").first().getDouble(0)
    val daysInvested: Int = priceListDFSummary.get(0).getString(1).toInt
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


    println(daysInvested, iterations)
    println(startingPrice)
    println(worstCase, averageCase, bestCase)
  }
}