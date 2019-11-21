package com.ashessin.cs441.hw3.stocksim

import org.apache.commons.math3.distribution.AbstractRealDistribution
import org.apache.spark.mllib.random.RandomRDDs.uniformVectorRDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class MontecarloSimulation(private val spark: SparkSession)
  extends Serializable {

  import spark.implicits._

  def createStockSchema(symbol: String): StructType = {
    StructType {
      Array(
        StructField("Date", DateType, nullable = false),
        StructField(symbol.toUpperCase, FloatType, nullable = true))
    }
  }

  def readStockFile(stockSymbol: String, stockSchema: StructType): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy().orderBy("Date")
    val dataColumn: Column = new Column(stockSymbol)
    spark.read.format("csv").schema(stockSchema).option("sep", ",").option("header", "true")
      .load("MSFT_2000.csv")
      .withColumn("change", dataColumn - lag(dataColumn, 1).over(windowSpec))
      .withColumn("pct_change", $"change" / lag(dataColumn, 1).over(windowSpec))
      .withColumn("log_returns", log1p("pct_change"))
  }

  def calculateStockColumnStats(df: DataFrame, c: String): (Double, Double, Double, Double) = {
    val cVariance: Double = df.select(variance(c)).first().getDouble(0)
    val cStddev: Double = df.select(stddev(c)).first().getDouble(0)
    val cMean: Double = df.select(mean(df(c))).first().getDouble(0)
    val cDrift: Double = cMean - (0.5 * cVariance)
    (cVariance, cStddev, cMean, cDrift)
  }

  def transormArrayDataframe(sparkSession: SparkSession,
                             arrayDatafeame: DataFrame, numberOfColumns: Int): DataFrame = {
    (0 until numberOfColumns).foldLeft(arrayDatafeame)((arrayDatafeame, num) =>
      arrayDatafeame.withColumn("c_" + (num + 1), $"valueArray".getItem(num))
    ).drop("valueArray")
  }

  def formPriceListsArrayDF(sparkSession: SparkSession,
                            stockSymbol: String, stockDF: DataFrame, timeIntervals: Int, iterations: Int,
                            dailyReturnArrayDF: DataFrame): DataFrame = {
    val lastPrice: Float = stockDF.select(stockSymbol).collect()(stockDF.count().toInt - 1).getFloat(0)
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
}