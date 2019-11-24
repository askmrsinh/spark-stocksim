package com.ashessin.cs441.hw3.stocksim

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ParIterable

class InvestmentBroker(private val spark: SparkSession,
                       private val priceListDFs: scala.collection.mutable.Map[String, DataFrame],
                       private val investmentValue: Double)
  extends Serializable {

  import spark.implicits._

  val investmentResults: ListBuffer[Row] = new ListBuffer()
  val startingPriceMap: scala.collection.mutable.Map[String, Double] =
    scala.collection.mutable.Map[String, Double]()
  val allStockEstimates: ParIterable[Row] = priceListDFs.par.flatMap { case (stockSymbol, priceListDF) =>
    startingPriceMap += (stockSymbol -> priceListDF.select("c_1").first().getDouble(0))
    val iterations: Int = priceListDF.columns.length

    val random = scala.util.Random
    val simulationColumn = new Column("c_" + (random.nextInt(iterations) + 1))
    println(f"Randomly selected simulation for $stockSymbol is: $simulationColumn.")

    var id = 0
    var previousEstimate = startingPriceMap(stockSymbol)
    var mean_pct_change = 0d
    priceListDF.select(simulationColumn).collect().map(x => {
      id += 1
      val change = x.getDouble(0) - previousEstimate
      val pct_change = change / previousEstimate
      mean_pct_change = (mean_pct_change + pct_change) / id
      previousEstimate = x.getDouble(0)
      Row(id, stockSymbol, x.getDouble(0), change, pct_change, mean_pct_change)
    })
  }
  var remainingFunds = investmentValue

  // TODO: Cleanup, rethink calculation approach
  def dummyStrategy(): Unit = {

    allStockEstimates.to[scala.collection.immutable.Seq].sortBy(_.getInt(0)).foreach(observation => {
      val mean_pct_change = observation.getDouble(5)
      val predictedValue = observation.getDouble(2)

      if (mean_pct_change >= 0)
        investmentResults += Row.fromSeq(buy(observation, mean_pct_change, predictedValue))
      else
        investmentResults += Row.fromSeq(sell(observation, mean_pct_change, predictedValue))
    })

    var allStockEstimatesDF: Dataset[Row] = spark.sqlContext.createDataFrame(
      spark.sparkContext.parallelize(investmentResults.to[collection.immutable.Seq]),
      StructType {
        Array(
          StructField("id", IntegerType, nullable = false),
          StructField("stockSymbol", StringType, nullable = true),
          StructField("predictedValue", DoubleType, nullable = true),
          StructField("change", DoubleType, nullable = true),
          StructField("pct_change", DoubleType, nullable = true),
          StructField("mean_pct_change", DoubleType, nullable = true),
          StructField("quantityBought", DoubleType, nullable = true),
          StructField("marketCost", DoubleType, nullable = true),
          StructField("remainingFunds", DoubleType, nullable = true))
      })
    allStockEstimatesDF.show(200)
  }

  def buy(observation: Row,
          mean_pct_change: Double, predictedValue: Double): Seq[Any] = {
    val w: Double = if (mean_pct_change == 0) predictedValue / startingPriceMap.values.sum else mean_pct_change
    var marketCost = 0d
    var quantityBought = 0d
    if (remainingFunds > predictedValue) {
      marketCost = w * remainingFunds / startingPriceMap.size
      quantityBought = (marketCost / predictedValue)
    } else {
      println(f"Insufficient funds to buy ${observation.getString(1)} @ ${predictedValue}")
    }
    remainingFunds = remainingFunds - marketCost
    observation.toSeq ++ Seq(quantityBought, marketCost, remainingFunds)
  }

  def sell(observation: Row,
           mean_pct_change: Double, predictedValue: Double): Seq[Any] = {
    val currentHolding = investmentResults.filter(_.getString(1) == observation.getString(1)).last
    var marketCost = 0d
    var quantitySold = 0d
    if (currentHolding.size > 0) {
      quantitySold = currentHolding.getDouble(6)
      marketCost = quantitySold * predictedValue
    }
    remainingFunds = remainingFunds + marketCost
    observation.toSeq ++ Seq(-quantitySold, marketCost, remainingFunds)
  }
}