package com.ashessin.cs441.hw3.stocksim

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, _}

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
  var remainingFunds: Double = investmentValue
  // TODO: Cleanup, rethink calculation approach

  /**
   * A dummy investment strategy to buy and sell stocks on any given day (observation)
   *
   * On the first day, equal amount of money is allocated for buying each of the symbols
   * in the portfolio. There after, stocks are bought/sold when the mean percentage change
   * in the past days of simulation is greater/less than 0 respectively for each symbol.
   *
   * Buying happens incrementally, where as selling is cumulative.
   * On the last day, all stocks are sold and profit/loss is booked.
   */
  def dummyStrategy(): Unit = {

    allStockEstimates.to[scala.collection.immutable.Seq].sortBy(_.getInt(0)).foreach(observation => {
      val mean_pct_change = observation.getDouble(5)
      val predictedValue = observation.getDouble(2)

      if (mean_pct_change >= 0)
        investmentResults += Row.fromSeq(buy(observation, mean_pct_change, predictedValue))
      else
        investmentResults += Row.fromSeq(sell(observation, mean_pct_change, predictedValue))
    })

    sellAll()

    val allStockEstimatesDF: Dataset[Row] = spark.sqlContext.createDataFrame(
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
          StructField("remainingFunds", DoubleType, nullable = true),
          StructField("outlook", StringType, nullable = true))
      })
    allStockEstimatesDF.show(252 * startingPriceMap.size)

    println(f"Investment amount: ${investmentValue}")
    println(f"Amount at the end of simulation: ${remainingFunds}")
    println(f"change: ${(remainingFunds - investmentValue) * 100 / investmentValue}%%")
  }

  def buy(observation: Row,
          mean_pct_change: Double, predictedValue: Double): Seq[Any] = {
    var marketCost = 0d
    var quantityBought = 0d
    if (remainingFunds > predictedValue) {
      if (mean_pct_change == 0) {
        marketCost = investmentValue / startingPriceMap.size
      } else {
        marketCost = remainingFunds / startingPriceMap.size
      }
      quantityBought = math.floor(marketCost / predictedValue)
    } else {
      val day = observation.getInt(0)
      println(f"Insufficient funds to buy ${observation.getString(1)} @ ${predictedValue} on ${day}.")
    }
    remainingFunds = remainingFunds - (quantityBought * predictedValue)
    observation.toSeq ++ Seq(quantityBought, quantityBought * predictedValue, remainingFunds, "BUY")
  }

  def sellAll(): Unit = {
    allStockEstimates.to[scala.collection.immutable.Seq].sortBy(_.getInt(0))
      .takeRight(startingPriceMap.size).foreach(observation => {
      val mean_pct_change = observation.getDouble(5)
      val predictedValue = observation.getDouble(2)
      investmentResults += Row.fromSeq(sell(observation, mean_pct_change, predictedValue))
    })
  }

  def sell(observation: Row,
           mean_pct_change: Double, predictedValue: Double): Seq[Any] = {
    var quantity = 0d
    investmentResults.filter(_.getString(1) == observation.getString(1)).foreach(
      x => {
        quantity = quantity + x.getDouble(6)
      }
    )
    var marketCost = 0d
    var quantitySold = 0d
    if (quantity > 0) {
      quantitySold = quantity
      marketCost = math.abs(quantitySold) * predictedValue
    }
    remainingFunds = remainingFunds + marketCost
    observation.toSeq ++ Seq(-quantitySold, marketCost, remainingFunds, "SELL")
  }
}