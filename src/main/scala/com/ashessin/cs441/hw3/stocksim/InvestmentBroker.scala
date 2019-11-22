package com.ashessin.cs441.hw3.stocksim

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class InvestmentBroker(private val spark: SparkSession) extends Serializable {

  def makeDynamicInvestment(investmentValue: Double, priceListDFs: List[DataFrame]): Unit = {

    for (priceListDF <- priceListDFs) {
      val startingPrice: Double = priceListDF.select("c_1").first().getDouble(0)
      var previousEstimate = startingPrice
      val iterations: Int = priceListDF.columns.length

      val random = scala.util.Random
      val simulationColumn = new Column("c_" + (random.nextInt(iterations) + 1))
      println(f"Randomly selected simulation is: ${simulationColumn}.")

      val estimatedValues: Array[Seq[Double]] = priceListDF.select(simulationColumn).collect().map(x => {
        val change = previousEstimate - x.getDouble(0)
        val pct_change = change / previousEstimate
        previousEstimate = x.getDouble(0)
        Seq(x.getDouble(0), change, pct_change)
      })
      estimatedValues.foreach(println)
    }
  }
}