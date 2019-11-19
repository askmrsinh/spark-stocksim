package com.ashessin.cs441.hw3.stocksim

import org.apache.commons.math3.distribution.{AbstractRealDistribution, NormalDistribution}
import org.apache.spark.mllib.random.RandomRDDs.uniformVectorRDD
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object Montecarlo {
    private val logger = LoggerFactory.getLogger("Montecarlo")

    def main(args: Array[String]): Unit = {

        logger.info("Starting Montecarlo")

        // Zeppelin creates and injects sc (SparkContext) and sqlContext (HiveContext or SqlContext)
        // So you don't need create them manually

        // set up environment
        val spark: SparkSession = SparkSession.builder.appName("Montecarlo")
          .config("spark.master", "local")
          .getOrCreate()
        import spark.implicits._

        val windowSpec: WindowSpec = Window.partitionBy().orderBy("Date")

        val stockSymbol: String = "MSFT"
        val stockSchema: StructType = createStockSchema(stockSymbol)

        val stockDF: DataFrame = spark.read.format("csv").schema(stockSchema)
          .option("sep", ",")
          .option("header", "true")
          .load("MSFT_2000.csv")
          .withColumn("change", $"MSFT" - lag("MSFT", 1).over(windowSpec))
          .withColumn("pct_change", $"change" / lag("MSFT", 1).over(windowSpec))
          .withColumn("log_returns", log1p("pct_change"))
        stockDF.createOrReplaceTempView("stockDF")
        if (logger.isDebugEnabled()) {
            stockDF.printSchema()
            stockDF.show(5)
        }

        val (variance, deviation, mean, drift) = calculateStockColumnStats(stockDF, "log_returns")
        logger.info("{} stock returns variance: {}", stockSymbol, variance)
        logger.info("{} stock returns deviation: {}", stockSymbol, deviation)
        logger.info("{} stock returns mean: {}", stockSymbol, mean)
        logger.info("{} stock returns drift: {}", stockSymbol, drift)

        val timeIntervals = 250
        val iterations = 1000

        val normalDistribution: NormalDistribution = new NormalDistribution(0, 1)

        val dailyReturnArrayDF: DataFrame = formDailyReturnArrayDF(spark,
            timeIntervals, iterations, normalDistribution, drift, deviation)
        if (logger.isDebugEnabled()) {
            dailyReturnArrayDF.printSchema()
            dailyReturnArrayDF.show(5)
        }

        val priceList = formPriceLists(spark, stockSymbol, stockDF, timeIntervals, iterations, dailyReturnArrayDF)
        val priceListArrayDF: DataFrame = priceList.toDF("valueArray")

        val priceListDF: DataFrame = transormArrayDataframe(spark, priceListArrayDF, iterations)
        if (logger.isDebugEnabled()) {
            priceListDF.printSchema()
            priceListDF.show(5)
        }
        priceListDF.createOrReplaceTempView("priceListDF")
    }

    def calculateStockColumnStats(df: DataFrame, c: String): (Double, Double, Double, Double) = {

        val cVariance: Double = df.select(variance(c)).first().getDouble(0)
        val cStddev: Double = df.select(stddev(c)).first().getDouble(0)
        val cMean: Double = df.select(mean(df(c))).first().getDouble(0)
        val cDrift: Double = cMean - (0.5 * cVariance)
        (cVariance, cStddev, cMean, cDrift)
    }

    def createStockSchema(symbol: String): StructType = {

        val stockSchema: StructType = StructType {
            Array(
                StructField("Date", DateType, nullable = false),
                StructField(symbol.toUpperCase, FloatType, nullable = true))
        }
        stockSchema
    }

    def calculateStockDailyReturn(distribution: AbstractRealDistribution,
                                  drift: Double, deviation: Double, value: Double): Double = {

        Math.exp(drift + deviation * distribution.inverseCumulativeProbability(value))
    }

    def transormArrayDataframe(sparkSession: SparkSession,
                               arrayDatafeame: DataFrame, numberOfColumns: Int): DataFrame = {

        import sparkSession.implicits._
        (0 until numberOfColumns).foldLeft(arrayDatafeame)((arrayDatafeame, num) =>
            arrayDatafeame.withColumn("c_" + (num + 1), $"valueArray".getItem(num)))
          .drop("valueArray")
    }

    def formPriceLists(sparkSession: SparkSession,
                       stockSymbol: String, stockDF: DataFrame,
                       timeIntervals: Int, iterations: Int, dailyReturnArrayDF: DataFrame): ListBuffer[List[Double]] = {

        val lastPrice: Float = stockDF.select(stockSymbol).collect()(stockDF.count().toInt - 1).getFloat(0)
        var priceList = new ListBuffer[List[Double]]()
        for (id <- 0 to timeIntervals) {
            if (id == 0) {
                priceList += List.fill(iterations)(lastPrice)
            } else {
                val x = priceList(id - 1)
                val y = dailyReturnArrayDF.select("valueArray").collect()(id - 1).getSeq[Double](0)
                priceList += x.zip(y).map { case (x, y) => x * y }
            }
        }
        priceList
    }

    def formDailyReturnArrayDF(sparkSession: SparkSession,
                               timeIntervals: Int, iterations: Int,
                               distribution: AbstractRealDistribution, drift: Double, deviation: Double): DataFrame = {

        import sparkSession.implicits._
        uniformVectorRDD(sparkSession.sparkContext, timeIntervals, iterations)
          .map(_.toArray.toIterable
            .map(x => calculateStockDailyReturn(distribution, drift, deviation, x)))
          .map(_.toArray).toDF("valueArray")

    }
}
