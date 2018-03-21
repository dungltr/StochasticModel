package Scala
import com.sparkexample.App
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}
/**
  * Created by letrungdung on 19/03/2018.
  */
object LinearRegressionExample {
  def main(args: Array[String]): Unit = {
    val sparkHome = new App().readhome("SPARK_HOME")
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.allowMultipleContexts", "true")
      .set(CBO_ENABLED.key, "true")
      .set(JOIN_REORDER_ENABLED.key, "true")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    //testOriginalExample(spark, sparkHome)
    testDreamExample(spark, sparkHome)
  }
  def testDreamExample(spark: SparkSession, sparkHome: String):Unit={
    val operatorData = "/Users/letrungdung/IReS-Platform/asap-platform/" +
      "asap-server/target/asapLibrary/operators/SQL_TPCH_Hive_Postgres/data"
    val training = spark.read.format("libsvm")
      .load(sparkHome + "/data/mllib/sample_linear_regression_data.txt")
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    // Fit the model
    val lrModel = lr.fit(training)
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

  }
  def testOriginalExample(spark: SparkSession, sparkHome: String): Unit = {
    val training = spark.read.format("libsvm")
      .load(sparkHome + "/data/mllib/sample_linear_regression_data.txt")
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    // Fit the model
    val lrModel = lr.fit(training)
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }
}

