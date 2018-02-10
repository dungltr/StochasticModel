/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.{SparkConf, SparkContext}

class ThirdScala {
  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left,right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
        println("optimization of one applied")
        left
    }
  }
  def main () {
    println("\n Hello from Scala")
    first()
    //second()
    println("\n Goodbye")
  }
  def tableExists(table: String, spark: SparkSession) = spark.catalog.tableExists(table)
  def databaseExists(database: String, spark: SparkSession) = spark.catalog.databaseExists(database)

  def second(): Unit ={
    val hiveLocation   = "hdfs://localhost:9000/Volumes/DATAHD/user/hive/warehouse"
    val conf = new SparkConf()
      .setAppName("SOME APP NAME")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.warehouse.dir",hiveLocation)

    //val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("SparkHiveExample")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      //.config("spark.sql.warehouse.dir", hiveLocation)
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    println("Start of SQL Session--------------------")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlContext.sql("select * from orders").show()


    //hiveContext.sql("select * from orders").show()
    //val hiveContext = new HiveContext(spark_new)
    //hiveContext.sql("select * from test").show

    //spark.sql("CREATE TABLE IF NOT EXISTS test13(age String)")
    //spark.sql("select * from test13").show()
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val data_hive = sqlContext.table("test13")
    //data_hive.createOrReplaceTempView("test13")
    //data_hive.show()
    println("End of SQL session-------------------")
    spark.stop()
    sc.stop()


    ///////////////////////////////////////////////////////////////////////
  }

  def first(){
    println("\n Hello from Hive")

    ///////////////////////////////////////////////////////////////////////
    val hiveLocation   = "hdfs://localhost:9000/Volumes/DATAHD/user/hive/warehouse"
    val conf = new SparkConf()
      .setAppName("SOME APP NAME")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.warehouse.dir",hiveLocation)

    //val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("SparkHiveExample")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      //.config("spark.sql.warehouse.dir", hiveLocation)
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    println("Start of SQL Session--------------------")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //////////////////////////////////////////////////////////////////////
    //SQLContext sqlContext_new = new HiveContext(sc) // The error occurred.
    val hiveLocation_new   = "hdfs://masterisima:9000/user/hive/warehouse"
    val conf_new = new SparkConf()
      .setAppName("jdf-dt-rtoc-withSQL")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("hive.metastore.uris", "thrift://masterisima:9083")
      .set("spark.sql.warehouse.dir", hiveLocation_new)
      .set("spark.driver.allowMultipleContexts", "true")
      .setMaster("local[*]")
    val spark_new = SparkSession
      .builder()
      .appName("Spark Postgres Example")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://masterisima:9083")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate();
    val sc_new = new SparkContext(conf_new)
    val sqlContext_new = new org.apache.spark.sql.SQLContext(sc_new)
    //val sparktest = SparkSession
    //    .builder()
    //    .appName("Spark HiveTest Example")
    //    .master("local[*]")
        //.config("spark.driver.allowMultipleContexts", "true")
    //    .getOrCreate();
    println("\n Test database is:"+ databaseExists("default",spark_new) +" Test table is: " + tableExists("default.lineitem",spark_new))

    val data_hive = sqlContext_new.table("orders")
    data_hive.createOrReplaceTempView("orders")
    //data_hive.show();

    println("\n Hello from Postgres")

//    val dataDF_postgres = spark.read.jdbc.jdbc("jdbc:postgresql://localhost:5432/tpch100m", "lineitem")
//        .option("user", username)
//        .option("password", password)
//        .load()

    //val dataDF_postgres = sqlContext.read.format("jdbc").options(
    //    Map("url" -> "jdbc:postgresql:tpch100m:5432",
    //    "dbtable" -> "lineitem")).load()
    //dataDF_postgres.createOrReplaceTempView("lineitem");
    //dataDF_postgres.show();

    //val dataDF_postgres2 = sqlContext.read.format("jdbc").options(
    //  Map("url" -> "jdbc:postgresql:tpch100m:5432",
    //    "dbtable" -> "orders")).load()
    //dataDF_postgres2.createOrReplaceTempView("orders");
    //dataDF_postgres2.show();

    val query = spark_new.sql("select * from orders,lineitem where l_orderkey = o_orderkey")
    query.show()
    println(query.queryExecution.optimizedPlan.numberedTreeString)
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule);

    sc.stop();
//    sc.close();
    spark.stop();
  }
  def main_third() {
    println("\n Hello world")
      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .getOrCreate()
      val textFile = spark.read.textFile("hdfs://localhost:9000/user/hive/warehouse/people.txt")
      println("\n The number of word in file is:=" + textFile.count)

      //////////////////////////////////////////////////////////////
      OriginalExample(spark)
      CustomExample(spark)
      /////////////////////////////////////////////////////////////
      spark.stop()
      println("\n Goodbye")
  }
  private def OriginalExample(spark: SparkSession): Unit = {
    val df = spark.read.option("header","true").csv("src/main/resources/sales.csv")
    val multipliedDF = df.selectExpr("amountPaid * 1")
    println(multipliedDF.queryExecution.optimizedPlan.numberedTreeString)
    
/*    //add our custom optimization
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")
    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
*/  }
  private def CustomExample(spark: SparkSession): Unit = {
    val df = spark.read.option("header","true").csv("src/main/resources/sales.csv")
    //add our custom optimization
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")
    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
  }  

}
