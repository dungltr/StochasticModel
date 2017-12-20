/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

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
    println("\n Goodbye")
  }
  
  def first(){
    println("\n Hello from Hive")
    val conf = new SparkConf()
            .setAppName("jdf-dt-rtoc-withSQL")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("hive.metastore.uris", "thrift://localhost:9083")
            .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    SQLContext sqlContext = new HiveContext(sc) // The error occurred.
    //
    //val data_hive = sqlContext.table("tpch100m.orders")
    //data_hive.createOrReplaceTempView("orders")
    //data_hive.show();
    println("\n Hello from Postgres")
    val spark = SparkSession
        .builder()
        .appName("Spark Postgres Example")
        .master("local[*]")
        .getOrCreate();
//    val dataDF_postgres = spark.read.jdbc.jdbc("jdbc:postgresql://localhost:5432/tpch100m", "lineitem")
//        .option("user", username)
//        .option("password", password)
//        .load()
    val dataDF_postgres = sqlContext.read.format("jdbc").options(
        Map("url" -> "jdbc:postgresql:localhost:5432",
        "dbtable" -> "tpch100m.lineitem")).load()
    dataDF_postgres.createOrReplaceTempView("lineitem");
    dataDF_postgres.show();
    
    
    val query = spark.sql("select * from orders,lineitem where l_orderkey = o_orderkey")
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
