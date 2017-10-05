/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

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
        right.asInstanceOf[Literal].value.asInstanceOf[Double] < 1.0 =>
        println("optimization of one applied")
        left
    }
  }
  def main_third() {
    println("\n Hello world")
      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .getOrCreate()
      val textFile = spark.read.textFile("hdfs://master:9000/user/hive/warehouse/people.txt")
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
