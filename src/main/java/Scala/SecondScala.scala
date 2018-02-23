/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode

class SecondScala {
  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left,right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
        println("optimization of one applied")
        left
    }
  }
  def main_test() {
    println("\n Hello world")
      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .getOrCreate()
      val textFile = spark.read.textFile("hdfs://localhost:9000//Volumes/DATAHD/user/hive/warehouse/people.txt")
      println("\n The number of word in file is:=" + textFile.count)

      //////////////////////////////////////////////////////////////
      OriginalExample(spark)
      CustomExample(spark)
      /////////////////////////////////////////////////////////////
      spark.stop()
      println("\n Goodbye")
  }
  private def OriginalExample(spark: SparkSession): Unit = {
    val sales = spark.read.option("header","true").csv("src/main/resources/sales.csv")
    sales.createOrReplaceTempView("sales")
    val customers = spark.read.option("header","true").csv("src/main/resources/customers.csv")
    customers.createOrReplaceTempView("customers")
    //val multipliedDF = df.selectExpr("amountPaid * 1")
    val SalesJoinCustomers = sales.join(customers, sales("customerID") === customers("customerID"))
//    println(SalesJoinCustomers)
    val WhereCustomersID = SalesJoinCustomers.where(sales("customerID")<3)
    val GroupBy = WhereCustomersID.groupBy(sales("customerID"), customers("customerID"))
    val sqlTxt = "select * from sales, customers where sales.customerId = customers.customerId and sales.customerId < 3"
    val query = spark.sql(sqlTxt)
    val test = spark.sessionState.sqlParser.parsePlan(sqlTxt)
    println("Show text of sqlParser"+test)
    query.show()
    println("Heeeeee"+query.toString+"quitIIIIIII")
    //println(GroupBy.toString)
    println("This is the executedPlan \n"+query.queryExecution.executedPlan)
    println("This is the logicalPlan \n"+query.queryExecution.logical)
    println("This is the sparkPlan \n"+query.queryExecution.sparkPlan)
    println("This is the optimizedPlan \n"+query.queryExecution.optimizedPlan.numberedTreeString)
    
    query.queryExecution.executedPlan
  }
  private def CustomExample(spark: SparkSession): Unit = {
    val df = spark.read.option("header","true").csv("src/main/resources/sales.csv")
    //add our custom optimization
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")
    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
    println("This is the logicalPlan \n"+multipliedDFWithOptimization.queryExecution.logical)
  }


}
