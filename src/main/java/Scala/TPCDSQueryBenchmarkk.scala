/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

//import java.io.File
import java.io.File

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression


//import Scala.TestCostBasedJoinReorder.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}
import org.apache.spark.{SparkConf, SparkContext}

object TPCDSQueryBenchmark {
  val conf =
    new SparkConf()
      .setMaster("local[1]")
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.allowMultipleContexts", "true")
      .set(CBO_ENABLED.key, "true")
      .set(JOIN_REORDER_ENABLED.key, "true")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)

  val spark = SparkSession.builder.config(conf).getOrCreate()
  val confSQL = spark.sessionState.conf
  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String): Map[String, Long] = {

    tables.map { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
    }.toMap
  }
  //spark.table("catalog_page").show()



  def printList(args: List[String]): Unit = {
    args.foreach(println)
  }
  def printListLogicalPlan(args: List[LogicalPlan]): Unit = {
    args.foreach(println)
  }
  def tpcdsAll(dataLocation: String, queries: Seq[String]): Unit = {
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation)
    spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    queries.foreach { name =>
      //val queryString = fileToString(new File(Thread.currentThread().getContextClassLoader
      //  .getResource(s"tpcds/$name.sql").getFile))
      val queryString = fileToString(new File(s"$dataLocation/$name.sql"))
      println(queryString)
      /*
      val queriesString = fileToString(new

          File(s"sql/core/src/test/scala/org/apache/spark/sql/" +
                   s"execution/benchmark/tpcds/queries/$name.sql"))
      */
      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan. Note that this
      // currently doesn't take WITH subqueries into account which might lead to fairly inaccurate
      // per-row processing time for those cases.
      val queryRelations = scala.collection.mutable.HashSet[String]()

      //  spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS")
      queryRelations.foreach(name => spark.table(name).count())

      //println(name)
      println(spark.sql(queryString).queryExecution.logical)
      println("spark.sessionState.conf.cboEnabled: "+spark.sessionState.conf.cboEnabled)
      println("spark.sessionState.conf.joinReorderEnabled: "+spark.sessionState.conf.joinReorderEnabled)
      spark.sql(queryString).queryExecution.logical.map {
        case ur @ UnresolvedRelation(t: TableIdentifier) =>
          queryRelations.add(t.table)
        case lp: LogicalPlan =>
          lp.expressions.foreach { _ foreach {
            case subquery: SubqueryExpression =>
              subquery.plan.foreach {
                case ur @ UnresolvedRelation(t: TableIdentifier) =>
                  queryRelations.add(t.table)
                case _ =>
              }
            case _ =>
          }
        }
        case _ =>
      }


      val listRelations = queryRelations.toList
      val a = spark.sql(queryString).queryExecution.logical.subqueries
      val b = a.toList
      printList(listRelations)
      printListLogicalPlan(b)
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      /*
      val benchmark = new Benchmark("TPCDS Snappy", numRows, 5)
      benchmark.addCase(name) { i =>
        spark.sql(queryString).collect()
      }
      benchmark.run()
      */
    }

    //spark.sql("DESC EXTENDED catalog_page").show(numRows = 30, truncate = false)
    //spark.sql("DESC EXTENDED catalog_returns").show(numRows = 30, truncate = false)

    val tableName = "catalog_page"
    spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
    val tableId = TableIdentifier(tableName)

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    val df = Seq((0, 0.0, "zero"), (1, 1.4, "one")).toDF("cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk")
    val allCols = df.columns.mkString(",")
    val analyzeTableSQL = s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS $allCols"
    df.show()
    println(analyzeTableSQL)

    //val plan = spark.sql(analyzeTableSQL).queryExecution.logical
    /*
    import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
    val cmd = plan.asInstanceOf[AnalyzeColumnCommand]
    */
  }

  def main(args: Array[String]): Unit = {

    // List of all TPC-DS queries
    val tpcdsQueries = Seq("query25" //"query02", "query03", "query04", "query05", "query06", "query07", "query08", "query09", "query10", "query11",
      //"query12", "query13", "query14a", "query14b", "query15", "query16", "query17", "query18", "query19", "query20",
      //"query21", "query22", "query23a", "query23b", "query24a", "query24b", "query25", "query26", "query27", "query28", "query29", "query30",
      //"query31", "query32", "query33", "query34", "query35", "query36", "query37", "query38", "query39a", "query39b", "query40",
      //"query41", "query42", "query43", "query44", "query45", "query46", "query47", "query48", "query49", "query50",
      //"query51", "query52", "query53", "query54", "query55", "query56", "query57", "query58", "query59", "query60",
      //"query61", "query62", "query63", "query64", "query65", "query66", "query67", "query68", "query69", "query70",
      //"query71", "query72", "query73", "query74", "query75", "query76", "query77", "query78", "query79", "query80",
      //"query81", "query82", "query83", "query84", "query85", "query86", "query87", "query88", "query89", "query90",
      //"query91", "query92", "query93", "query94", "query95", "query96", "query97", "query98", "query99"
    )
    // In order to run this benchmark, please follow the instructions at
    // https://github.com/databricks/spark-sql-perf/blob/master/README.md to generate the TPCDS data
    // locally (preferably with a scale factor of 5 for benchmarking). Thereafter, the value of
    // dataLocation below needs to be set to the location where the generated data is stored.
    val dataLocation = "/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/"
    tpcdsAll(dataLocation, queries = tpcdsQueries)
    //testSomething(dataLocation, queries = tpcdsQueries, Tables = tables)
  }
  def testSomething(dataLocation: String, queries: Seq[String], Tables: Seq[String]): Unit = {
    spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    spark.conf.set(CBO_ENABLED.key, "true")
    spark.conf.set(JOIN_REORDER_ENABLED.key, "true")
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    queries.foreach {
      name => val queryString = fileToString(new File(s"$dataLocation/$name.sql"))
      Tables.foreach {
        tableName => spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      }

        println("This is the logical plan for: " + name + "---------------------------------------------------")
        val plan = spark.sql(queryString).queryExecution.analyzed
        val noAliasesPlan = EliminateSubqueryAliases(plan)
        println(noAliasesPlan.numberedTreeString)
        println("spark.sessionState.conf.cboEnabled: "+spark.sessionState.conf.cboEnabled)
        println("spark.sessionState.conf.joinReorderEnabled: "+spark.sessionState.conf.joinReorderEnabled)

        /*
        println("This is the optimization plan for: " + name + "---------------------------------------------------")
        val optimizePlan = spark.sql(queryString).queryExecution.optimizedPlan
        println(optimizePlan.numberedTreeString)
        val noAliasesOptimizePlan = EliminateSubqueryAliases(optimizePlan)
        println(noAliasesOptimizePlan.numberedTreeString)

        //println(spark.sql(queryString).explain(true))
        */
        println("This is the ReOrder plan for: " + name + "---------------------------------------------------")
        val reOrderPlan = spark.sql(queryString).queryExecution.analyzed
        val joinsReordered = Optimize.execute(reOrderPlan)
        println(joinsReordered.numberedTreeString)

    }
  }
  object Optimize extends RuleExecutor[LogicalPlan] {
    println("customer Optimize")
    //val spark = SparkSession.builder.config(conf).getOrCreate()
    val batches =
      Batch("EliminateSubqueryAliases", Once, EliminateSubqueryAliases) ::
        //Batch("RewriteSubquery", Once,
        //  RewritePredicateSubquery,
        //  CollapseProject) ::
        Batch("Join Reorder", Once, CostBasedJoinReorder(confSQL)) :: Nil
  }
}
