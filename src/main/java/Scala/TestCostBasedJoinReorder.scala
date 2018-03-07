package Scala
import Irisa.Enssat.Rennes1.ParetoPlanSet
import Scala.JoinReorderDP.JoinPlan
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}

import scala.collection.mutable
/**
  * Created by letrungdung on 01/03/2018.
  */
object TestCostBasedJoinReorder {
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
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)

  
  val tables = Seq("store_sales", "store_returns","catalog_sales"// "catalog_page", "catalog_returns", "customer"//, "customer_address",
    //"customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    //"promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    //"web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    //"time_dim", "web_page"
  )
  
  def setupTables(dataLocation: String): Map[String, Long] = {
    println("Hello from setupTables")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    println("Goodbye from setupTables")
    tables.map { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
    }.toMap    
  }
  val allPlanMap = mutable.Map.empty[List[Int], JoinPlan]
  def takeListPlan(listMapLogicalPlan: List[(List[Int], JoinReorderDP.JoinPlan)]):Unit = {
    for (temp <- listMapLogicalPlan){
      val listInt = temp._1
      val listJoin = temp._2
      println(listInt)
      println(listJoin.itemIds)
      println(listJoin.plan)
      println(listJoin.planCost)
      allPlanMap.update(listInt,listJoin)
    }
    takeLogicalPlan()
  }
  val allPlanList = List.empty[LogicalPlan]

  def takeLogicalPlan(): Unit ={
    for(temp<-allPlanMap){
      ParetoPlanSet.addLogicalPlan(temp._2.plan)
      ParetoPlanSet.addCostPlan(temp._2.planCost)
      ParetoPlanSet.addSetPlan(temp._1)
    }
    ParetoPlanSet.filterPlans()
  }
  def tpcdsAll(dataLocation: String, queries: Seq[String]): Unit = {
    println("Hello from tpcdsAll")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation)
    /*
    val tableNames = tables

    val store_sales = spark.read.parquet(s"$dataLocation/store_sales").createOrReplaceTempView("store_sales")
    val store_returns = spark.read.parquet(s"$dataLocation/store_returns").createOrReplaceTempView("store_returns")
    val catalog_sales = spark.read.parquet(s"$dataLocation/catalog_sales").createOrReplaceTempView("catalog_sales")
    val Store_sales = spark.table("store_sales")
    val Store_returns = spark.table("store_returns")
    val Catalog_sales = spark.table("catalog_sales")

    Store_sales.show()
    Store_returns.show()
    Catalog_sales.show()
    */
    val tableNames = Seq("t1", "t2", "tiny")
    import org.apache.spark.sql.catalyst.TableIdentifier
    val tableIds = tableNames.map(TableIdentifier.apply)
    val sessionCatalog = spark.sessionState.catalog
    tableIds.foreach { tableId =>
      sessionCatalog.dropTable(tableId, ignoreIfNotExists = true, purge = true)
    }

    val belowBroadcastJoinThreshold = spark.sessionState.conf.autoBroadcastJoinThreshold - 1
    println("spark.sessionState.conf.autoBroadcastJoinThreshold:=" + spark.sessionState.conf.autoBroadcastJoinThreshold)
    spark.range(10).write.saveAsTable("t1")//404L
    // t2 is twice as big as t1
    spark.range(10).write.saveAsTable("t2")//408L
    spark.range(10).write.saveAsTable("tiny") // 412L
    // 123 412 404 408
    // 132 412 404 408
    // 231 412 404 408

    // 213 412 408 404
    // 312 412 408 404
    // 321 412 408 404
    // Compute row count statistics
    tableNames.foreach { t =>
      spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS")
    }

    // Load the tables
    val t1 = spark.table("t1")
    val t2 = spark.table("t2")
    val tiny = spark.table("tiny")

    t1.show()
    t2.show()
    tiny.show()

    // Example: Inner join with join condition
    val q = t1.join(t2, Seq("id")).join(tiny, Seq("id"))
    //val queryRelations = scala.collection.mutable.HashSet[String]()
    /*q.queryExecution.logical.map {
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
    val queryRelation = scala.collection.mutable.HashSet[String]()
    val listRelations = queryRelations.toList
    val a = q.queryExecution.logical.subqueries
    val b = a.toList
    printList(listRelations)
    printListLogicalPlan(b)
    */
    val plan = q.queryExecution.analyzed
    println(plan.numberedTreeString)

    //val optimizePlan = q.queryExecution.optimizedPlan
    //println(optimizePlan)
    //val noAliasesPlan = EliminateSubqueryAliases(plan)
    //println(noAliasesPlan.numberedTreeString)

    val joinsReordered = Optimize.execute(plan)
    //val condition = extractInnerJoins(plan)
    //val joinsReordered = Optimize.execute(plan)
    println(joinsReordered.numberedTreeString)

  }

  //Optimizer(sessionCatalog: SessionCatalog, conf: SQLConf)
  //extends RuleExecutor[LogicalPlan]
  object Optimize extends RuleExecutor[LogicalPlan] {

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val confSQL = spark.sessionState.conf
    val batches =
      Batch("EliminateSubqueryAliases", Once, EliminateSubqueryAliases) ::
        //Batch("RewriteSubquery", Once,
        //  RewritePredicateSubquery,
        //  CollapseProject) :: Nil
    Batch("Join Reorder", Once, CostBasedJoinReorder(confSQL)) :: Nil
    spark.stop()
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
  }
  def test():Unit={
    println("Hello from TestCostBasedJoinReorder")
    val tpcdsQueries = Seq("query25")
    val dataLocation = "/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/"
    tpcdsAll(dataLocation, queries = tpcdsQueries)
  }
}
