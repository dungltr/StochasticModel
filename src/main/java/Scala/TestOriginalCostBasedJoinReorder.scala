package Scala

import java.io.IOException
import java.nio.file.{Files, Paths}

import Irisa.Enssat.Rennes1.thesis.sparkSQL.{OriginalPareto, historicData}
import Scala.TestCostBasedJoinReorder.tpcdsAll
import com.sparkexample.App
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}

/**
  * Created by letrungdung on 04/04/2018.
  */
object  TestOriginalCostBasedJoinReorder {

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


  val tables = Seq("catalog_page", "catalog_returns", "customer",
    "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns",
    "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason",
    "call_center", "warehouse", "ship_mode",
    "income_band", "time_dim", "web_page")
  val clouds = Seq("amazon","amazon","amazon",
    "amazon","amazon","amazon",
    "amazon","amazon","amazon",
    "amazon","amazon","amazon",
    "amazon","amazon","amazon",
    "amazon","amazon","amazon",
    "amazon","amazon","amazon",
    "amazon","amazon","amazon")
  def setupTables(dataLocation: String): Map[String, Long] = {
    val spark = SparkSession.builder.config(conf).getOrCreate()
    tables.map { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
    }.toMap
  }
  object OptimizeOriginal extends RuleExecutor[LogicalPlan] {

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val confSQL = spark.sessionState.conf
    println("The id Query is:= " + confSQL.getConfString("idQuery"))
    val batches =
      Batch("EliminateSubqueryAliases", Once, EliminateSubqueryAliases) ::
        //Batch("RewriteSubquery", Once,
        //  RewritePredicateSubquery,
        //  CollapseProject) :: Nil
        //Batch("Join Reorder", Once, CostBasedJoinReorder(confSQL)) :: Nil
        Batch("Join Reorder", Once, FirstCostBasedJoinReorder(confSQL)) :: Nil
  }
  def testBigTable():Unit={
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val tableNames = tables
    import org.apache.spark.sql.catalyst.TableIdentifier
    val tableIds = tableNames.map(TableIdentifier.apply)
    val sessionCatalog = spark.sessionState.catalog
    tableIds.foreach { tableId =>
      sessionCatalog.dropTable(tableId, ignoreIfNotExists = true, purge = true)
    }
    //val belowBroadcastJoinThreshold = spark.sessionState.conf.autoBroadcastJoinThreshold - 1
    //println("spark.sessionState.conf.autoBroadcastJoinThreshold:=" + spark.sessionState.conf.autoBroadcastJoinThreshold)
    val homeDataDesktop = "/Users/letrung/Downloads"
    val homeDataLaptop = "/Volumes/DATAHD/Downloads"
    val homeUbuntu = "/home/ubuntu/Documents"
    val tpcdsHome = new App().readhome("tpcds");
    val dataLocation = tpcdsHome
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    //val tableSizes = setupTables(dataLocation)
    val tableSizes = setupTables(dataLocation)
    tableNames.foreach { name =>
      spark.read.parquet(s"$dataLocation/$name").write.saveAsTable(s"t$name")
      spark.sql(s"ANALYZE TABLE t$name COMPUTE STATISTICS")
    }

    val Store_sales = spark.table("tstore_sales")
    val Store_returns = spark.table("tstore_returns")
    val Catalog_sales = spark.table("tcatalog_sales")
    val Catalog_returns = spark.table("tcatalog_returns")
    val Store = spark.table("tstore")
    val Item = spark.table("titem")


    val q0 = Store_sales
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Item,Store_sales("ss_item_sk")===Item("i_item_sk"))
      .join(Store,Store("s_store_sk")===Store_returns("sr_store_sk"))
    val q11 = Store_sales
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Item,Item("i_item_sk")===Catalog_sales("cs_item_sk"))

    val q12 = Store_sales
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Store_returns,Catalog_sales("cs_item_sk")===Store_returns("sr_item_sk"))
      .join(Item,Item("i_item_sk")===Store_returns("sr_item_sk"))

    val q21 = Store_sales
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Catalog_returns,Catalog_sales("cs_item_sk")===Catalog_returns("cr_item_sk"))

    val q22 = Store_sales
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_returns,Catalog_sales("cs_item_sk")===Catalog_returns("cr_item_sk"))

    val q23 = Catalog_sales
      .join(Catalog_returns,Catalog_sales("cs_item_sk")===Catalog_returns("cr_item_sk"))
      .join(Store_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))

    val Q = Seq(q11)//q11,q21,q12,q22,q23)//,q32, q31,q41,q33
    val r = scala.util.Random
    val randomInt = r.nextInt(Q.size)
    val q = Q.apply(randomInt)
    //.join(Store,Store("s_store_sk")===Store_sales("ss_store_sk"))
    //.join(Store_returns,Store_returns("sr_item_sk")===Catalog_sales("cs_item_sk"))

    val queryRelations = scala.collection.mutable.HashSet[String]()
    val  lines = q.queryExecution.logical.numberedTreeString.split("\n").toSeq
    lines.foreach(line =>
      if(line.contains("SubqueryAlias")) {
        queryRelations.add(line.substring(line.indexOf("SubqueryAlias")).replace("SubqueryAlias",""))
      }
    )
    val listRelations = queryRelations.toSet
    var kindOftest = ""
    val folder = kindOftest + listRelations.toString()
      .replace("Set","")
      .replace(" ","")
      .replace(",","_")
      .replace("(","")
      .replace(")","")
    println("List of tables in the query: q" + randomInt)
    listRelations.foreach(relation=>println(relation))
    historicData.setupFolder(folder,"")
    historicData.storeIdQuery(folder)
    val confSQL = spark.sessionState.conf
    confSQL.setConfString("idQuery",folder)
    println("---------------" )

    val sparkQ = q

    val plan = sparkQ.queryExecution.logical
    println("The original logical plan -----------------------------------------------")
    println(plan.numberedTreeString)
    println("-------------------------------------------------------------------------")
    println("The optimized plan of Spark-----------------------------------------------")
    val optimizedPlan = sparkQ.queryExecution.optimizedPlan
    println(optimizedPlan.numberedTreeString)
    println("--------------------------------------------------------------------------")
    val joinsReordered = OptimizeOriginal.execute(plan)
    val plans = OriginalPareto.setLogicalPlans(listRelations.size)
    val costs = OriginalPareto.setCosts(listRelations.size)
    val sets = OriginalPareto.setList(listRelations.size)
    kindOftest = "original"
    for (i <- 0 until sets.size()){
      val folder_home = "data/dream/"+ kindOftest + "/" + folder + "/" + sets.get(i).toString()
      val folderPath = Paths.get(folder_home)
      if (!Files.exists(folderPath)) Files.createDirectory(folderPath)
      val nameValue = "executeTime"
      val file = folder_home + "/" + nameValue + ".csv"
      val numberVariables = 2
      val filePath = Paths.get(file)
      if (!Files.exists(filePath)) try {
        Files.createFile(filePath)
        historicData.setupFile(file, numberVariables)
        //historicData.addtitle(file,numberVariables)
      }
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
    val ran = r.nextInt(plans.size())
    println("")
    println("Begin running logical plan " + ran + " in Pareto plan set with Pareto.size: "+plans.size())
    val startTime = System.nanoTime()
    val runPlan = plans.get(ran)
    val costPlan = costs.get(ran)
    val setPlan = sets.get(ran)
    val nameValue = "executeTime"

    val card = costPlan.card.toDouble// + r.nextInt(100)*costPlan.card.toDouble/1000
    val size = costPlan.size.toDouble// + r.nextInt(100)*costPlan.size.toDouble/1000

    val estimateValue = 0//historicData.estimateAndStore(folder,setPlan.toString(), nameValue, card, size)
    //val fileExecute = "data/dream/" + folder + "/" + setPlan.toString() + "/executeTime.csv"
    //println(fileExecute)
    val estimateValueMOEA = 0//LinearRegressionManual.guessValue(fileExecute,fileExecute, card, size)
    //println("The predict Value of Dream is: " + estimateValue)
    println("The predict Value of MOEA is: " + estimateValueMOEA)
    println(runPlan.numberedTreeString)
    println("Cost value of logical plan is: " + costPlan)
    println("setID of logical plan is: " + setPlan)
    spark.sessionState.executePlan(runPlan)
    val listPhysicalPlan = spark.sessionState.planner.plan(runPlan).toSeq
    println("The physical plan of logical plan: " + ran + " in Pareto plan set with Pareto.size " + plans.size())
    listPhysicalPlan.foreach(element => println(element))
    val durationInMs = System.nanoTime() - startTime
    println("End of running physical plan: " + "-------"  + durationInMs+" nano  seconds")
    //historicData.updateValue(folder,setPlan.toString(),costPlan,durationInMs.toDouble,"executeTime")
    //historicData.saveError(folder,setPlan.toString(),nameValue, durationInMs.toDouble, estimateValue, 0.8)
    //val WEKA = "executeTimeWEKA"
    //historicData.saveError(folder,setPlan.toString(), WEKA, durationInMs.toDouble, estimateValueMOEA, 0.8)
  }

  def main(args: Array[String]): Unit = {
    // List of all TPC-DS queries
    val tpcdsQueries = Seq("query25")
    val dataLocation = "/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/"
    tpcdsAll(dataLocation, queries = tpcdsQueries)
  }
}
