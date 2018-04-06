package Scala
import java.io.{File, IOException}
import java.nio.file.{Files, Paths}

import Algorithms.LinearRegressionManual
import Irisa.Enssat.Rennes1.thesis.sparkSQL.{Pareto, historicData}
import com.sparkexample.App
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
//import Scala.TPCDSQueryBenchmark.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}
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
  def tpcdsAll(dataLocation: String, queries: Seq[String]): Unit = {
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

    //val belowBroadcastJoinThreshold = spark.sessionState.conf.autoBroadcastJoinThreshold - 1
    //println("spark.sessionState.conf.autoBroadcastJoinThreshold:=" + spark.sessionState.conf.autoBroadcastJoinThreshold)
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

    val joinsReordered = OptimizeS.execute(plan)
    //val condition = extractInnerJoins(plan)
    //val joinsReordered = Optimize.execute(plan)
    println(joinsReordered.numberedTreeString)

  }

  //Optimizer(sessionCatalog: SessionCatalog, conf: SQLConf)
  //extends RuleExecutor[LogicalPlan]
  object OptimizeS extends RuleExecutor[LogicalPlan] {

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val confSQL = spark.sessionState.conf
    println("The id Query is:= " + confSQL.getConfString("idQuery"))
    val batches =
      Batch("EliminateSubqueryAliases", Once, EliminateSubqueryAliases) ::
        //Batch("RewriteSubquery", Once,
        //  RewritePredicateSubquery,
        //  CollapseProject) :: Nil
    //Batch("Join Reorder", Once, CostBasedJoinReorder(confSQL)) :: Nil
    Batch("Join Reorder", Once, MultipleCostBasedJoinReorder(confSQL)) :: Nil
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
  def testQuerySmall():Unit={
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val tpcdsQueries = Seq("query25")
    val queries = tpcdsQueries
    val dataLocation = "/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/"
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
      /*
      val queryRelations = scala.collection.mutable.HashSet[String]()


      queryRelations.foreach(name => spark.table(name).count())
      queryRelations.foreach(t => spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS"))

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
      printList(listRelations)

      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      */
      val tableNames = tables
      tableNames.foreach { name =>
        spark.read.parquet(s"$dataLocation/$name").write.saveAsTable(s"$name")
        spark.sql(s"ANALYZE TABLE $name COMPUTE STATISTICS")
      }
      val Store_sales = spark.table("store_sales")
      val Store_returns = spark.table("store_returns")
      val Catalog_sales = spark.table("catalog_sales")
      val Catalog_returns = spark.table("catalog_returns")
      //Store_sales.show()
      //Store_returns.show()
      //Catalog_sales.show()
      val plan = Store_sales
        .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
        .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
        .join(Catalog_returns,Catalog_sales("cs_item_sk")===Catalog_returns("cr_item_sk"))


      plan.explain(true)
      println(plan.queryExecution.logical.numberedTreeString)

      val optimizePlan = plan.queryExecution.optimizedPlan
      println(optimizePlan.numberedTreeString)
      //spark.sql(queryString).show()
      val joinsReordered = OptimizeS.execute(plan.queryExecution.logical)
      println(joinsReordered.numberedTreeString)
    }
    //spark.sql("DESC EXTENDED catalog_page").show(numRows = 30, truncate = false)
    //spark.sql("DESC EXTENDED catalog_returns").show(numRows = 30, truncate = false)
    /*
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
    */

    //val plan = spark.sql(analyzeTableSQL).queryExecution.logical
    /*
    import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
    val cmd = plan.asInstanceOf[AnalyzeColumnCommand]
    */
  }
  def testSmallTable():Unit={
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val tableNames = Seq("t1", "t2", "tiny")
    import org.apache.spark.sql.catalyst.TableIdentifier
    val tableIds = tableNames.map(TableIdentifier.apply)
    val sessionCatalog = spark.sessionState.catalog
    tableIds.foreach { tableId =>
      sessionCatalog.dropTable(tableId, ignoreIfNotExists = true, purge = true)
    }
    //val belowBroadcastJoinThreshold = spark.sessionState.conf.autoBroadcastJoinThreshold - 1
    //println("spark.sessionState.conf.autoBroadcastJoinThreshold:=" + spark.sessionState.conf.autoBroadcastJoinThreshold)
    spark.range(10).write.saveAsTable("t1")//404L
    spark.range(10).write.saveAsTable("t2")//408L
    spark.range(10).write.saveAsTable("tiny") // 412L
    val person = Person("Andy", 32)

    // Encoders are created for Java beans



    tableNames.foreach { t =>
      spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS")
    }
    val t1 = spark.table("t1")
    val t2 = spark.table("t2")
    val tiny = spark.table("tiny")
    t1.show()
    t2.show()
    tiny.show()
    // Example: Inner join with join condition
    val q = t1.join(t2, Seq("id")).join(tiny, Seq("id"))
    val plan = q.queryExecution.analyzed
    println(plan.numberedTreeString)
    val joinsReordered = OptimizeS.execute(plan)
    println(joinsReordered.numberedTreeString)

  }
  def testSmallTable2():Unit={
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val tableNames = Seq("t1", "t2", "t4", "tiny", "people", "job", "nation","framePeople","frameJob","frameNation")
    import org.apache.spark.sql.catalyst.TableIdentifier
    val tableIds = tableNames.map(TableIdentifier.apply)
    val sessionCatalog = spark.sessionState.catalog
    tableIds.foreach { tableId =>
      sessionCatalog.dropTable(tableId, ignoreIfNotExists = true, purge = true)
    }
    //val belowBroadcastJoinThreshold = spark.sessionState.conf.autoBroadcastJoinThreshold - 1
    //println("spark.sessionState.conf.autoBroadcastJoinThreshold:=" + spark.sessionState.conf.autoBroadcastJoinThreshold)
    spark.range(10).write.saveAsTable("t1")//404L
    spark.range(10).write.saveAsTable("t2")//408L
    spark.range(10).write.saveAsTable("tiny") // 412L
    //spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i"))).write.saveAsTable("t4")

    val people = spark.read.json("src/main/resources/people.json")
    val job = spark.read.json("src/main/resources/job.json")
    val nation = spark.read.json("src/main/resources/nation.json")

    spark.read.json("src/main/resources/people.json").write.saveAsTable("framePeople")
    spark.read.json("src/main/resources/job.json").write.saveAsTable("frameJob")
    spark.read.json("src/main/resources/nation.json").write.saveAsTable("frameNation")

    people.write.saveAsTable("people")
    job.write.saveAsTable("job")
    nation.write.saveAsTable("nation")

    tableNames.foreach { t =>
      spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS")
    }

    val t1 = spark.table("t1")
    val t2 = spark.table("t2")
    val tiny = spark.table("tiny")
    val t4= spark.table("t4")

    val framePeople = spark.table("framePeople")
    val frameJob = spark.table("frameJob")
    val frameNation = spark.table("frameNation")

    // Queries can then join DataFrame data with data stored in Hive.
     // Example: Inner join with join condition
    val q = t4.join(t2, t2("id")===t4("stt")).join(tiny, tiny("id")===t4("stt"))
    val q2 = framePeople
      .join(frameJob,Seq("id"))
      .join(frameNation,Seq("id"))

    val plan = q2.queryExecution.analyzed
    println(plan.numberedTreeString)
    val joinsReordered = OptimizeS.execute(plan)
    println(joinsReordered.numberedTreeString)

  }
  def testSmallTable3():Unit={
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
    val dataLocation = "/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/"
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation)
    tableNames.foreach { name =>
      spark.read.parquet(s"$dataLocation/$name").write.saveAsTable(s"$name")
      spark.sql(s"ANALYZE TABLE $name COMPUTE STATISTICS")
    }

    spark.range(10).write.saveAsTable("t1")//404L
    spark.range(10).write.saveAsTable("t2")//408L
    spark.range(10).write.saveAsTable("tiny") // 412L
    //spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i"))).write.saveAsTable("t4")


    spark.read.json("src/main/resources/people.json").write.saveAsTable("framePeople")
    spark.read.json("src/main/resources/job.json").write.saveAsTable("frameJob")
    spark.read.json("src/main/resources/nation.json").write.saveAsTable("frameNation")

    tableNames.foreach { name =>
      spark.read.parquet(s"$dataLocation/$name").write.saveAsTable(s"frame$name")
      spark.sql(s"ANALYZE TABLE frame$name COMPUTE STATISTICS")
    }
    /*
    spark.read.parquet("/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/store_sales")
      .write.saveAsTable("frameStoreSales")
    spark.read.parquet("/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/store_returns")
      .write.saveAsTable("frameStoreReturns")
    spark.read.parquet("/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/catalog_sales")
      .write.saveAsTable("frameCatalogSales")
    spark.read.parquet("/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/catalog_returns")
      .write.saveAsTable("frameCatalogReturns")


    val tableNames1 = Seq("t1", "t2", "t4", "tiny",
      "framePeople","frameJob","frameNation",
      "frameStoreSales","frameStoreReturns","frameCatalogSales","frameCatalogReturns")
    tableNames1.foreach { t =>
      spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS")
    }

    */

    val t1 = spark.table("t1")
    val t2 = spark.table("t2")
    val tiny = spark.table("tiny")
    val t4= spark.table("t4")

    val framePeople = spark.table("framePeople")
    val frameJob = spark.table("frameJob")
    val frameNation = spark.table("frameNation")

    val Store_sales = spark.table("framestore_sales")
    val Store_returns = spark.table("framestore_returns")
    val Catalog_sales = spark.table("framecatalog_sales")
    val Catalog_returns = spark.table("framecatalog_returns")

    /*
    val frameStoreSales = spark.table("frameStoreSales")
    val frameStoreReturns = spark.table("frameStoreReturns")
    val frameCatalogSales = spark.table("frameCatalogSales")
    val frameCatalogReturns = spark.table("frameCatalogReturns")
    */
    //Store_sales.show()
    //Store_returns.show()
    //Catalog_sales.show()

    // Queries can then join DataFrame data with data stored in Hive.
    // Example: Inner join with join condition
    val q = t4
      .join(t2, t2("id")===t4("stt"))
      .join(tiny, tiny("id")===t4("stt"))
    val q2 = framePeople
      .join(frameJob,frameJob("job_id")===framePeople("people_id"))
      .join(frameNation,frameNation("nation_id")===framePeople("people_id"))
    val q3 = Store_sales
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Catalog_returns,Catalog_sales("cs_item_sk")===Catalog_returns("cr_item_sk"))
    /*
    val q4 = frameStoreSales
      .join(frameStoreReturns,frameStoreSales("ss_item_sk")===frameStoreReturns("sr_item_sk"))
      .join(frameCatalogSales,frameStoreSales("ss_item_sk")===frameCatalogSales("cs_item_sk"))
      .join(frameCatalogReturns,frameCatalogSales("cs_item_sk")===frameCatalogReturns("cr_item_sk"))
    */
    val plan = q3.queryExecution.analyzed
    println(plan.numberedTreeString)
    val joinsReordered = OptimizeS.execute(plan)
    println(joinsReordered.numberedTreeString)

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

    // Queries can then join DataFrame data with data stored in Hive.
    // Example: Inner join with join condition
    //val queryString = fileToString(new File(s"$dataLocation/query25.sql"))
    //println(queryString)
    //val queryplan = spark.sql(queryString).queryExecution.analyzed
    val q0 = Store_sales
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Item,Store_sales("ss_item_sk")===Item("i_item_sk"))
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
    /*
    val q31 = Store_sales
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Catalog_returns,Store_sales("ss_item_sk")===Catalog_returns("cr_item_sk"))
      .join(Item,Item("i_item_sk")===Catalog_returns("cr_item_sk"))

    val q32 = Store_sales
      .join(Catalog_returns,Store_sales("ss_item_sk")===Catalog_returns("cr_item_sk"))
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))
      .join(Item,Item("i_item_sk")===Catalog_returns("cr_item_sk"))

    val q33 = Item
      .join(Catalog_returns,Item("i_item_sk")===Catalog_returns("cr_item_sk"))
      .join(Store_sales,Store_sales("ss_item_sk")===Catalog_returns("cr_item_sk"))
      .join(Store_returns,Store_sales("ss_item_sk")===Store_returns("sr_item_sk"))

    val q41 = Store_sales
      .join(Catalog_sales,Store_sales("ss_item_sk")===Catalog_sales("cs_item_sk"))
      .join(Catalog_returns,Catalog_sales("cs_item_sk")===Catalog_returns("cr_item_sk"))
      .join(Item,Item("i_item_sk")===Catalog_sales("cs_item_sk"))
  */
    //q1: catalog_sales-store_returns-catalog_returns-store_sales
    val Q = Seq(q0)//q11,q21,q12,q22,q23)//,q32, q31,q41,q33
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
    val folder = listRelations.toString().replace("Set","").replace(" ","").replace(",","_").replace("(","").replace(")","")
    println("List of tables in the query: q" + randomInt)
    listRelations.foreach(relation=>println(relation))
    historicData.setupFolder(folder,"")
    historicData.storeIdQuery(folder)
    val confSQL = spark.sessionState.conf
    confSQL.setConfString("idQuery",folder)
    println("---------------" )
    //val listTables = q.queryExecution.listTables("default")
    //listTables.foreach(name=>println(name.table))
    //val plan = q.queryExecution.logical
    val sparkQ = q
    //val sparkQ = spark.sql(queryString)
    val plan = sparkQ.queryExecution.logical
    println("The original logical plan -----------------------------------------------")
    println(plan.numberedTreeString)
    println("-------------------------------------------------------------------------")
    println("The optimized plan of Spark-----------------------------------------------")
    val optimizedPlan = sparkQ.queryExecution.optimizedPlan
    println(optimizedPlan.numberedTreeString)
    println("--------------------------------------------------------------------------")
    val joinsReordered = OptimizeS.execute(plan)

    val plans = Pareto.setLogicalPlans()
    val costs = Pareto.setCosts()
    val sets = Pareto.setList()
    for (i <- 0 until sets.size()){
      val folder_home = "data/dream/" + folder + "/" + sets.get(i).toString()
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
      /*
      if (CsvFileReader.count(file) == 0) {

      }
      */
    }
    val ran = r.nextInt(plans.size())
      println("")
      println("Begin running logical plan " + ran + " in Pareto plan set with Pareto.size: "+plans.size())
      val startTime = System.nanoTime()
      val runPlan = plans.get(ran)
      val costPlan = costs.get(ran)
      val setPlan = sets.get(ran)
      val nameValue = "executeTime"
    /*
    val folder_home = "data/dream/" + folder + "/" + setPlan.toString()
    val nameValue = "executeTime"
    val file = folder_home + "/" + nameValue + ".csv"
    val numberVariables = 2
    val filePath = Paths.get(file)
    if (!Files.exists(filePath)) try
      Files.createFile(filePath)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    if (CsvFileReader.count(file) == 0) {
      historicData.setupFile(file, numberVariables)
    }
    */

    val card = costPlan.card.toDouble// + r.nextInt(100)*costPlan.card.toDouble/1000
    val size = costPlan.size.toDouble// + r.nextInt(100)*costPlan.size.toDouble/1000

    val estimateValue = historicData.estimateAndStore(folder,setPlan.toString(), nameValue, card, size)
    val fileExecute = "data/dream/" + folder + "/" + setPlan.toString() + "/executeTime.csv"
    println(fileExecute)
    val estimateValueMOEA = LinearRegressionManual.guessValue(fileExecute,fileExecute, card, size)
    println("The predict Value of Dream is: " + estimateValue)
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
    historicData.updateValue(folder,setPlan.toString(),costPlan,durationInMs.toDouble,"executeTime")
    historicData.saveError(folder,setPlan.toString(),nameValue, durationInMs.toDouble, estimateValue, 0.8)
    val WEKA = "executeTimeWEKA"
    historicData.saveError(folder,setPlan.toString(), WEKA, durationInMs.toDouble, estimateValueMOEA, 0.8)
    //println(joinsReordered.numberedTreeString)
    /*
    var logicalQuery: LogicalPlan = plan
    println("Begin running physical plan")
    val physicalPlan = spark.sql(queryString).queryExecution.sparkPlan
    for (physicalP <-physicalPlan){
      println(physicalP.numberedTreeString)
    }
    println("End of running physical plan")
    */
    //println("Testing new")
    //logicalQuery = EliminateSubqueryAliases.apply(logicalQuery)
    //println("logicalQuery : " + logicalQuery)
    //println(spark.sessionState.executePlan(logicalQuery).toString())
    //val data: DataFrame = Dataset.apply(spark, logicalQuery)
    //data.show()

  }

  /*
  import org.apache.spark.sql.SparkSession
  def apply(Sqlctx: SparkSession, Plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(Sqlctx, Plan)
  }
  */

  def testQuery():Unit={
    //val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tpcdsQueries = Seq("query25")
    val queries = tpcdsQueries
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
    val dataLocation = "/Volumes/DATAHD/Downloads/spark-tpc-ds-performance-test-master/spark-warehouse/tpcds.db/"
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    //val tableSizes = setupTables(dataLocation)

    tableNames.foreach { name =>
      spark.read.parquet(s"$dataLocation/$name").write.saveAsTable(s"frame$name")
      spark.sql(s"ANALYZE TABLE frame$name COMPUTE STATISTICS")
    }
    queries.foreach { name =>
      //val queryString = fileToString(new File(Thread.currentThread().getContextClassLoader
      //  .getResource(s"tpcds/$name.sql").getFile))
      val queryString = fileToString(new File(s"$dataLocation/$name.sql"))
      //println(queryString)

      val Store_sales = spark.table("framestore_sales")
      val Store_returns = spark.table("framestore_returns")
      val Catalog_sales = spark.table("framecatalog_sales")
      val Catalog_returns = spark.table("framecatalog_returns")

      val q = Store_sales
        .join(Store_returns, Store_sales("ss_item_sk") === Store_returns("sr_item_sk"))
        .join(Catalog_sales, Store_sales("ss_item_sk") === Catalog_sales("cs_item_sk"))
        .join(Catalog_returns, Catalog_sales("cs_item_sk") === Catalog_returns("cr_item_sk"))

      // Queries can then join DataFrame data with data stored in Hive.
      // Example: Inner join with join condition
      //var queryString2 = "select * " +
        //"from " +
        //"framestore_sales, " +
        //"framestore_returns " +
        //"framecatalog_sales, " +
        //"framecatalog_returns " +
        //"where " +
        //"ss_item_sk = sr_item_sk"
        //" and " +
        //"ss_item_sk = cs_item_sk and " +
        //"cs_item_sk = cr_item_sk "
      //val plan = spark.sql(queryString2).queryExecution.analyzed
      val plan = q.queryExecution.analyzed
      //q.show()

      //val df = sqlContext.sql(queryString2)
      //val plan = df.queryExecution.analyzed
      //println(plan.numberedTreeString)
      val joinsReordered = OptimizeS.execute(plan)
      //println(joinsReordered.numberedTreeString)
    }
  }
}

