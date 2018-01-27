/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.HiveContext

case class Person(name: String, age: Long){}
class SimpleApp {
//System.getProperty("user.name")
  val HOME = System.getenv().get("HOME")
  val FILEUSER = HOME + "/username.txt"
  val username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER)

  val FILENAME = HOME + "/password.txt"
  val password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME)
  val query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4")
  
  
  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left,right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] < 1.0 =>
        println("optimization of one applied")
        left
    }
  }
  def main() {
  //def main(args: Array[String]) {
    println("\n Hello world")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
//    val textFile = spark.read.textFile("hdfs://master:9000/user/hive/warehouse/people.txt")
//    println("\n The number of word in file is:=" + textFile.count)
    first()
    //runBasicDataFrameExample(spark)
    //runDatasetCreationExample(spark)
    //UntypedUserDefinedAggregate(spark)
    //runInferSchemaExample(spark)
    //runProgrammaticSchemaExample(spark) // can not run in this example
    spark.stop()
    println("\n Goodbye")
  }
  def first(){
    val conf = new SparkConf()
            .setAppName("jdf-dt-rtoc-withSQL")
            .set("spark.driver.allowMultipleContexts", "true")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("hive.metastore.uris", "thrift://master:9083")
            .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .setMaster("local[*]")
    val sc = new JavaSparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val data_hive = sqlContext.table("tpch100m.orders")
    val dbHive = "orders"
    data_hive.createOrReplaceTempView(dbHive)
    data_hive.show()

    val dbTablePostgres = "lineitem"

    val spark = SparkSession
        .builder()
        .appName("Spark Postgres Example")
        .master("local[*]")
        .getOrCreate();
//    val dataDF_postgres = spark.read.jdbc("jdbc:postgresql://localhost:5432", "tpch100m.lineitem")
//        .option("user", username)
//        .option("password", password)
//        .load()
    val dataDF_postgres = sqlContext.read.format("jdbc").options(
        Map("url" -> "jdbc:postgresql:tpch100m",
        "dbtable" -> "lineitem",
        "user" -> username,
        "password" -> password)).load()
    dataDF_postgres.createOrReplaceTempView("lineitem");
    dataDF_postgres.show();    

    val query = spark.sql(query4)//"select * from orders,lineitem where l_orderkey = o_orderkey")
    println("--------------------sparkPlan--------------------------------")
    println(query.queryExecution.sparkPlan)
    println("--------------------show()-----------------------------------")
    query.show()
    println("---------------------queryExecution--------------------------")
    println(query.queryExecution)
    println("---------------------optimizedPlan.numberedTreeString--------")
    println(query.queryExecution.optimizedPlan.numberedTreeString)
    
    //Second(spark)
    
    
    sc.stop()
    sc.close()
    spark.stop()
  }
  private def Second(spark: SparkSession): Unit = {
    val df = spark.sql(query4)//"select * from orders,lineitem where l_orderkey = o_orderkey")
    //add our custom optimization
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("order_count")
    println("after optimization")
    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
  }  
  private def firstExample(): Unit = {
//    SparkConf conf = SparkConf.setAppName("jdf-dt-rtoc-withSQL").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
//    JavaSparkContext sc = new JavaSparkContext(conf)
//    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://master:9083")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS tpch100m.order_lineitem AS (SELECT * FROM tpch100m.orders,tpch100m.lineitem WHERE l_orderkey = o_orderkey)").show()
    val frame = Seq(("one", 1), ("two", 2), ("three", 3)).toDF("word", "count")
        // see the frame created
    frame.show()
        /**
         * +-----+-----+
         * | word|count|
         * +-----+-----+
         * |  one|    1|
         * |  two|    2|
         * |three|    3|
         * +-----+-----+
         */
        // write the frame
    frame.write.mode("overwrite").saveAsTable("t4")    
    //sql("CREATE TABLE IF NOT EXISTS ABC AS (SELECT * FROM DUNGBINH)").show()
    //sql.show()
    spark.stop()
  }
  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("hdfs://master:9000/user/hive/warehouse/people.json")
  //  val df = spark.read.json("examples/src/main/resources/people.json")
    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)
    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+
    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
  }
  
  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    
    import spark.implicits._
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    //val path = "/user/hive/warehouse/people.json"
    val path = "examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
  }
  private def runInferSchemaExample(spark: SparkSession): Unit = {
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
//      .textFile("/user/hive/warehouse/people.txt")
      .textFile("examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
  }
/*  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import org.apache.spark.sql.types._

    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("/user/hive/warehouse/people.txt")
//    val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
  }
*/  private def UntypedUserDefinedAggregate(spark: SparkSession): Unit = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
    import org.apache.spark.sql.types._

    object MyAverage extends UserDefinedAggregateFunction {
      // Data types of input arguments of this aggregate function
      def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
      // Data types of values in the aggregation buffer
      def bufferSchema: StructType = {
        StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
      }
      // The data type of the returned value
      def dataType: DataType = DoubleType
      // Whether this function always returns the same output on the identical input
      def deterministic: Boolean = true
      // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
      // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
      // the opportunity to update its values. Note that arrays and maps inside the buffer are still
      // immutable.
      def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0L
      }
      // Updates the given aggregation buffer `buffer` with new input data from `input`
      def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) {
          buffer(0) = buffer.getLong(0) + input.getLong(0)
          buffer(1) = buffer.getLong(1) + 1
        }
      }
      // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
      def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
      }
      // Calculates the final result
      def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
    }

    // Register the function to access it
    spark.udf.register("myAverage", MyAverage)

    //val df = spark.read.json("/user/hive/warehouse/employees.json")
    val df = spark.read.json("examples/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
  }
}
