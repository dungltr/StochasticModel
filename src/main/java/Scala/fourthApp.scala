package Scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class fourthApp {
  val HOME = System.getenv().get("HOME")
  val FILEUSER = HOME + "/username.txt"
  val username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER)

  val FILENAME = HOME + "/password.txt"
  val password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME)
  val query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4")
def main(){
  test(); 
}
  def desktopLocation = "hdfs://master:9000/user/hive/warehouse"
  def laptopLocation = "hdfs://localhost:9000/Volumes/DATAHD/user/hive/warehouse"
  def desktopThrift = "thrift://master:9083"
  def laptopThrift = "thrift://localhost:9083"
  def test(){
  println("\n Hello from val conf = new SparkConf()")
    ///////////////////////////////////////////////////////////////////////
    val hiveLocation   = laptopLocation
    val conf = new SparkConf()
      .setAppName("SOME APP NAME")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.warehouse.dir",hiveLocation)
      
    val thrift = laptopThrift
    println("Start of val spark = SparkSession------------")
    val spark = SparkSession
      .builder()
      .appName("SparkHiveExample")
      .master("local[*]")
      .config("hive.metastore.uris", thrift)
      //.config("spark.sql.warehouse.dir", hiveLocation)
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    println("Start of val sc = new SparkContext(conf)--------------------")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val data_hive = sqlContext.table("orders")
    //data_hive.createOrReplaceTempView("orders")
    sc.stop();
    spark.stop();
  }
  def test2(){
    println("\n Hello world")
  val spark = SparkSession
    .builder()
    .appName("Spark Postgres Example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()
  val query = "select o_orderpriority, count(*) as order_count from orders as o where o_orderdate >= '1996-05-01' and o_orderdate < '1996-08-01' and exists (select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority" 
  val SQL = spark.sql(query)//Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  //SQL.queryExecution.analyzed
  println(query.toString())
  spark.stop()  
  println("\n Goodbye world") 
  }
}
