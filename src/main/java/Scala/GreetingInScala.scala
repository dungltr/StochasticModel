/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala
import org.apache.spark.sql.SparkSession

class GreetingInScala {
    val HOME = System.getenv().get("HOME")
    val FILEUSER = HOME + "/username.txt"
    val username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER)

    val FILENAME = HOME + "/password.txt"
    val password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME)
    val query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4")
    def greet() {
        val delegate = new GreetingInJava
        delegate.greet()
    }
    def desktopLocation = "hdfs://master:9000/user/hive/warehouse"
    def laptopLocation = "hdfs://localhost:9000/Volumes/DATAHD/user/hive/warehouse"
    def desktopThrift = "thrift://master:9083"
    def laptopThrift = "thrift://localhost:9083"
    def tableExists(table: String, spark: SparkSession) = spark.catalog.tableExists(table)
    def databaseExists(database: String, spark: SparkSession) = spark.catalog.databaseExists(database)
    def main(): Unit ={
        first()
    }
    def first(): Unit ={
        val HOME = System.getenv().get("HOME")
        val FILEUSER = HOME + "/username.txt"
        val username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER)

        val FILENAME = HOME + "/password.txt"
        val password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME)
        val query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4")
        println("\n Hello world")
        val hiveLocation   = desktopLocation
        /*
        val conf = new SparkConf()
          .setAppName("SOME APP NAME")
          .setMaster("local[*]")
          .set("spark.driver.allowMultipleContexts", "true")
          .set("spark.sql.warehouse.dir",hiveLocation)
        */
        //val sc = new SparkContext(conf)
        val thrift = desktopThrift
        /*val spark = SparkSession
          .builder()
          .appName("SparkHiveExample")
          .master("local[*]")
          .config("hive.metastore.uris", thrift)
          //.config("spark.sql.warehouse.dir", hiveLocation)
          .config("spark.driver.allowMultipleContexts", "true")
          .enableHiveSupport()
          .getOrCreate()
        */
        println("Start of SQL Session--------------------")
        //val sqlContext = new HiveContext(sc)
        //val data_hive = sqlContext.table("tpch100m.orders")
        //val dbHive = "orders"
        //data_hive.createOrReplaceTempView(dbHive)
        //data_hive.show()
        //spark.stop()

        println("\n Goodbye")
    }
}