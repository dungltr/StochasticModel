package Scala

import org.apache.spark.sql.SparkSession

class fourthApp {
  val HOME = System.getenv().get("HOME")
  val FILEUSER = HOME + "/username.txt"
  val username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER)

  val FILENAME = HOME + "/password.txt"
  val password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME)
  val query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4")
def main{
  println("\n Hello world")
  val spark = SparkSession
    .builder()
    .appName("Spark Postgres Example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()
  val query = "select o_orderpriority, count(*) as order_count from orders as o where o_orderdate >= '1996-05-01' and o_orderdate < '1996-08-01' and exists (select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority" 
  //val SQL = spark.sql(query)//Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  println(query.toString())
  spark.stop()  
  println("\n Goodbye world")  
}
}
