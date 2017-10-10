/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;
import java.io.File;
import java.io.Serializable;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.hive.HiveContext;
//import org.apache.derby.jdbc.EmbeddedDriver;
/**
 *
 * @author letrung
 */
public class TestJava {
    public static class Person implements Serializable {
    private String name;
    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }
    public static void main (){
    System.out.println("\n Hello from Java");
/*    SparkSession spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .getOrCreate();
    Dataset<Row> jsonFile = spark.read().format("json").load("hdfs://master:9000/user/hive/warehouse/people.json");
    System.out.println("\n The number of word in file is:=" + jsonFile.count());
*/    //////////////////////////////////////////////////////////////
//    OriginalExample(spark);
//    FirstExample();
//    CustomExample(spark);
      /////////////////////////////////////////////////////////////
//    spark.stop();
    System.out.println("\n Goodbye");
    }
    public static void OriginalExample(SparkSession spark) {
    Dataset<Row> sales = spark.read().option("header","true").csv("src/main/resources/sales.csv");
    sales.createOrReplaceTempView("sales");
    Dataset<Row> customers = spark.read().option("header","true").csv("src/main/resources/customers.csv");
    customers.createOrReplaceTempView("customers");
    //val multipliedDF = df.selectExpr("amountPaid * 1")
//    Dataset<Row> SalesJoinCustomers = sales.join(customers, sales.col("customerId")=customers.col("customerId"));
            //sales.join(customers, sales("customerID") === customers("customerID"));
//            sales.
//    println(SalesJoinCustomers)
//    Dataset<Row> WhereCustomersID = SalesJoinCustomers.where(sales("customerID")<3);
//    Dataset<Row> GroupBy = WhereCustomersID.groupBy(sales("customerID"), customers("customerID"));
    
    Dataset<Row> query = spark.sql("select * from sales, customers where sales.customerId = customers.customerId and sales.customerId < 3");
    query.show();
    System.out.println(query.queryExecution().optimizedPlan().numberedTreeString());
//    System.out.println(GroupBy.toString);
/*    //add our custom optimization
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")
    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
*/  }
    public static void FirstExample() {
    String HOME=System.getenv().get("HOME");
    String FILEUSER = HOME + "/username.txt";
    String username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER);
    String FILENAME = HOME + "/password.txt";
    String password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME);
/*    SparkSession spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://master:9083")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();
//    import spark.implicits._
//    import spark.sql
    Dataset<Row> query = spark.sql("CREATE TABLE IF NOT EXISTS tpch100m.order_lineitem AS (SELECT * FROM tpch100m.orders,tpch100m.lineitem WHERE l_orderkey = o_orderkey)");
    //Dataset<Row> frame = (("one", 1), ("two", 2), ("three", 3)).toDF("word", "count");
        // see the frame created
    //frame.show()
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
    //frame.write.mode("overwrite").saveAsTable("t4")    
    //sql("CREATE TABLE IF NOT EXISTS ABC AS (SELECT * FROM DUNGBINH)").show()
    //sql.show()
//    query.show();
//    spark.stop();

    SparkConf conf = new SparkConf()
            .setAppName("jdf-dt-rtoc-withSQL")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("hive.metastore.uris", "thrift://master:9083")
            .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    SQLContext sqlContext = new HiveContext(sc); // The error occurred.
    Dataset<Row> data_hive = sqlContext.table("tpch100m.orders");
    data_hive.createOrReplaceTempView("orders");
    
//            String[] listTable = sqlContext.tableNames("mydb");
//    System.out.println(listTable[0]);  

    data_hive.show();
    
    SparkSession spark = SparkSession
        .builder()
        .appName("Spark Postgres Example")
        .master("local[*]")
        .getOrCreate();
    Dataset<Row> dataDF_postgres = spark.read()
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/tpch100m")
        .option("dbtable", "lineitem")
        .option("user", username)
        .option("password", password)
        .load();
    dataDF_postgres.createOrReplaceTempView("lineitem");
    dataDF_postgres.show();
    
    
    Dataset<Row> query = spark.sql("select * from orders,lineitem where l_orderkey = o_orderkey");
    query.show();
    System.out.println(query.queryExecution().optimizedPlan().numberedTreeString());
        
//    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule);
    sc.stop();
    sc.close();
    spark.stop();

  
    
//    Dataset<Row> customers = spark.read().option("header","true").csv("src/main/resources/customers.csv");
//    customers.createOrReplaceTempView("customers");
    //val multipliedDF = df.selectExpr("amountPaid * 1")
//    Dataset<Row> SalesJoinCustomers = sales.join(customers, sales.col("customerId")=customers.col("customerId"));
            //sales.join(customers, sales("customerID") === customers("customerID"));
//            sales.
//    println(SalesJoinCustomers)
//    Dataset<Row> WhereCustomersID = SalesJoinCustomers.where(sales("customerID")<3);
//    Dataset<Row> GroupBy = WhereCustomersID.groupBy(sales("customerID"), customers("customerID"));
    
    
//    System.out.println(query.queryExecution().optimizedPlan().numberedTreeString());
//    System.out.println(GroupBy.toString);
/*    //add our custom optimization
    spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")
    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
*/  }
}
