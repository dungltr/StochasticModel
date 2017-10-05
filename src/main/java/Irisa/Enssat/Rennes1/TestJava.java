/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;
import java.io.File;
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
    public static void main (){
    System.out.println("\n Hello world");
/*    SparkSession spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .getOrCreate();
    Dataset<Row> jsonFile = spark.read().format("json").load("hdfs://localhost:9000/user/hive/warehouse/people.json");
    System.out.println("\n The number of word in file is:=" + jsonFile.count());
*/    //////////////////////////////////////////////////////////////
//    OriginalExample(spark);
    //FirstExample();
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
    String username = System.getProperty("user.name");
    String HOME=System.getenv().get("HOME");
    String FILENAME = HOME + "/Documents/password.txt";
    String password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME);
    
    SparkConf conf = new SparkConf()
            .setAppName("jdf-dt-rtoc-withSQL")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    SQLContext sqlContext = new HiveContext(sc); // The error occurred.
//    Dataset<Row> data_hive = 
            String[] listTable = sqlContext.tableNames("mydb");
    System.out.println(listTable[0]);        
//    data_hive.show();
    sc.stop();
    sc.close();
    /*
    Dataset<Row> dataDF_postgres = spark.read()
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/mydb")
      .option("dbtable", "database_hive_postgres")
      .option("user", username)
      .option("password", password)
      .load();
    dataDF_postgres.createOrReplaceTempView("database_hive_postgres");
    Dataset<Row> query = spark.sql("select * from database_hive_postgres");
    query.show();
    */
    
/*    
    SparkSession spark_hive = SparkSession
        .builder()
        .appName("Java Spark SQL Hive basic example")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        .master("local")
        .getOrCreate();
    
    
    Dataset<Row> df2 = spark_hive.read()
//        .format("org.apache.derby.jdbc.EmbeddedDriver")
	.format("jdbc")
        .option("url", "jdbc:hive2://")
//	.option("database", "mydb")
        .option("dbtable", "dungbinh")
        .option("user", "APP")//username)
        .option("password", "mine")//password)
        .load();
    df2.createOrReplaceTempView("customer");

//    Dataset<Row> query2 = spark_hive.sql("select * from customer");
//    query2.show();
/*    
    DataFrameReader dataDF_hive = spark_hive.read()
      .format("jdbc")
Y      .option("url", "jdbc:derby://usr/local/share/hive/metastore_db");//hive2://localhost:10000/mydb")
//      .option("dbtable", "customer")
//      .option("user", "")
//      .option("password", "")
//      .load();
    Dataset<Row> df2 = dataDF_hive.table("mydb.customer");
    df2.createOrReplaceTempView("customer");
    Dataset<Row> query2 = spark.sql("select * from customer");
    query2.show();
/*    
    SparkConf conf = new SparkConf().setAppName("SparkHive Example");
    SparkContext sc = new SparkContext(conf);
    HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
    Dataset<Row> df = hiveContext.sql("show databases");
    df.show();
*/
    /*
    Properties connectionProperties = new Properties();
    connectionProperties.put("user", username);
    connectionProperties.put("password", password);
    Dataset<Row> jdbcDF2 = spark.read()
      .jdbc("jdbc:postgresql://localhost:5432/mydb", "database_hive_postgres", connectionProperties);
*/    
    
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
