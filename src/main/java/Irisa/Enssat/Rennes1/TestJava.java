/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

import Scala.SecondScala;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.internal.SQLConf;

import java.io.Serializable;

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
    static String HOME = System.getenv().get("HOME");
    static String FILEUSER = HOME + "/username.txt";
    static String username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER);

    static String FILENAME = HOME + "/password.txt";
    static String password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME);
    static String query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4");
    
    public static void main (String [] arg) throws ParseException{
    System.out.println("\n Hello from Java");
    /*
    SparkSession spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.driver.allowMultipleContexts", "true")
        .getOrCreate();
    */
    //Dataset<Row> jsonFile = spark.read().format("json").load("hdfs://localhost:9000//Volumes/DATAHD/user/hive/warehouse/people.txt");
    //System.out.println("\n The number of word in file is:=" + jsonFile.count());
    //////////////////////////////////////////////////////////////
    //OriginalExample(spark);
    //FirstExample();
    lastExample();
//    CustomExample(spark);
      /////////////////////////////////////////////////////////////
    //spark.stop();
    System.out.println("\n Goodbye");
    }
    public static void OriginalExample(SparkSession spark) throws ParseException {
      String orders = "src/main/resources/nation.tbl";
      String lineitem = "src/main/resources/region.tbl";
      Dataset<Row> ordersDataset = spark.read().option("header","true").csv(orders);
      Dataset<Row> lineitemDataset = spark.read().option("header","true").csv(lineitem);
      ordersDataset.createOrReplaceTempView("orders");
      lineitemDataset.createOrReplaceTempView("lineitem");

    Dataset<Row> sales = spark.read().option("header","true").csv("src/main/resources/sales.csv");
    sales.createOrReplaceTempView("sales");
    Dataset<Row> customers = spark.read().option("header","true").csv("src/main/resources/customers.csv");
    customers.createOrReplaceTempView("customers");

    spark.sessionState().conf().cboEnabled();
    //val multipliedDF = df.selectExpr("amountPaid * 1")
    //Dataset<Row> SalesJoinCustomers = sales.join(customers, sales.col("customerId"));//===customers.col("customerId"));
        
    //sales.join(customers, sales.col("customerID"));
//            sales.
    //System.out.println(SalesJoinCustomers);
    //Dataset<Row> WhereCustomersID = SalesJoinCustomers.where("customerID < 3");
    //Dataset<Row> GroupBy = WhereCustomersID.groupBy(sales.col("customerID"), customers.col("customerID"));
      SQLConf conf = new SQLConf();
      spark.sessionState().conf().setConf(conf.CBO_ENABLED(), true);
      spark.sessionState().conf().setConf(conf.JOIN_REORDER_ENABLED(), true);
      System.out.println(spark.sessionState().conf().cboEnabled());

      String sql1 = "select * from orders";
      Dataset<Row> query1 = spark.sql(sql1);
      query1.show();

      String sql2 = "select * from lineitem";
      Dataset<Row> query2 = spark.sql(sql2);
      query2.show();

    String sqlTxt = "select * from sales, customers where sales.customerId = customers.customerId and sales.customerId < 3";

    Dataset<Row> query = spark.sql(sqlTxt);
    //spark.catalog().listDatabases().select("customerId").show();
    LogicalPlan test = spark.sessionState().sqlParser().parsePlan(sqlTxt);
    System.out.println("-----------Show text of sqlParser--------------------------------");
    System.out.println(""+test);
    System.out.println("-----------Show test.computeStats().toString()--------------------------------");
    //test.computeStats(spark).toString();
    System.out.println("-----------query.show()--------------------------------");
    query.show();
    query.explain(true);
    System.out.println("calling Second Scala--------------------------------");
    SecondScala scala = new SecondScala();
    scala.OriginalExample(spark);
    //scala.CustomExample(spark);
    System.out.println("ending Second Scala--------------------------------");
    /*
    System.out.println("-----------This is the query.explain()--------------------------------");
    System.out.println("");
    query.explain();
      System.out.println("---------This is the executedPlan ----------------------------------");
      System.out.println("\n"+query.queryExecution().executedPlan());

      System.out.println("---------query.queryExecution().logical()----------------------------------");
      System.out.println(" \n"+query.queryExecution().logical());
      System.out.println("---------This is the sparkPlan----------------------------------");
      System.out.println(" \n"+query.queryExecution().sparkPlan());
      System.out.println("---------This is the optimizedPlan----------------------------------");
      System.out.println(" \n"+query.queryExecution().optimizedPlan().numberedTreeString());
      System.out.println("---------This is the query.queryExecution().logical().children()----------------------------------");
      System.out.println("\n" + query.queryExecution().logical().children());
      System.out.println("---------This is the query.queryExecution().sparkPlan().children()----------------------------------");
      System.out.println(" \n"+query.queryExecution().sparkPlan().children());
      System.out.println("-------------------------------------------");
      System.out.println(";");
      System.out.println("---------This is the query.explain()----------------------------------");
      query.explain();

      System.out.println("---------query.logicalPlan()----------------------------------");
      System.out.println(" \n"+query.logicalPlan());
      System.out.println("---------query.queryExecution().analyzed()----------------------------------");
      System.out.println(query.queryExecution().analyzed());
      System.out.println("---------query.queryExecution().optimizedPlan().stats(conf)----------------------------------");
      System.out.println(query.queryExecution().optimizedPlan().stats(conf));
  */

  }
    public static void FirstExample() {
      System.out.println("\n Hello from First in Java");
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
            .set("hive.metastore.uris", "thrift://localhost:9083")
            .set("spark.driver.allowMultipleContexts", "true")
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
    public static void lastExample(){
        SparkSession spark = SparkSession
            .builder()
            .appName("Spark Postgres Example")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.allowMultipleContexts", "true")
            .config("hive.metastore.uris", "thrift://master:9083")    
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")              
            .enableHiveSupport()
            .getOrCreate();
        
        SparkConf conf = new SparkConf()
            .setAppName("jdf-dt-rtoc-withSQL")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("hive.metastore.uris", "thrift://master:9083")
            .set("spark.driver.allowMultipleContexts", "true")
            .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext; // The error occurred.
        sqlContext = new HiveContext(sc);
        
        Dataset<Row> data_hive = sqlContext.table("tpch100m.orders");
        String dbHive = "orders";
        data_hive.createOrReplaceTempView(dbHive);
        data_hive.show();
    
    //val dbTablePostgres = "lineitem"
    

        
//    val dataDF_postgres = spark.read.jdbc("jdbc:postgresql://localhost:5432", "tpch100m.lineitem")
//        .option("user", username)
//        .option("password", password)
//        .load()
    
        
        
        Dataset<Row> dataDF_postgres = sqlContext.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:tpch100m")
                .option("dbtable", "lineitem")
                .option("user", username)
                .option("password", password)
                .load();
    dataDF_postgres.createOrReplaceTempView("lineitem");
    dataDF_postgres.show();    
    
    
    Dataset<Row> query = spark.sql(query4);//Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
    System.out.println(query.toString());
    // "select * from orders,lineitem where l_orderkey = o_orderkey")
    
    System.out.println("--------------------sparkPlan--------------------------------");
    System.out.println(query.queryExecution().sparkPlan());
    System.out.println("--------------------show()-----------------------------------");
    query.show();
    System.out.println("---------------------queryExecution--------------------------");
    System.out.println(query.queryExecution());
    System.out.println("---------------------optimizedPlan.numberedTreeString--------");
    System.out.println(query.queryExecution().optimizedPlan().numberedTreeString());
    System.out.println("---------------------query.queryExecution.analyzed--------");
    System.out.println(query.queryExecution().analyzed());
    System.out.println("---------------------query.explain(true)--------");
    query.explain(true);
    System.out.println("---------------------query.queryExecution.executedPlan--------");
    System.out.println(query.queryExecution().executedPlan());
    
    
    //Second(spark)
    
    
    sc.stop();
    sc.close();
    spark.stop();
    }
}
