/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

//import org.assertj.core.api.Assertions.*;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.catalyst.CatalystConf;

/**
 *
 * @author letrung
 */
public class TestCatalys {
    static String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    private static final SparkSession SESSION = SparkSession.builder() 
  .master("local[1]")
  .config("spark.ui.enabled", "false")
  .config("spark.eventLog.enabled", "true")
  .config("spark.eventLog.dir", warehouseLocation)
  .appName("CatalystOptimizer Test").getOrCreate();
 

    public static void should_get_dataframe_from_database() {   
  // categories as 18 entries
//    Dataset<Row> dataset = getBaseDataset("database_hive_postgres");
 
//    Dataset<Row> filteredDataset = dataset.where("LENGTH(name) > 5")
//            .where("name != 'mushrooms'")
//            .limit(3);
 
  // To see logical plan, filteredDataset.logicalPlan() methods can be used,
  // such as: treeString(true), asCode()
  // To see full execution details, fileteredDataset.explain(true)
  // should be called
  //assertThat(filteredDataset.count()).isEqualTo(3);
}
/* 
public static Dataset<Row> getBaseDataset(String dbTable) {
  // Please note that previous query won't generate real SQL query. It will only
  // check if specified column exists. It can be observed with RDBMS query logs.
  // For the case of MySQL, below query is generated:
  // SELECT * FROM meals WHERE 1=0
  // Only the action (as filteredDataset.show()) will execute the query on database.
  // It also can be checked with query logs.
  String username = System.getProperty("user.name");
    String HOME=System.getenv().get("HOME");
    String FILENAME = HOME + "/Documents/password.txt";
    String password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME);
   
  return SESSION.read()
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
//    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "database_hive_postgres")
    .option("user", username)
    .option("password", password)
    .load();
*/
/*  return SESSION.read() // Backup origin
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/fooder")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", dbTable)
    .option("user", "root")
    .option("password", "")
    .load();
*/  
//}
}
