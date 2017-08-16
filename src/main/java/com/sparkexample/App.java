package com.sparkexample;
import static com.google.common.base.Preconditions.checkArgument;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.NodeName;
import gr.ntua.cslab.asap.operators.Operator;
import static gr.ntua.cslab.asap.staticLibraries.DatasetLibrary.add;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.Workflow;
import gr.ntua.cslab.asap.workflow.WorkflowNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;

//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.time.LocalDate;
//import java.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;




/**
 * Hello world!
 *
 */
public class App 
{   String SPARK_HOME=readhome("SPARK_HOME");
    String HADOOP_HOME=readhome("HADOOP_HOME");
    String HIVE_HOME=readhome("HIVE_HOME");
    String IRES_HOME=readhome("IRES_HOME");
    String node_pc = readhome("NODE_PC");
    /**
   * We use a logger to print the output. Sl4j is a common library which works with log4j, the
   * logging system used by Apache Spark.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  /**
   * This is the entry point function when the task is called with spark-submit.sh from command
   * line. In our example we will call the task from a WordCountTest instead.
   * See {@see http://spark.apache.org/docs/latest/submitting-applications.html}
     * @param args
   */
  
    public void main( String[] args ) throws Exception
    {
//      
//        Console console = System.console();
//        String input = console.readLine("Enter input:");
        
//        checkArgument(args.length > 1, "file:///Users/letrung/Dropbox/JavaWorkSpace/TestJavaSpark/src/test/resources/loremipsum.txt");
//        new App().run(args[0]);
//        new App().run(input);
           
        System.out.println( "Finish counting word!" );
        System.out.println( "Begin SQL" );
//        App.LearningSQL();
        System.out.println( "Finish SQL" );
        
//        checkArgument(args.length > 1, "file:///Users/letrung/Dropbox/JavaWorkSpace/TestJavaSpark/src/test/resources/loremipsum.txt");
//        TestOperators Test = new TestOperators("T","localhost");
        
    }
    public void runningSpark() {
        System.out.println( "Finish counting word!" );
        System.out.println( "Begin SQL" );
//        App.LearningSQL();
        System.out.println( "Finish SQL" );
    }
    public String getComputerName() throws UnknownHostException
    {
    String computername=InetAddress.getLocalHost().getHostName();
    return computername;
    }
    public String readhome(String filename) {
       String sCurrentLine = "nothing";
       try (BufferedReader br = new BufferedReader(new FileReader(System.getenv().get("HOME")+"/"+filename+".txt"))) {
			while ((sCurrentLine = br.readLine()) != null) {
                                return sCurrentLine;
			}

		} catch (IOException e) {
			e.printStackTrace();                       
		}
       return sCurrentLine;
    }
    public void createasapproperties1(){
        
        String HOME=System.getenv().get("HOME");
        String Folder_asap_properties = IRES_HOME+"/asap-platform/asap-server/target/conf";
        String file_name_asap_properties = "asap.properties";
        String asapproperties = "# At least one option from server.{plain,ssl}.port must be provided!\n" +
"\n" +
"# unencrypted traffic port\n" +
"server.plain.port = 1323\n" +
"\n" +
"# SSL configurations\n" +
"# server.ssl.port = 8443\n" +
"# server.ssl.keystore.path = \n" +
"# server.ssl.keystore.password = \n" +
"\n" +
"asap.dir = asapLibrary\n" +
"asap.basicLuaConf = asapLibrary/BasicLuaConf.lua\n" +
"asap.hdfs_path = "+HADOOP_HOME+"/bin/hdfs\n" +
"asap.asap_path = "+IRES_HOME+"/asap-tools/bin/asap\n" +
"asap.server_home = "+IRES_HOME+"/asap-platform/asap-server/target";

        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(Folder_asap_properties+"/"+file_name_asap_properties), "utf-8"));
            writer.write(asapproperties);
        } 
        catch (IOException ex) {
                // report
        } 
        finally {
            try {writer.close();} 
            catch (Exception ex) {/*ignore*/}
        }
    }
    public void createasapproperties2(){
        
        String HOME=System.getenv().get("HOME");
        String Folder_asap_properties = IRES_HOME+"/asap-platform/asap-server/target/classes";
        String file_name_asap_properties = "asap.properties";
        String asapproperties = "# At least one option from server.{plain,ssl}.port must be provided!\n" +
"\n" +
"# unencrypted traffic port\n" +
"server.plain.port = 1323\n" +
"\n" +
"# SSL configurations\n" +
"# server.ssl.port = 8443\n" +
"# server.ssl.keystore.path = \n" +
"# server.ssl.keystore.password = \n" +
"\n" +
"asap.dir = asapLibrary\n" +
"asap.basicLuaConf = asapLibrary/BasicLuaConf.lua\n" +
"asap.hdfs_path = "+HADOOP_HOME+"/bin/hdfs\n" +
"asap.asap_path = "+IRES_HOME+"/asap-tools/bin/asap\n" +
"asap.server_home = "+IRES_HOME+"/asap-platform/asap-server/target";

        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(Folder_asap_properties+"/"+file_name_asap_properties), "utf-8"));
            writer.write(asapproperties);
        } 
        catch (IOException ex) {
                // report
        } 
        finally {
            try {writer.close();} 
            catch (Exception ex) {/*ignore*/}
        }
    }
    public void createReporter_config(){  
        String File_Reporter_config = "/etc/reporter_config.json";
        String Contain = "{\n" +
                        "   \"backend\": \"mongo\",\n" +
                        "   \"host\":\""+node_pc+"\"\n" +
                        "}";
        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(File_Reporter_config), "utf-8"));
            writer.write(Contain);
        } 
        catch (IOException ex) {
                // report
        } 
        finally {
            try {writer.close();} 
            catch (Exception ex) {/*ignore*/}
        }
    }
    public void creatbasicluaconf(){
        
        String HOME=System.getenv().get("HOME");
        String Folder_basicluaconf = IRES_HOME+"/asap-platform/asap-server/target/asapLibrary";
        String file_name_basicluaconf = "BasicLuaConf.lua";
        
        
        
        String BasicLuaConf = "IRES_HOME=\"/usr/local/Cellar/ires\"\n" +
"MASTER_JAR_LOCATION = IRES_HOME .. \"/lib/kitten-master-0.2.0-jar-with-dependencies.jar\"\n" +
"\n" +
"YARN_CLASSPATH=\"/usr/local/share/hadoop/etc/hadoop:/usr/local/share/hadoop/share/hadoop/common/lib/*:/usr/local/share/hadoop/share/hadoop/common/*:/usr/local/share/hadoop/share/hadoop/hdfs:/usr/local/share/hadoop/share/hadoop/hdfs/lib/*:/usr/local/share/hadoop/share/hadoop/hdfs/*:/usr/local/share/hadoop/share/hadoop/yarn/lib/*:/usr/local/share/hadoop/share/hadoop/yarn/*:/usr/local/share/hadoop/share/hadoop/mapreduce/lib/*:/usr/local/share/hadoop/share/hadoop/mapreduce/*:/usr/local/share/hadoop/contrib/capacity-scheduler/*.jar\"\n" +
"\n" +
"-- Resource and environment setup.\n" +
"base_resources = {\n" +
"  [\"master.jar\"] = { file = MASTER_JAR_LOCATION }\n" +
"}\n" +
"base_env = {\n" +
"  CLASSPATH = table.concat({\"${CLASSPATH}\", YARN_CLASSPATH, \"./master.jar\"}, \":\"),\n" +
"}\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"  name = \"Asap master\",\n" +
"  timeout = 1000000000,\n" +
"  memory = 1024,\n" +
"  cores = 1,\n" +
"  master = {\n" +
"    name = \"Asap master\",\n" +
"    env = base_env,\n" +
"    resources = base_resources,\n" +
"    command = {\n" +
"      base = \"${JAVA_HOME}/bin/java -Xms64m -Xmx128m com.cloudera.kitten.appmaster.ApplicationMaster\",\n" +
"      args = { \"-conf job.xml\" },\n" +
"    }\n" +
"  }\n" +
"}";
        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(Folder_basicluaconf+"/"+file_name_basicluaconf), "utf-8"));
            writer.write(BasicLuaConf);
        } 
        catch (IOException ex) {
                // report
        } 
        finally {
            try {writer.close();} 
            catch (Exception ex) {/*ignore*/}
        }
    }
    /**
   * The task body
     * @param inputFilePath
   */
  public void run(String inputFilePath) {
    /*
     * This is the address of the Spark cluster. We will call the task from WordCountTest and we
     * use a local standalone cluster. [*] means use all the cores available.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
     */
    String master = "local[*]";

    /*
     * Initialises a Spark context.
     */
    SparkConf conf = new SparkConf()
        .setAppName(App.class.getName())
        .setMaster(master);
    JavaSparkContext context = new JavaSparkContext(conf);

    /*
     * Performs a work count sequence of tasks and prints the output with a logger.
     */
/*    context.textFile(inputFilePath)
        .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .foreach(result -> LOGGER.info(
            String.format("Word [%s] count [%d].", result._1(), result._2)));
*/  }
    public void LearningSQL(){
        //Starting point
        SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate();
        

        /*Creating DataFrame***************************************************/
        Dataset<Row> df = spark.read().json("file:///Users/letrung/Dropbox/JavaWorkSpace/TestJavaSpark/src/test/resources/people.json");

        // Displays the content of the DataFrame to stdout
        df.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
/*Untyped Dataset Operations (aka DataFrame Operations)************************/        
        // Print the schema in a tree format
df.printSchema();
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
df.select("name").show();
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
//df.select(col("name"), col("age").plus(1)).show();
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
//df.filter(col("age").gt(21)).show();
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
//df.groupBy("age").count().show();
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+

/*// Running SQL**************************************************************
// Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people");

Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
sqlDF.show();
*/
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
/****Global Temporary View*****************************************************/
// Register the DataFrame as a global temporary view
/*
df.createGlobalTempView("people");

// Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

// Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
*/    
    }

    /**
     *
     * @param directory_library
     * @param NameOfWorkflow
     * @param name_host
     * @param int_localhost
     * @throws java.lang.Exception
     */
    

    

}

