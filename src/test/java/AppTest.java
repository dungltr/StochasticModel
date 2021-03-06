
import Algorithms.Algorithms;
import static Algorithms.Algorithms.estimateCostValue;
import static Algorithms.Algorithms.estimateSizeOfMatrix;
import Algorithms.CreateDatabase;
import Algorithms.LinearRegression;
import Algorithms.LinearRegressionManual;
import Algorithms.Writematrix2CSV;
import Algorithms.testScilab;
import Algorithms.testWriteMatrix2CSV;
import IRES.TPCHQuery;
import IRES.runWorkFlowIRES;
import IRES.testCreateDatabase;
import IRES.testQueryPlan;
import LibraryIres.Move;
import LibraryIres.Move_Data;
import LibraryIres.Move_IRES;
import LibraryIres.createWorkflow;
import LibraryIres.runWorkflow;
import Standalone.TPCHStandalone;
import Irisa.Enssat.Rennes1.TestScript;
import WriteReadData.CsvWriteReadTest;
import org.junit.Test;
import com.sparkexample.*;
import java.io.IOException;

import java.net.URISyntaxException;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;

import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.client.RestClient;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.examples.AddWorkflowFromDir;
import static gr.ntua.cslab.asap.examples.AddWorkflowFromDir.readFromDir;
import gr.ntua.cslab.asap.examples.TestOperators;
import gr.ntua.cslab.asap.examples.TestWorkflows;
import gr.ntua.cslab.asap.examples.TestWorkflowsIMR;

import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.NodeName;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.MaterializedWorkflow1;
import gr.ntua.cslab.asap.workflow.Workflow;
import gr.ntua.cslab.asap.workflow.WorkflowNode;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import static java.lang.System.in;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.time.LocalDate;
//import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.login.Configuration;

/**
 * Unit test for simple App.
 */
public class AppTest{ 
    static int int_localhost = 1323;
    static String name_host = "localhost";
    static String SPARK_HOME = new App().readhome("SPARK_HOME");
    static String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    static String HIVE_HOME = new App().readhome("HIVE_HOME");
    static String IRES_HOME = new App().readhome("IRES_HOME");
    static String ASAP_HOME = IRES_HOME;
    static String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    static String node_pc = new App().readhome("NODE_PC");
    @BeforeClass
    /**
    * Start up the asap-server before executing the unit tests.
    */
    public static void setup() throws InterruptedException, IOException {
        System.out.println("ASAP_HOME="+ASAP_HOME);
//        Runtime.getRuntime().exec(ASAP_HOME+"/asap-platform/asap-server/src/main/scripts/asap-server restart");

//        String[] cmd = new String[]{"/bin/sh", ASAP_HOME+"/start-ires.sh"};
//        TestScript testScript = new TestScript();
//        testScript.runScript("sh "+ASAP_HOME+"/asap-server-start.sh");//ASAP_HOME+"/asap-platform/asap-server/src/main/scripts/asap-server start");
//        Process pr = Runtime.getRuntime().exec(cmd);
//        while (pr.isAlive());
        Thread.sleep(1000);
    }	
    @AfterClass
    /**
    * Stop the asap-server when the unit tests have finished.
    */
    public static void tearDown() throws InterruptedException, InterruptedException, IOException {
//          Runtime.getRuntime().exec(ASAP_HOME+"/asap-platform/asap-server/src/main/scripts/asap-server stop");
//        TestScript testScript = new TestScript();
//        testScript.runScript("sh "+ASAP_HOME+"/asap-server-stop.sh");
//        String[] cmd = new String[]{"/bin/sh", ASAP_HOME+"/stop-ires.sh"};
//        Process pr = Runtime.getRuntime().exec(cmd);
//        while (pr.isAlive());
          Thread.sleep(1000);
    }
//    @Test
    public void createProperties(){
        new App().createasapproperties1();
        new App().createasapproperties2();
        new App().createReporter_config();
    }
    @Test
    public void testConvert_IRES() throws Exception {
        createProperties();
        String file_test = "/home/ubuntu/test.csv";
        String training_file = "/home/ubuntu/no_delayrealValue.csv";
        String testing_file = "/home/ubuntu/newValue.csv";
        LinearRegressionManual.testLinearRegression(file_test);
        LinearRegressionManual.main_test(training_file,testing_file);

    }
//    @Test
    public void testall() throws Exception{
        double TimeOfDay = 24.00*Math.random();
        int times = 0;
//        testCreateDatabase.testCreateHiveDataBase(TimeOfDay);
//        testCreateDatabase.testCreaePostgresDataBase(TimeOfDay);
        for (int i = 0; i < times; i++)
        { 
                    System.out.println("\n This is the "+i+"th round-------------------------------------------------------------------------------------------------------");

            TimeOfDay = 8.00*Math.random();          
//            testCreateDatabase.testCreateHiveDataBase(TimeOfDay);
//            testCreateDatabase.testCreatePostgresDataBase(TimeOfDay);
//            testQueryPlan.testQueryPlanIRES_Hive_Postgres(TimeOfDay);
//            testQueryPlan.testQueryPlanIRES_Postgres(TimeOfDay);        
//        setup();
//        TPCHQuery.TPCH_Hive_Postgres(TimeOfDay, "", "");
//        TPCHQuery.TPCH_Hive_Postgres(TimeOfDay, "", "");
//        tearDown();
//        setup();
//        TPCHQuery.TPCH_Hive_Hive(TimeOfDay, "lineitem");
//        tearDown();
//        setup();
//            TPCHQuery.TPCH_Postgres_Postgres(TimeOfDay, "lineitem");
//        tearDown();
//        TPCHStandalone.TPCH_Standalone_Hive_Hive(TimeOfDay); 
//        TPCHStandalone.TPCH_Standalone_Hive_Postgres(TimeOfDay); 
//            TPCHStandalone.TPCH_Standalone_Postgres_Postgres(TimeOfDay);
//    Thread.sleep(1000);
        }       
    }
    /*    
    @Test
    public void testhadoop() throws URISyntaxException {
        String inputFile = getClass().getResource("loremipsum.txt").toURI().toString();
        new App().run(inputFile);
//        new App().runningSpark();
        new App().createasapproperties();
        System.out.println(inputFile);
    }
*/
    
}
