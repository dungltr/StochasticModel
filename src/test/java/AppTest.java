
import com.sparkexample.App;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import thesis.experiments.DPccpExperiment;

import java.io.IOException;

//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.time.LocalDate;
//import java.time.format.DateTimeFormatter;

//import trctree.TupleTree;

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
//    static String test = new App().readhome("TEST");
//    static String training = new App().readhome("TRAINING");
//    static String testing = new App().readhome("TESTING");
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
//        DPccpExperiment.main(new String[] {"arg1", "arg2", "arg3"});
//        String file_test = test;
//        String training_file = training;
//        String testing_file = testing;
        //SparkExample.JavaSQLDataSourceExample.main(new String[] {"arg"});
        //SparkExample.JavaSparkHiveExample.main_test();
        //SparkExample.JavaSparkSQLExample.main(new String[] {"arg"});
        //SparkExample.JavaUserDefinedTypedAggregation.main(new String[] {"arg"});
        //SparkExample.JavaUserDefinedUntypedAggregation.main(new String[] {"arg"});
        //Irisa.Enssat.Rennes1.TestCatalys.should_get_dataframe_from_database();
        //Bootstrap.main(new String[] {"arg"});
        //////////////////////////////////////
        Irisa.Enssat.Rennes1.TestScala.test();
        //Irisa.Enssat.Rennes1.TestJava.main(new String[] {"arg1", "arg2", "arg3"});
        //Irisa.Enssat.Rennes1.TestSparkMlib.test();
        //////////////////////////////////////
//	testall();
//        com.sparkexample.testSparkDataFrame.test();
//        RATree.test_ratree();
//        System.out.println(System.getenv().get("HOME"));
//        thesis.experiments.Experiment1.main(new String[] {"arg1", "arg2", "arg3"});
//        thesis.enumeration_algorithms.IDP1_Balanced.main(new String[] {"arg1", "arg2", "arg3"});
//        TupleTree.main();
//        LinearRegressionManual.testLinearRegression(file_test);
//        LinearRegressionManual.small_test(training_file,testing_file);
//        LinearRegressionManual.main_test(training_file,testing_file);
//        LinearRegressionManual.test_minDataset(training_file,testing_file,0.8);
//        LinearRegressionManual.TPCH(0.0, "", "100m", "Hive", "Hive", "predict");
//        Irisa.Enssat.Rennes1.TestScript.testall();
	
        for (int i =0; i<1; i++)
        { 
//            Irisa.Enssat.Rennes1.TestMO.main(new String[] {"arg"});
//          TPCHQuery.Move(Math.random(),"tpch","100m","Hive","Hive","Move","training");
//          TPCHQuery.Move(Math.random(),"tpch","100m","Hive","Postgres","Move","training");
//          TPCHQuery.Move(Math.random(),"tpch","100m","Postgres","Hive","Move","training");
//          TPCHQuery.Move(Math.random(),"tpch","100m","Postgres","Postgres","Move","training");
//            TPCHQuery.SQL(Math.random(),"tpch","100m","Hive","Postgres","SQL","training");

//          TPCHQuery.Join(Math.random(),"tpch","100m","Postgres","Postgres","Join","training");
//          TPCHQuery.Join(Math.random(),"tpch","100m","Hive","Hive","Join","training");
//          TPCHQuery.WorkflowMove(Math.random(),"tpch","100m","Hive","Postgres","Move", "training");
//          TPCHQuery.WorkflowMove(Math.random(),"tpch","100m","Postgres","Hive","Move", "training");
//          TPCHQuery.WorkflowMove(Math.random(),"tpch","100m","Hive","Hive","Join", "training");
//          TPCHQuery.WorkflowMove(Math.random(),"tpch","100m","Postgres","Postgres","Join", "training");
//        TPCHQuery.WorkflowJoin(Math.random(),"tpch","100m","Hive","Hive","Join", "training");
//        TPCHQuery.WorkflowJoin(Math.random(),"tpch","100m","Postgres","Postgres","Join", "training");
//        TPCHQuery.WorkflowJoinMove(Math.random(),"tpch","100m","Hive","Postgres","Join","training");
//        IRES.TestWorkFlow.createWorkflowJoin();
//	IRES.TestWorkFlow.smallworkflow();
//        IRES.TestWorkFlow.workflow();
	}
//      IRES.TestWorkFlow.createWorkflowJoin();
	}
//    @Test
    public void testall() throws Exception{
        double TimeOfDay = 24.00*Math.random();
        int times = 1;
//        testCreateDatabase.testCreateHiveDataBase(TimeOfDay);
//        testCreateDatabase.testCreaePostgresDataBase(TimeOfDay);
        for (int i = 0; i < times; i++)
        { 
                    System.out.println("\n This is the "+i+"th round-------------------------------------------------------------------------------------------------------");

            TimeOfDay = Math.random();          
//            testCreateDatabase.testCreateHiveDataBase(TimeOfDay);
//            testCreateDatabase.testCreatePostgresDataBase(TimeOfDay);
//            testQueryPlan.testQueryPlanIRES_Hive_Postgres(TimeOfDay);
//            testQueryPlan.testQueryPlanIRES_Postgres(TimeOfDay);        
//        setup();
//        TPCHQuery.TPCH_Hive_Postgres(TimeOfDay, "lineitem", "", "Training");
//        TPCHQuery.TPCH_Hive_Postgres(TimeOfDay, "", "");
//        tearDown();
//        setup();

//        TPCHQuery.TPCH_Hive_Hive(TimeOfDay, "lineitem", "", "training");
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
