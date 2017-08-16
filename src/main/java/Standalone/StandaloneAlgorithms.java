/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Standalone;

import static Algorithms.Algorithms.estimateCostValue;
import static Algorithms.Algorithms.estimateSizeOfMatrix;
import static Algorithms.Algorithms.roundMaxtrix;
import static Algorithms.Algorithms.setupStochasticValue;
import static Algorithms.Algorithms.setupValue;
import Algorithms.SimulateStochastic;
import Algorithms.reportResult;
import Algorithms.testWriteMatrix2CSV;
import IRES.LuaScript;
import IRES.ParquetCSV;
import IRES.Script;
import IRES.runWorkFlowIRES;
import static IRES.runWorkFlowIRES.Nameop;
import IRES.testQueryPlan;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.WorkflowClient;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author letrung
 */
public class StandaloneAlgorithms {

//    private static int numberParameter = 3;
    private static int numberOfSize = 3;   
    private static int numberOfTmp = 5;
//    private static int YarnParamter = 2;
    private static int numberOfSize_Hive_Postgres = 6;
    private static int numberOfSize_Postgres_Postgres = 6;
    private static int numberOfSize_Hive_Hive = 2;
    int int_localhost = 1323;
    String name_host = "localhost";
    String SPARK_HOME = new App().readhome("SPARK_HOME");
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String IRES_HOME = new App().readhome("IRES_HOME");
    String ASAP_HOME = IRES_HOME;
    String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    String directory_library = IRES_library+"/target/asapLibrary/";
    String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    String directory_datasets = IRES_library+"/target/asapLibrary/datasets/";
    String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    
    String[] start = new String[]{"/bin/sh", ASAP_HOME+"/start-ires.sh"};
    String[] stop = new String[]{"/bin/sh", ASAP_HOME+"/stop-ires.sh"};
    
    public double runScriptOperator(Move_Data Data, String workflow, String policy, double EsTime) throws InterruptedException, IOException{        
        double actualTime = 1.0;         
        double estimatedTime = 0.0;
        double estimatedCost = 0;
        double price = 1.0; 
        StandaloneScript MasterSh = new StandaloneScript();
        estimatedTime = EsTime; 
        estimatedCost = price * estimatedTime;        
        int count=0;
        String NameOp = Nameop(Data);
        while(true){
            long start = System.currentTimeMillis();
//            String[] cmd = new String[]{"/bin/sh", OperatorFolder+NameOp+"/"+NameOp+".sh"};
            MasterSh.runScript("sh "+ OperatorFolder+NameOp+"/"+NameOp+".sh");
//            while (pr.isAlive());
//            Process proc = Runtime.getRuntime().exec(OperatorFolder+NameOp+"/"+NameOp+".sh"); //Whatever you want to execute
            long stop = System.currentTimeMillis();            
            actualTime = (double)(stop-start)/1000.0;// -12.0;
            System.out.println("ActualTime of "+NameOp+" is: "+actualTime+" and EstimatedTime of "+NameOp+" is: "+estimatedTime+"-and EstimateCost of "+NameOp+" is: "+estimatedCost);                    
            Thread.sleep(5000);
            count++;
            if(count>=1)// old value is 1000          
            break;
        }       
        return actualTime;
    }    
    public void setup(Move_Data Data, YarnValue yarnValue, double[] size, String Size_tpch, double TimeOfDay) throws Exception {        
        int numberParameter = size.length + 1;
        String[] randomQuery = testQueryPlan.createRandomQuery();
        double[] size_random = calculateSize(randomQuery, Data.get_From(), Data.get_To(),Size_tpch);
        double[] yarn_random = testQueryPlan.createRandomYarn(); 
        
        double Data_size;
        double Ram;
        double Core;
        double TimeRepsonse = 0;
        String SQL = "";
        int i = 0;        
        double Numberuser = 100;
        
        String directory = testWriteMatrix2CSV.getDirectory(Data);        
        String delay_ys = "";
        if (TimeOfDay<1) delay_ys = "no_delay";
	String NameOfRealValue = delay_ys+"realValue";
        String NameOfParameter = delay_ys+"Parameter";
        String NameOfEstimateValue = delay_ys+"Estimate";
        double[] Parameter = initParamter(numberParameter);

        for (i = 0; i< size.length+2; i++)
                {   System.out.println("\nTest Time:"+i+"--------------------------------------------------------");
                    //TimeOfDay = 24*Math.random();
                    randomQuery = testQueryPlan.createRandomQuery();
                    size_random = calculateSize(randomQuery, Data.get_From(), Data.get_To(),Size_tpch);
                    size_random[size_random.length-1] = TimeOfDay;
                    createData(Data, SQL, yarnValue);                    
                    TimeRepsonse =  Math.random()*500;//IRES.runWorkflow(NameOfWorkflow, policy);
                    double delay = SimulateStochastic.waiting(Numberuser,TimeOfDay);
                    TimeRepsonse = TimeRepsonse + delay;     
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size_random,TimeRepsonse)), NameOfRealValue);
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size_random,TimeRepsonse)), NameOfEstimateValue);
                    testWriteMatrix2CSV.storeParameter(Data, Parameter, NameOfParameter);
                }        
    }
    public void createData (Move_Data Data, String SQL, YarnValue yarn) throws Exception {
        String NameOp = Nameop(Data);
        LuaScript script = new LuaScript();
        String lua = script.LuaScript2(Data, yarn);
        Path path = Paths.get(OperatorFolder+NameOp);
        Files.createDirectories(path);
        createfile(OperatorFolder+NameOp, NameOp + ".lua", lua);
        switch (Data.get_From().toLowerCase()) {
            case "hive":
                {
                    if (Data.get_To().toLowerCase().contains("postgres"))
                    {
                        create_Data_Hive_Postgres_remote(Data, SQL);                   
                        create_Remote_Hive_CSV(Data, SQL);
                        create_SQL_Postgres(Data, SQL);
                    }//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                    else 
                        if (Data.get_To().toLowerCase().contains("spark"))
                        {
                            create_Data_Hive_Spark(Data);
                        }
                        else 
                            if (Data.get_To().toLowerCase().contains("hive")) {
                                create_Data_SQL_Hive_remote(Data,SQL);
                                create_Data_SQL_Hive(Data,SQL); 
                            }                    
                }               
                break;
            case "postgres":
                {
                if (Data.get_To().toLowerCase().contains("hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if (Data.get_To().toLowerCase().contains("spark"))
                    create_Data_Postgres_Spark(Data);
                else if (Data.get_To().toLowerCase().contains("postgres"))
                    {
                    create_Data_Postgres_Postgres_remote(Data, SQL);
                    create_Remote_Postgres_CSV(Data, SQL);
                    create_SQL_Postgres(Data, SQL);
                    }
                }
                break;
            case "spark":
                {
                if (Data.get_To().toLowerCase() == "postgres")
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if (Data.get_To().toLowerCase() == "hive")
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }       
    } 
    public void create_Data_Hive_Postgres(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();       
        String sh = "";
        if (Data.get_Operator() == "TPCH")
            sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2Posgres() + script.TPCH_Postgres_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2Posgres() + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    public void create_Data_Hive_Postgres_remote(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.CSV2Posgres() + script.TPCH_Postgres_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.CSV2Posgres() + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    
    public void create_Data_SQL_Hive_remote(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Hive_SQL_remote(Data, SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);       
    }
    public void create_Remote_Hive_CSV(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.Hive2CSV() + script.bottom_sh();
     
        else 
            sh = script.top_sh(Data) + script.Hive2CSV() + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + "_remote" + ".sh", sh);      
    }
    public void create_Data_Hive_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2HDFS() + script.HDFS2Parquet(Data) + script.bottom_sh();
        ParquetCSV convertCSV2Parquet = new ParquetCSV();
        String PY = convertCSV2Parquet.CSV2Parquet(Data.get_DataIn());
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", PY);
    }
    public void create_Data_Postgres_Postgres_remote(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();      
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.POSTGRES2CSV_remote() + script.CSV2Posgres() + script.TPCH_Postgres_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.POSTGRES2CSV_remote() + script.CSV2Posgres() + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    public void create_Remote_Postgres_CSV(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.Postgres2CSV() + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.Postgres2CSV() + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + "_remote" + ".sh", sh);
    }
    public void create_Data_Postgres_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Postgres2CSV() + script.CSV2HDFS() + script.HDFS2Hive() + script.bottom_sh(); 
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    
    public void create_Data_Postgres_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Postgres2CSV() + script.CSV2HDFS() + script.HDFS2Parquet(Data) + script.bottom_sh();
        StandaloneParquetCSV convertCSV2Parquet = new StandaloneParquetCSV();
        String PY = convertCSV2Parquet.CSV2Parquet(Data.get_DataIn());
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", PY);
    }
    
    public void create_Data_Spark_Postgres(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Parquet2CSV(Data) + script.CSV2Posgres() + script.bottom_sh();
        StandaloneParquetCSV convertParquet2CSV = new StandaloneParquetCSV();
        String PY = convertParquet2CSV.Parquet2CSV();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", PY);
    }
    
    public void create_Data_Spark_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Parquet2CSV(Data) + script.CSV2HDFS() + script.HDFS2Hive() + script.bottom_sh();
        StandaloneParquetCSV convertParquet2CSV = new StandaloneParquetCSV();
        String PY = convertParquet2CSV.Parquet2CSV();
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", PY);
    } 
    public void create_Data_SQL_Postgres(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);
        
    }   
    public void create_Data_SQL_Hive(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        StandaloneScript script = new StandaloneScript();
        String sh = script.top_sh(Data) + script.Hive_SQL(Data, SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp +"_remote"+ ".sh", sh);
        
    }
    public static void createfile(String OperatorFolder,String filename, String content){        
        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(OperatorFolder+"/"+filename), "utf-8"));
            writer.write(content);
        } 
        catch (IOException ex) {
                // report
        } 
        finally {
            try {writer.close();} 
            catch (Exception ex) {/*ignore*/}
        }
    }
    public void create_SQL_Postgres(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);
        
    }
    public static String Nameop(Move_Data Data){
        String NameOp = Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To();
        return NameOp;
    }
    public static String AbstractOp(Move_Data Data){
        String NameOp = Nameop(Data);
        return "Abstract_"+NameOp;        
    }
    
    public void mainStandalone(Move_Data Data, String SQL, YarnValue yarnValue, double TimeOfDay, double[] size ) throws Exception{

//        runWorkFlowIRES IRES = new runWorkFlowIRES();
        int numberParameter = size.length + 1;
        int numerOfVariable = numberParameter-1; 

        String realValue, parameter, estimate, directory, error;
        directory = testWriteMatrix2CSV.getDirectory(Data);
	String delay_ys = "";
        if (TimeOfDay<1) delay_ys = "no_delay";
        
        realValue = directory + "/"+delay_ys+"realValue.csv";
        String NameOfRealValue = delay_ys+"realValue";
        
        parameter = directory + "/"+delay_ys+"Parameter.csv";
        error = directory + "/error_"+delay_ys+ Data.get_Operator()+ ".csv";
        
        estimate = directory + "/"+delay_ys+"Estimate.csv";
        String NameOfEstimateValue = delay_ys+"Estimate";

        int Max = CsvFileReader.count(realValue)-1;
        double R_2_limit = 0.8;
        int sizeOfValue;
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
    
        String NameOp = runWorkFlowIRES.Nameop(Data);
        String NameOfWorkflow = NameOp+"_Workflow";     
//        runWorkFlowIRES workflow = new runWorkFlowIRES();
///////////Set up for the first time only ////////////////////////         
        double costEstimateValue = 0;
        
        String DataIn = Data.get_DataIn();
        String DataOut = Data.get_DataOut();
        double Numberuser = 100;
	//if (!Files.exists(realValue))            Files.createFile(realValue);
        //Path filePathRealValue = Paths.get(realValue);   
        
        sizeOfValue = estimateSizeOfMatrix(Max, numerOfVariable, directory, R_2_limit, delay_ys);
        System.out.println("\nReal Running:--------------------------------------------------------"+
                "\n"+Data.get_DataIn()+"\n"+yarnValue.toString());
        double[] StochasticValue = setupStochasticValue(size);
        costEstimateValue = estimateCostValue(sizeOfValue, realValue, parameter, StochasticValue, R_2_limit);
        createData(Data, SQL, yarnValue); 
        testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size, costEstimateValue)), NameOfEstimateValue); 

        double Time_Cost = runScriptOperator(Data, NameOfWorkflow, policy, costEstimateValue); 
        
        double delay = SimulateStochastic.waiting(Numberuser,TimeOfDay);
        Time_Cost = Time_Cost + delay;
         
        testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size, Time_Cost)), NameOfRealValue);
        System.out.println("\n Estimate Value is: " + costEstimateValue);
        System.out.println("\n Real Value is: " + Double.toString(Time_Cost-delay));
        System.out.println("\n Delay Value is: " + delay);       
        reportResult.reportError(error, setupStochasticValue(setupValue(size, Time_Cost)), costEstimateValue);
        reportResult.report(sizeOfValue, realValue, estimate, error);

    } 
    public static double [] setupStochasticValue(double[] size){
        double[] StochasticValue = new double [size.length];
        for (int i = 0; i < size.length; i++)
            StochasticValue[i] = size[i];
        return StochasticValue;
    }    
    public static double [] initParamter(int numberParameter){
        double [] Parameter = new double [numberParameter];
        for (int i = 0; i < numberParameter; i++)
            Parameter[i] = 1;
        return Parameter;
    }
    public static double[] calculateSize(String[] randomQuery, String From, String To, String Size_tpch) {
        double R1,R2;
        if ((From.toLowerCase().contains("hive"))&&(To.toLowerCase().contains("postgres"))) {
            double[] size = new double[numberOfSize_Hive_Postgres];
            size[0] = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);
            size[1] = testQueryPlan.pageDataset(randomQuery[1],Size_tpch);
            size[2] = testQueryPlan.tupleDataset(randomQuery[1],Size_tpch);
            size[3] = testQueryPlan.pageDataset(randomQuery[3],Size_tpch);
            size[4] = testQueryPlan.tupleDataset(randomQuery[3],Size_tpch);
            return size;
        }
        else {
            if ((From.toLowerCase().contains("hive"))&&(To.toLowerCase().contains("hive"))) {
            double[] size = new double[numberOfSize_Hive_Hive];
            size[0] = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);
            size[1] = testQueryPlan.sizeDataset(randomQuery[3],Size_tpch);
            return size;
            } 
            else {
                if ((From.toLowerCase().contains("postgres"))&&(To.toLowerCase().contains("postgres"))) {
                    double[] size = new double[numberOfSize_Postgres_Postgres];
                    size[0] = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);
                    size[1] = testQueryPlan.pageDataset(randomQuery[1],Size_tpch);
                    size[2] = testQueryPlan.tupleDataset(randomQuery[1],Size_tpch);
                    size[3] = testQueryPlan.pageDataset(randomQuery[3],Size_tpch);
                    size[4] = testQueryPlan.tupleDataset(randomQuery[3],Size_tpch);
                    return size;
                }
                else
                {    
                    double[] size = new double[numberOfSize];
                    R1 = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);// Size of Data In R1
                    R2 = testQueryPlan.sizeDataset(randomQuery[3],Size_tpch);// Size of Data In R2
                    size[0] = R1;// + R2 + R1*Math.log(R1) + R2*Math.log(R2);
                    size[1] = R1*R2;
                    size[2] = R2;
                    return size;
                }
            }
        }
        
    }
}
