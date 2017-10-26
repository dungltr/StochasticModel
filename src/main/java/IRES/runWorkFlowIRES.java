/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;
////////////////////////////////////////////////////////////////////////////////
import static Algorithms.Algorithms.estimateCostValue;
import static Algorithms.Algorithms.setupStochasticValue;
import static Algorithms.Algorithms.setupValue;
import Algorithms.testWriteMatrix2CSV;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import LibraryIres.createWorkflow;
import LibraryIres.runWorkflow;
import Irisa.Enssat.Rennes1.TestScript;
import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.rest.beans.OperatorDictionary;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.WorkflowNode;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import org.apache.commons.io.FileUtils;
/**
 *
 * @author letrung
 */
public class runWorkFlowIRES {
    
    
    static int int_localhost = 1323;
    static String name_host = "localhost";
    static String SPARK_HOME = new App().readhome("SPARK_HOME");
    static String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    static String HIVE_HOME = new App().readhome("HIVE_HOME");
    static String IRES_HOME = new App().readhome("IRES_HOME");
    static String HDFS = new App().readhome("HDFS");
    static String ASAP_HOME = IRES_HOME;
    static String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    static String directory_library = IRES_library+"/target/asapLibrary/";
    static String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    static String directory_datasets = IRES_library+"/target/asapLibrary/datasets/";
    static String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    static String directory_workflow = directory_library + "workflows/";
    static String[] start = new String[]{"/bin/sh", ASAP_HOME+"/start-ires.sh"};
    static String[] stop = new String[]{"/bin/sh", ASAP_HOME+"/stop-ires.sh"};
    
    public static void copydata(String NameMaterialize) throws IOException{
        String folderName = OperatorFolder;// + NameOp;
        String folderWorkflow = directory_workflow + NameMaterialize;
	File folder = new File(folderName);
        File[] listOfOperators = folder.listFiles();
        
        File folderOperator = new File(folderWorkflow + "/operators");
        File[] listOfOperatorsDest = folderOperator.listFiles();
        
	for (int j = 0; j < listOfOperators.length; j++) {
            //File srcDir = new File(listOfOperators[j].toString()+"/data");  
            for (int i = 0; i < listOfOperatorsDest.length; i++) {
                if (listOfOperatorsDest[i].toString()
                        .replace(folderWorkflow+"/operators/", "")
                        .contains(listOfOperators[j].toString()
                                .replace(folderName, ""))){                   
                    //File destDir = new File(listOfOperatorsDest[i].toString()+"/data");
                    //FileUtils.copyDirectory(srcDir, destDir);
                    FileUtils.copyDirectory(FileUtils.getFile(listOfOperators[j]
                            .toString()+"/data"), 
                            FileUtils.getFile(listOfOperatorsDest[i]
                                    .toString()+"/data"));
                    System.out.println("Source: "+listOfOperators[j].toString()+" and " 
                            + listOfOperators[j].toString()
                            .replace(folderWorkflow+"/operators/", ""));
                    System.out.println("Destination: "+listOfOperatorsDest[i].toString() + " and " 
                            +listOfOperatorsDest[i].toString()
                            .replace(folderName, ""));
                }
            }
        }
    }
    public double runWorkflow(Move_Data Data, double[] size, String workflow, String policy) throws Exception{
        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        double actualTime = 1.0;
        int i = 0;
        String NameOp = Nameop(Data);
        String materializedWorkflow = wcli.materializeWorkflow(workflow, policy);
        System.out.println(materializedWorkflow);
        System.out.println("Add materializedWorkflow successful"+workflow);
        ////Execution 
        double estimatedTime = 0;
        double estimatedCost = 0;
        System.out.println(NameOp);
	File folder = new File(directory_workflow+materializedWorkflow+"/operators/");
        File[] listOfFiles = folder.listFiles();
        for (int j = 0; j < listOfFiles.length; j++) {
            NameOp = listOfFiles[j].toString();
            NameOp = listOfFiles[j].toString().replace(directory_workflow+materializedWorkflow+"/operators/", "");
	    System.out.println("MaterializedWorkflow successful is: " + NameOp);
            estimatedTime = Double.parseDouble(wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator(NameOp).getExecTime());
	    estimatedCost = Double.parseDouble(wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator(NameOp).getCost());        
        }

        testWriteMatrix2CSV.storeValueServer(Data, "", setupStochasticValue(setupValue(size, estimatedTime)), "execTime_estimate");    
        int count=0;
	copydata(materializedWorkflow);

        while(true){
            String w = wcli.executeWorkflow(materializedWorkflow);
            long start = System.currentTimeMillis();
            wcli.waitForCompletion(w);
            long stop = System.currentTimeMillis();
            actualTime = (double)(stop-start)/1000.0;// -12.0;
            System.out.println("ActualTime of "+NameOp+" is: "+actualTime+" and EstimatedTime of "+NameOp+" is: "+estimatedTime+"-and EstimateCost of "+NameOp+" is: "+estimatedCost);                    
            count++;
            Thread.sleep(1000);
            if(count>=1)// old value is 1000
	    break;
        }       
        //wcli.removeMaterializedWorkflow(materializedWorkflow);

        return actualTime;
    }
    public static void reset(String directory){
        String dir = directory;
        File filename = new File(dir);
        if (filename.exists()){
                filename.delete();
        }
    }
    public static String datasetin (Move_Data Data){
        return Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To()+"_"+Data.get_DataIn();
    }
    public static String datasetout (Move_Data Data){
        return Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To()+"_"+Data.get_DataOut();
    }
    public static String datasetin2 (Move_Data Data){
        return "Move_TPCH_Hive_Postgres_"+Data.get_DataIn().toUpperCase();
    }
    public static String datasetout2 (Move_Data Data){
        return "Move_TPCH_Postgres_Postgres_"+Data.get_DataOut().toUpperCase();
    }
    public static String datasetout3 (Move_Data Data){
        return "Join_TPCH_Postgres_Postgres_"+Data.get_DataIn().toUpperCase()+Data.get_DataOut().toUpperCase();
    }
    public void createDatasetMove_Hive_Postgres(Move_Data Data, double [] size, String SQL, double TimeOfDay) throws Exception {
        String node_pc = new App().getComputerName();
        Dataset d1 = new Dataset(datasetin(Data));
        d1.add("Constraints.Engine.SQL",Data.get_From()+Data.get_Operator());
	d1.add("Constraints.Engine.location",node_pc);
        d1.add("Constraints.type","SQL");
	d1.add("Execution.name",Data.get_DataIn());
        d1.add("Execution.schema", Data.get_Schema());
        d1.add("Execution.path", "hdfs://"+HDFS+"/"+Data.get_DatabaseIn()+".db/"+Data.get_DataIn());
	d1.add("Optimization.size",Data.get_DataInSize());      
        if (!SQL.equals("")){
            if (Data.get_To().toLowerCase().equals("postgres")){
                d1.add("Optimization.page",Double.toString(size[1]));
                d1.add("Optimization.tuple",Double.toString(size[2]));           
                d1.add("Optimization.page1",Double.toString(size[3]));
                d1.add("Optimization.tuple1",Double.toString(size[4]));
            }
            if (Data.get_To().toLowerCase().equals("hive")&&
                Data.get_From().toLowerCase().equals("postgres")){
                d1.add("Optimization.page",Double.toString(size[1]));
                d1.add("Optimization.tuple",Double.toString(size[2]));           
                d1.add("Optimization.size1",Double.toString(size[3]));
            }
            if (Data.get_To().toLowerCase().equals("hive")&&
                Data.get_From().toLowerCase().equals("hive")){
                d1.add("Optimization.size1",Double.toString(size[1]));
            }
        }
        if ((!Data.get_To().toLowerCase().equals("hive")||
                !Data.get_From().toLowerCase().equals("hive"))&&SQL.equals("")){
            d1.add("Optimization.page",Double.toString(size[1]));
            d1.add("Optimization.tuple",Double.toString(size[2]));
        }
        d1.add("Optimization.random",Double.toString(TimeOfDay));
	d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
        
        Dataset d2 = new Dataset(datasetout(Data));
        d2.add("Constraints.Engine.SQL",Data.get_To()+Data.get_Operator());
	d2.add("Constraints.Engine.location",node_pc);
        d2.add("Constraints.type","SQL");
	d2.add("Execution.name",Data.get_DataOut());
        d2.add("Execution.schema", Data.get_Schema());
	d2.add("Optimization.size",Data.get_DataInSize());      
	d2.writeToPropertiesFile(directory_datasets + d2.datasetName);
    }
    public void createDatasetJoin(Move_Data Data, double [] size, String SQL, double TimeOfDay) throws Exception {
        String node_pc = new App().getComputerName();
        Dataset d1 = new Dataset(datasetin(Data));
        d1.add("Constraints.Engine.SQL",Data.get_From()+Data.get_Operator());
	d1.add("Constraints.Engine.location",node_pc);
        d1.add("Constraints.type","SQL");
	d1.add("Execution.name",Data.get_DataIn());
        d1.add("Execution.schema", Data.get_Schema());
        d1.add("Execution.path", "hdfs://"+HDFS+"/"+Data.get_DatabaseIn()+".db/"+Data.get_DataIn());
	d1.add("Optimization.size",Data.get_DataInSize());      
        if (Data.get_To().toLowerCase().equals("postgres")){
                d1.add("Optimization.page",Double.toString(size[1]));
                d1.add("Optimization.tuple",Double.toString(size[2]));           
            }
        d1.add("Optimization.random",Double.toString(TimeOfDay));
	d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
        
        Dataset d2 = new Dataset(datasetout(Data));
        d2.add("Constraints.Engine.SQL",Data.get_From()+Data.get_Operator());
	d2.add("Constraints.Engine.location",node_pc);
        d2.add("Constraints.type","SQL");
	d2.add("Execution.name",Data.get_DataOut());
        d2.add("Execution.schema", Data.get_Schema());
	d2.add("Optimization.size",Data.get_DataOutSize());  
        if (Data.get_To().toLowerCase().equals("postgres")){          
            d2.add("Optimization.page",Double.toString(size[3]));
            d2.add("Optimization.tuple",Double.toString(size[4]));
        }
	d2.writeToPropertiesFile(directory_datasets + d2.datasetName);  
        
        Dataset d3 = new Dataset(Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To()+"_"+Data.get_DataIn().toUpperCase()+Data.get_DataOut().toUpperCase());
        d3.add("Constraints.Engine.SQL",Data.get_To()+Data.get_Operator());
	d3.add("Constraints.Engine.location",node_pc);
        d3.add("Constraints.type","SQL");
	d3.add("Execution.name",Data.get_DataIn()+"_"+Data.get_DataOut());
        d3.add("Execution.schema", Data.get_Schema());
	d3.add("Optimization.size",Data.get_DataInSize());      
	d3.writeToPropertiesFile(directory_datasets + d3.datasetName);
    }
public void createDatasetJoin2(Move_Data Data, double [] size, String SQL, double TimeOfDay) throws Exception {
        String node_pc = new App().getComputerName();
        Dataset d1 = new Dataset(datasetin2(Data));
        d1.add("Constraints.Engine.SQL",Data.get_From()+Data.get_Operator());
        d1.add("Constraints.Engine.location",node_pc);
        d1.add("Constraints.type","SQL");
        d1.add("Execution.name",Data.get_DataIn());
        d1.add("Execution.schema", Data.get_Schema());
        d1.add("Execution.path", "hdfs://"+HDFS+"/"+Data.get_DatabaseIn()+".db/"+Data.get_DataIn());
        d1.add("Optimization.size",Data.get_DataInSize());      
        if (Data.get_To().toLowerCase().equals("postgres")){
                d1.add("Optimization.page",Double.toString(size[1]));
                d1.add("Optimization.tuple",Double.toString(size[2]));           
            }
        d1.add("Optimization.random",Double.toString(TimeOfDay));
        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
        
        Dataset d2 = new Dataset(datasetout2(Data));
        d2.add("Constraints.Engine.SQL",Data.get_From()+Data.get_Operator());
        d2.add("Constraints.Engine.location",node_pc);
        d2.add("Constraints.type","SQL");
        d2.add("Execution.name",Data.get_DataOut());
        d2.add("Execution.schema", Data.get_Schema());
        d2.add("Optimization.size",Data.get_DataOutSize());  
        if (Data.get_To().toLowerCase().equals("postgres")){          
            d2.add("Optimization.page",Double.toString(size[3]));
            d2.add("Optimization.tuple",Double.toString(size[4]));
        }
        d2.writeToPropertiesFile(directory_datasets + d2.datasetName);  
        
        Dataset d3 = new Dataset(datasetout3(Data));
        d3.add("Constraints.Engine.SQL",Data.get_To()+Data.get_Operator());
        d3.add("Constraints.Engine.location",node_pc);
        d3.add("Constraints.type","SQL");
        d3.add("Execution.name",Data.get_DataIn()+"_"+Data.get_DataOut());
	d3.add("Execution.schema", Data.get_Schema());
        d3.add("Optimization.size",Data.get_DataInSize());      
        d3.writeToPropertiesFile(directory_datasets + d3.datasetName);
	}
    public void createAbstractOperatorMove(Move_Data Data, String SQL) throws IOException, Exception {
        String node_pc = new App().getComputerName();
        String NameOp = Nameop(Data);
        String AbstractOp = "Abstract_"+NameOp;
        String AlgorithmsName = "move";        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);        
        AbstractOperator op = new AbstractOperator(AbstractOp);//AopAbstractOperator);//AopAbstractOperator);
        op.add("Constraints.Engine", Data.get_To());
        op.add("Constraints.Input.number","1");
	op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
	op.add("Constraints.Output.number", "1");
	reset(directory_library + "abstractOperators/" + op.opName);
        op.writeToPropertiesFile(directory_library + "abstractOperators/" + op.opName);                      
        cli.addAbstractOperator(op);
        //op.writeToPropertiesFile(op.opName);
    }
    public void createAbstractOperatorJoin(Move_Data Data, String SQL) throws IOException, Exception {
        String node_pc = new App().getComputerName();
        String NameOp = Nameop(Data);
        String AbstractOp = "Abstract_"+NameOp;
        String AlgorithmsName = "join";        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);        
        AbstractOperator op = new AbstractOperator(AbstractOp);//AopAbstractOperator);//AopAbstractOperator);
        op.add("Constraints.Engine", Data.get_To());
        op.add("Constraints.Input.number","2");
	op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
	op.add("Constraints.Output.number", "1");
	reset(directory_library + "abstractOperators/" + op.opName);
        op.writeToPropertiesFile(directory_library + "abstractOperators/" + op.opName);                      
        cli.addAbstractOperator(op);
        //op.writeToPropertiesFile(op.opName);
    }
    public void createOperatorMove(Move_Data Data, String SQL, double costEstimateValue) throws IOException, Exception {
        String node_pc = new App().getComputerName();
        String NameOp = Nameop(Data);
        String AbstractOp = "Abstract_"+NameOp;
        String AlgorithmsName = "move";   
        String numberArgument = "3";
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        Operator mop1 = new Operator(NameOp,"");
        
        mop1.add("Constraints.Engine",Data.get_To());
        String To = Data.get_To();
        switch (To) {
            case "HIVE": case "hive": case "Hive":
                {
                mop1.add("Constraints.EngineSpecification.Distributed.HIVE.masterLocation", node_pc);
                }
                break;    
            case "POSTGRES": case "postgres": case "Postgres":
                {
                mop1.add("Constraints.EngineSpecification.Distributed.Spark.masterLocation", node_pc);
                }
                break;   
            case "SPARK": case "spark": case "Spark":
                {
                mop1.add("Constraints.EngineSpecification.Distributed.Spark.masterLocation", node_pc);
                }
                break;   
            default:  
                mop1.add("Constraints.EngineSpecification.Centralized.PostgreSQL.location", node_pc);
                mop1.add("Constraints.EngineSpecification.Distributed.HIVE.masterLocation", node_pc);
                mop1.add("Constraints.EngineSpecification.Distributed.Spark.masterLocation", node_pc);
                break;
        }
        
        mop1.add("Constraints.Input.number","1");
        mop1.add("Constraints.Input0.Engine.SQL", Data.get_From()+Data.get_Operator());
        mop1.add("Constraints.Input0.Engine.location", node_pc);
        mop1.add("Constraints.Input0.type", "SQL");
        mop1.add("Constraints.OpSpecification.Algorithm.name", AlgorithmsName);        
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Output0.Engine.SQL", Data.get_To()+Data.get_Operator());
        mop1.add("Constraints.Output0.Engine.location", node_pc);
        mop1.add("Constraints.Output0.type", "SQL");

        mop1.add("Optimization.Out0.size", "In0.size");// different in Hive-Spark or Postgres-Spark //Optimization.Out0.size=20
        mop1.add("Optimization.cost", "1.0"); 
        mop1.add("Optimization.execTime", Double.toString(costEstimateValue));//"1.0"); // different in Hive-Spark or in Postgres-Spark// Optimization.execTime=In0.size/1.2
                 
        mop1.add("Optimization.inputSpace.In0.size", "Double,1E8,1E10,l");
        if (!SQL.equals("")){
            if (Data.get_To().toLowerCase().equals("postgres")){
                mop1.add("Optimization.inputSpace.In0.page", "Double,1E8,1E10,l");
                mop1.add("Optimization.inputSpace.In0.tuple", "Double,1E8,1E10,l");
                mop1.add("Optimization.inputSpace.In0.page1", "Double,1E8,1E10,l");
                mop1.add("Optimization.inputSpace.In0.tuple1", "Double,1E8,1E10,l");
            }
            if (Data.get_To().toLowerCase().equals("hive")&&
                Data.get_From().toLowerCase().equals("postgres")){
                mop1.add("Optimization.inputSpace.In0.page", "Double,1E8,1E10,l");
                mop1.add("Optimization.inputSpace.In0.tuple", "Double,1E8,1E10,l");
                mop1.add("Optimization.inputSpace.In0.size1", "Double,1E8,1E10,l");
            }
            if (Data.get_To().toLowerCase().equals("hive")&&
                Data.get_From().toLowerCase().equals("hive")){
                mop1.add("Optimization.inputSpace.In0.size1", "Double,1E8,1E10,l");
            }
        }
        if ((!Data.get_To().toLowerCase().equals("hive")||
                !Data.get_From().toLowerCase().equals("hive"))&&SQL.equals("")){
            mop1.add("Optimization.inputSpace.In0.page", "Double,1E8,1E10,l");
            mop1.add("Optimization.inputSpace.In0.tuple", "Double,1E8,1E10,l");
        }
/*        if ((Data.get_To().toLowerCase().equals("postgres")||
                Data.get_From().toLowerCase().equals("postgres"))&&(SQL.equals(""))){
            mop1.add("Optimization.inputSpace.In0.page", "Double,1E8,1E10,l");
            mop1.add("Optimization.inputSpace.In0.tuple", "Double,1E8,1E10,l");            
            mop1.add("Execution.Argument3", "In0.page");
            mop1.add("Execution.Argument4", "In0.tuple"); 
            mop1.add("Execution.Argument5", "In0.random");
            numberArgument = "6";
        }
*/        
        mop1.add("Optimization.inputSpace.In0.random", "Double,1E8,1E10,l");

	mop1.add("Optimization.model.Out0.size", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost",      "gr.ntua.ece.cslab.panic.core.models.UserFunction");//AbstractWekaModel");//UserFunction");//UserFunction");       
        mop1.add("Optimization.model.execTime",  "gr.ntua.ece.cslab.panic.core.models.LinearRegression");//UserFunction");//UserFunction");//AbstractWekaModel");//UserFunction");
        
        mop1.add("Optimization.outputSpace.Out0.size", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");        
        mop1.add("Optimization.outputSpace.execTime", "Double");
        
        mop1.add("Optimization.inputSource.type","csv");
        mop1.add("Optimization.inputSource.host",node_pc);
        mop1.add("Optimization.inputSource.db","metrics");
       
        mop1.add("Execution.LuaScript",NameOp+".lua");  
        if ("SPARK".equals(Data.get_From())|| 
                "Spark".equals(Data.get_From())|| 
                "spark".equals(Data.get_From()))       
            mop1.add("Execution.Argument0", Data.get_DatabaseOut());
        else 
            mop1.add("Execution.Argument0", Data.get_DatabaseIn());
        mop1.add("Execution.Argument1", "In0.name");
        mop1.add("Execution.Argument2", "In0.schema");       
         
        if ("SPARK".equals(Data.get_To())|| 
                "Spark".equals(Data.get_To())||
                "spark".equals(Data.get_To())||
                "SPARK".equals(Data.get_From())||
                "Spark".equals(Data.get_From())||
                "spark".equals(Data.get_From())) 
        {
            numberArgument = "4";
            mop1.add("Execution.Argument3", "local[*]");//Execution.Argument2=spark://master:7077
        }
        

        mop1.add("Execution.Arguments.number", numberArgument);    
        mop1.add("Execution.Output0.name", "In0.name");
        mop1.add("Execution.Output0.schema", "In0.schema");

        cli.addOperator(mop1); 
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
    }
    public void createOperatorJoin(Move_Data Data, String SQL, double costEstimateValue) throws IOException, Exception {
        String node_pc = new App().getComputerName();
        String NameOp = Nameop(Data);
        String AbstractOp = "Abstract_"+NameOp;
        String AlgorithmsName = "join";   
        String numberArgument = "3";
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        Operator mop1 = new Operator(NameOp,"");
        
        mop1.add("Constraints.Engine",Data.get_To());
        String To = Data.get_To();
        switch (To) {
            case "HIVE": case "hive": case "Hive":
                {
                mop1.add("Constraints.EngineSpecification.Distributed.HIVE.masterLocation", node_pc);
                }
                break;    
            case "POSTGRES": case "postgres": case "Postgres":
                {
                mop1.add("Constraints.EngineSpecification.Distributed.Spark.masterLocation", node_pc);
                }
                break;   
            case "SPARK": case "spark": case "Spark":
                {
                mop1.add("Constraints.EngineSpecification.Distributed.Spark.masterLocation", node_pc);
                }
                break;   
            default:  
                mop1.add("Constraints.EngineSpecification.Centralized.PostgreSQL.location", node_pc);
                mop1.add("Constraints.EngineSpecification.Distributed.HIVE.masterLocation", node_pc);
                mop1.add("Constraints.EngineSpecification.Distributed.Spark.masterLocation", node_pc);
                break;
        }      
        mop1.add("Constraints.Input.number","2");
        mop1.add("Constraints.Input0.Engine.SQL", Data.get_From()+Data.get_Operator());
        mop1.add("Constraints.Input0.Engine.location", node_pc);
        mop1.add("Constraints.Input0.type", "SQL");
        
        mop1.add("Constraints.Input1.Engine.SQL", Data.get_From()+Data.get_Operator());
        mop1.add("Constraints.Input1.Engine.location", node_pc);
        mop1.add("Constraints.Input1.type", "SQL");
        
        mop1.add("Constraints.OpSpecification.Algorithm.name", AlgorithmsName);        
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Output0.Engine.SQL", Data.get_To()+Data.get_Operator());
        mop1.add("Constraints.Output0.Engine.location", node_pc);
        mop1.add("Constraints.Output0.type", "SQL");

        mop1.add("Optimization.Out0.size", "In0.size");// different in Hive-Spark or Postgres-Spark //Optimization.Out0.size=20
        mop1.add("Optimization.cost", "1.0"); 
        mop1.add("Optimization.execTime", Double.toString(costEstimateValue));//"1.0"); // different in Hive-Spark or in Postgres-Spark// Optimization.execTime=In0.size/1.2
                 
        mop1.add("Optimization.inputSpace.In0.size", "Double,1E8,1E10,l");
        mop1.add("Optimization.inputSpace.In1.size", "Double,1E8,1E10,l");
        
        if (Data.get_To().toLowerCase().equals("postgres")){
            mop1.add("Optimization.inputSpace.In0.page", "Double,1E8,1E10,l");
            mop1.add("Optimization.inputSpace.In0.tuple", "Double,1E8,1E10,l");
            mop1.add("Optimization.inputSpace.In1.page", "Double,1E8,1E10,l");
            mop1.add("Optimization.inputSpace.In1.tuple", "Double,1E8,1E10,l");
        }
        mop1.add("Optimization.inputSpace.In0.random", "Double,1E8,1E10,l");

	mop1.add("Optimization.model.Out0.size", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost",      "gr.ntua.ece.cslab.panic.core.models.UserFunction");//AbstractWekaModel");//UserFunction");//UserFunction");       
        mop1.add("Optimization.model.execTime",  "gr.ntua.ece.cslab.panic.core.models.LinearRegression");//UserFunction");//UserFunction");
        
        mop1.add("Optimization.outputSpace.Out0.size", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");        
        mop1.add("Optimization.outputSpace.execTime", "Double");
        
        mop1.add("Optimization.inputSource.type","csv");
        mop1.add("Optimization.inputSource.host",node_pc);
        mop1.add("Optimization.inputSource.db","metrics");
       
        mop1.add("Execution.LuaScript",NameOp+".lua");  
        if ("SPARK".equals(Data.get_From())|| 
                "Spark".equals(Data.get_From())|| 
                "spark".equals(Data.get_From()))       
            mop1.add("Execution.Argument0", Data.get_DatabaseOut());
        else 
            mop1.add("Execution.Argument0", Data.get_DatabaseIn());
        mop1.add("Execution.Argument1", "In0.name");
        mop1.add("Execution.Argument2", "In0.schema");       
         
        if ("SPARK".equals(Data.get_To())|| 
                "Spark".equals(Data.get_To())||
                "spark".equals(Data.get_To())||
                "SPARK".equals(Data.get_From())||
                "Spark".equals(Data.get_From())||
                "spark".equals(Data.get_From())) 
        {
            numberArgument = "4";
            mop1.add("Execution.Argument3", "local[*]");//Execution.Argument2=spark://master:7077
        }
        

        mop1.add("Execution.Arguments.number", numberArgument);    
        mop1.add("Execution.Output0.name", "In0.name");
        mop1.add("Execution.Output0.schema", "In0.schema");

        cli.addOperator(mop1); 
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
    }
    public void createWorkflowMove(Move_Data Data, String SQL) throws Exception{
        String InPutData = datasetin(Data);//"asapServerLog";//Data.get_DataIn();
        String OutPutData = datasetout(Data);
        String NameOp = Nameop(Data);
        String AbstractOp = "Abstract_"+NameOp;
        String NameOfAbstractWorkflow = NameOp+"_Workflow";
        String AlgorithmsName = NameOp + "_query";
//        createDataset(Data, SQL);
//        createOperatorMove(Data, SQL);
//        createAbstractOperatorMove(Data, SQL);       
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
        AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        Operator mop1 = new Operator(NameOp,"");
        Dataset d1 = new Dataset(InPutData);
        
        d1.inputFor(mop1, 0);
//        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp = new AbstractOperator(AbstractOp);//AopAbstractOperator);
        WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp);
		
	Dataset d2 = new Dataset(OutPutData);
	WorkflowNode t2 = new WorkflowNode(false,true,OutPutData);
	t2.setDataset(d2);

        t1.addOutput(0,op1);
                
	op1.addInput(0,t1);
	op1.addOutput(0,t2);
		
	t2.addInput(0,op1);
		
	abstractWorkflow1.addTarget(t2);  
        WorkflowClient wcli = new WorkflowClient();
	wcli.setConfiguration(conf);
    
        wcli.addAbstractWorkflow(abstractWorkflow1);

// To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
        abstractWorkflow.addInputEdge(d1,abstractOp,0);
	abstractWorkflow.addOutputEdge(abstractOp,d2,0);
        abstractWorkflow.getWorkflow(d2);
//	wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
    }
    public void createWorkflowJoin(Move_Data Data, String SQL) throws Exception{
        String InPutData1 = datasetin(Data);//"asapServerLog";//Data.get_DataIn();
        String InPutData2 = datasetout(Data);//"asapServerLog";//Data.get_DataIn();
        String OutPutData = Data.get_DataIn()+datasetout(Data);
        String NameOp = Nameop(Data);
        String AbstractOp = "Abstract_"+NameOp;
        String NameOfAbstractWorkflow = NameOp+"_Workflow";
        String AlgorithmsName = NameOp + "_query";
//        createDataset(Data, SQL);
//        createOperatorMove(Data, SQL);
//        createAbstractOperatorMove(Data, SQL);       
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
        AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        Operator mop1 = new Operator(NameOp,"");
        
        Dataset d1 = new Dataset(InPutData1);        
        d1.inputFor(mop1, 0);
//        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData1);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp = new AbstractOperator(AbstractOp);//AopAbstractOperator);
        WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp);
        
        Dataset d2 = new Dataset(InPutData2);
        d2.inputFor(mop1, 0);
//        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
	WorkflowNode t2 = new WorkflowNode(false,false,InPutData2);
	t2.setDataset(d2);
                
		
	Dataset d3 = new Dataset(OutPutData);
	WorkflowNode t3 = new WorkflowNode(false,true,OutPutData);
	t3.setDataset(d3);

        t1.addOutput(0,op1);
        t2.addOutput(0,op1);
                
	op1.addInput(0,t1);
        op1.addInput(1,t2);
	op1.addOutput(0,t3);
		
	t3.addInput(0,op1);
		
	abstractWorkflow1.addTarget(t3);  
        WorkflowClient wcli = new WorkflowClient();
	wcli.setConfiguration(conf);
    
        wcli.addAbstractWorkflow(abstractWorkflow1);

// To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
        abstractWorkflow.addInputEdge(d1,abstractOp,0);
        abstractWorkflow.addInputEdge(d2,abstractOp,1);
	abstractWorkflow.addOutputEdge(abstractOp,d3,0);
        abstractWorkflow.getWorkflow(d3);
//	wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
    }
    public void createDataMove (Move_Data Data,String SQL) throws Exception {
        String NameOp = Nameop(Data);
        LuaScript script = new LuaScript();
        String lua = script.LuaScript(Data);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
        switch (Data.get_From().toLowerCase()) {
            case "hive":
                {
                if (Data.get_To().toLowerCase().contains("postgres"))
                {
                    create_Data_Hive_Postgres(Data, SQL);
                    create_SQL_Postgres(Data, SQL);
                }//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                    
                else if (Data.get_To().toLowerCase().contains("spark"))
                    create_Data_Hive_Spark(Data);
                }
                break;
            case "Postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Postgres_Spark(Data);
                else if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_SQL_Postgres(Data,SQL);
                }
                break;
            case "SPARK": case "spark": case "Spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }
    }
    public void createDataMove2 (Move_Data Data, String SQL, YarnValue yarn) throws Exception {
        String NameOp = Nameop(Data);
        LuaScript script = new LuaScript();
        String lua = script.LuaScript2(Data, yarn);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
       switch (Data.get_From().toLowerCase()) {
            case "hive":
                {
                if (Data.get_To().toLowerCase().contains("postgres"))
                {
                    create_Data_Hive_Postgres_remote(Data, SQL);
                    create_Remote_Hive_CSV(Data, SQL);
                    create_SQL_Postgres(Data, SQL);
                }//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if (Data.get_To().toLowerCase().contains("spark"))
                    {
                        create_Data_Hive_Spark(Data);
                    }
                else if (Data.get_To().toLowerCase().contains("hive"))
                    {   
                        //create_Data_SQL_Hive(Data,SQL); 
                        //create_Data_SQL_Hive_remote(Data,SQL);
			create_Data_Hive_Hive_remote(Data, SQL);
			create_Remote_Hive_CSV(Data,SQL);
			create_SQL_Hive(Data, SQL);
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
                    //create_Data_SQL_Postgres(Data,SQL);
                    {
                    create_Data_Postgres_Postgres_remote(Data, SQL);
                    create_Remote_Postgres_CSV(Data, SQL);
                    create_SQL_Postgres(Data, SQL);
                    }
                }
                break;
            case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "Postgres")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "Hive")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }       
    }
    public void create_Data_Hive_Postgres(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2Posgres() + script.TPCH_Postgres_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2Posgres() + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
        public void create_Data_Hive_Postgres_remote(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.CSV2Posgres() + script.TPCH_Postgres_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.CSV2Posgres() + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
public void create_Data_Hive_Hive_remote(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut$
        String NameOp = Nameop(Data);
        Script script = new Script();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.CSV2HDFS() + script.HDFS2Hive() + script.TPCH_Hive_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.CSV2HDFS() + script.HDFS2Hive() + script.Hive_SQL(Data,SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }

        public void create_Data_Postgres_Postgres_remote(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh(Data) + script.POSTGRES2CSV_remote() + script.CSV2Posgres() + script.TPCH_Postgres_SQL(Data,SQL) + script.bottom_sh();
        else 
            sh = script.top_sh(Data) + script.POSTGRES2CSV_remote() + script.CSV2Posgres() + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    public void create_Remote_Hive_CSV(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh_remote(Data) + script.Hive2CSV() + script.bottom_sh();
        else 
            sh = script.top_sh_remote(Data) + script.Hive2CSV() + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + "_remote" + ".sh", sh);
    }
    public void create_Remote_Postgres_CSV(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();       
        String sh = "";
        if (Data.get_Operator().contains("TPCH"))
            sh = script.top_sh_remote(Data) + script.Postgres2CSV() + script.bottom_sh();
        else 
            sh = script.top_sh_remote(Data) + script.Postgres2CSV() + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + "_remote" + ".sh", sh);
    }
    public void create_Data_Hive_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2HDFS() + script.HDFS2Parquet(Data) + script.bottom_sh();
        ParquetCSV convertCSV2Parquet = new ParquetCSV();
        String PY = convertCSV2Parquet.CSV2Parquet(Data.get_DataIn());
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", PY);
    }
    
    public void create_Data_Postgres_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres2CSV() + script.CSV2HDFS() + script.HDFS2Hive() + script.bottom_sh(); 
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    
    public void create_Data_Postgres_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres2CSV() + script.CSV2HDFS() + script.HDFS2Parquet(Data) + script.bottom_sh();
        ParquetCSV convertCSV2Parquet = new ParquetCSV();
        String PY = convertCSV2Parquet.CSV2Parquet(Data.get_DataIn());
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", PY);
    }
    
    public void create_Data_Spark_Postgres(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Parquet2CSV(Data) + script.CSV2Posgres() + script.bottom_sh();
        ParquetCSV convertParquet2CSV = new ParquetCSV();
        String PY = convertParquet2CSV.Parquet2CSV();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", PY);
    }
    
    public void create_Data_Spark_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Parquet2CSV(Data) + script.CSV2HDFS() + script.HDFS2Hive() + script.bottom_sh();
        ParquetCSV convertParquet2CSV = new ParquetCSV();
        String PY = convertParquet2CSV.Parquet2CSV();
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", PY);
    } 
    public void create_Data_SQL_Postgres(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres_SQL(SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);
        
    }   
    public void create_Data_SQL_Hive(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh_remote(Data) + script.Hive_SQL(Data, SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp +"_remote"+ ".sh", sh);
        
    }
    public void create_Data_SQL_Hive_remote(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.HIVE2CSV_remote() + script.bottom_sh();//Hive_SQL_remote(Data, SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);       
    }
    public void create_Data_SQL_Postgres_remote(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres_SQL_remote(Data, SQL) + script.bottom_sh();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);       
    }
    public void createfile(String OperatorFolder,String filename, String content){        
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
    public void create_Start_Workflow(Move_Data Data){        
        Measure measure = new Measure();
        String Start = measure.StartMeasure();
        String NameOp = Nameop(Data);
        createfile(OperatorFolder + "/" + NameOp, "Start_"+NameOp + ".sh", Start);
    }
    public void create_Stop_Workflow(Move_Data Data, double size){        
        Measure measure = new Measure();
        String Stop = measure.StopMeasure(Data,size);
        String NameOp = Nameop(Data);
        createfile(OperatorFolder + "/" + NameOp, "Stop_"+NameOp + ".sh", Stop);
    }
    public void create_SQL_Postgres(Move_Data Data, String SQL){
        String NameOp = Nameop(Data);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);
        
    }
    public void create_SQL_Hive(Move_Data Data, String SQL){
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
}
