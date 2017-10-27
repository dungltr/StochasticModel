/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import Algorithms.Algorithms;
import Algorithms.testWriteMatrix2CSV;
import static IRES.TPCHQuery.Schema;
import static IRES.TPCHQuery.calculateSize;
import static IRES.TPCHQuery.readSQL;
import static IRES.runWorkFlowIRES.Nameop;
import static IRES.runWorkFlowIRES.copydata;
import static IRES.runWorkFlowIRES.datasetin;
import static IRES.runWorkFlowIRES.datasetout;
import static IRES.testQueryPlan.createRandomQuery;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.WorkflowNode;
import java.nio.file.Files;
import java.io.File;
import gr.ntua.cslab.asap.workflow.Workflow;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import org.apache.commons.io.FileUtils;
import java.util.Locale;

import java.io.FileInputStream;
import java.io.InputStream;
/**
 *
 * @author letrung
 */
public class TestWorkFlow {
    static int int_localhost = 1323;
    static String name_host = "localhost";
    static String IRES_HOME = new App().readhome("IRES_HOME");
    static String ASAP_HOME = IRES_HOME;
    static String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    static String directory_library = IRES_library+"/target/asapLibrary/";
    static String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    static String directory_datasets = IRES_library+"/target/asapLibrary/datasets/";
    static String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    static String directory_workflow = IRES_library+"/target/asapLibrary/workflows/";
    private static int numberOfTmp = 5;
    private static int YarnParamter = 2;
    private static int numberOfSize = 3;
    
    private static int numberOfSize_Hive_Postgres = 6;
    private static int numberOfSize_Postgres_Hive = 5;
    private static int numberOfSize_Postgres_Postgres = 6;
    private static int numberOfSize_Hive_Hive = 3;
    
    private static int numberOfSize_Move_Hive_Hive = 2;   
    private static int numberOfSize_Move_Hive_Postgres = 4;
    private static int numberOfSize_Move_Postgres_Hive = 4;
    private static int numberOfSize_Move_Postgres_Postgres = 4;
	
    private static List<Operator> operators;
     public static void copydata(String AbstractWorkflow, String NameMaterialize) throws IOException{
        String folderName = OperatorFolder;// + NameOp;
        String folderWorkflow = directory_workflow + NameMaterialize+"/operators";
        String folderAbstract = directory_library +"abstractWorkflows/"+ AbstractWorkflow + "/operators";
        
        File folderSource1 = new File(folderName);
        File[] listOfSource1 = folderSource1.listFiles();
        
        File folderSource2 = new File(folderAbstract);
        File[] listOfSource2 = folderSource2.listFiles();
        
        File folderDest = new File(folderWorkflow);
        
             
            //File srcDir = new File(listOfOperators[j].toString()+"/data");  
        for (int i = 0; i < listOfSource2.length; i++) {
 	       for (int j = 0; j < listOfSource1.length; j++) {
		if (listOfSource1[j].isDirectory()&&listOfSource2[i].toString()
                        .replace(folderAbstract, "")
                        .contains(listOfSource1[j].toString()
                                .replace(folderName, ""))){                   
                    //File destDir = new File(listOfOperatorsDest[i].toString()+"/data");
                    //FileUtils.copyDirectory(srcDir, destDir);
                    FileUtils.copyDirectory(FileUtils.getFile(listOfSource1[j]
                            .toString()), 
                            FileUtils.getFile(folderDest
                                    .toString()+"/"+listOfSource1[j].toString()
                                .replace(folderName, "")+"_"+i));
/*                    System.out.println("Source: "+listOfOperators[j].toString()+" and " 
                            + listOfOperators[j].toString()
                            .replace(folderWorkflow+"/operators/", ""));
                    System.out.println("Destination: "+listOfOperatorsDest[i].toString() + " and " 
                            +listOfOperatorsDest[i].toString()
                            .replace(folderName, ""));
*/                }
            }
        }
    }
    public static void createWorkflowJoin() throws Exception{
	operators = new ArrayList<Operator>();
	String table1 = "orders";
        String table2 = "lineitem";
        
        String Op1 = "Move_TPCH_Hive_Postgres";
        String Op2 = "Move_TPCH_Postgres_Hive";
        String Op3 = "Join_TPCH_Hive_Hive";
        String Op4 = "Join_TPCH_Postgres_Postgres";
        String Op5 = "Move_TPCH_Hive_Hive";
        String Op6 = "Move_TPCH_Postgres_Postgres";
        
        String InPutData1 = Op1+"_"+table1;
        String InPutData2 = Op6+"_"+table2;
        String InPutData3 = Op1+"_"+table1.toUpperCase();
        String InPutData4 = Op6+"_"+table2.toUpperCase();
	String InPutData5 = Op4+"_"+table1.toUpperCase()+table2.toUpperCase();

        String AbstractOp1 = "Abstract"+"_"+Op1;
        String AbstractOp2 = "Abstract"+"_"+Op2;
        String AbstractOp3 = "Abstract"+"_"+Op3;
        String AbstractOp4 = "Abstract"+"_"+Op4;
        String AbstractOp5 = "Abstract"+"_"+Op5;
        String AbstractOp6 = "Abstract"+"_"+Op6;
        
        String NameOfAbstractWorkflow = "Test_Workflow";
      
	List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();
        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
	WorkflowClient cli = new WorkflowClient();
	cli.setConfiguration(conf);

	OperatorClient ocli = new OperatorClient();		
        ocli.setConfiguration(conf);
        
//        cli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        
        Operator mop1 = new Operator(Op1,"");//directory_operator+Op1);       
//        mop1.readFromDir();
//	mop1.writeToPropertiesFile(directory_operator+"Op1");
//	mop1.writeModels(directory_operator+"Op1");
//	mop1.reConfigureModel();
	Dataset d1 = new Dataset(InPutData1);        
//      d1.readPropertiesFromFile(directory_datasets+InPutData1);
       materializedDatasets.add(d1); 

//	String filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d1.datasetName;
//        d1.writeToPropertiesFile(filedataset);

	d1.inputFor(mop1, 0); 
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData1);
	t1.setDataset(d1);
 
        AbstractOperator abstractOp1 = new AbstractOperator(AbstractOp1);//AopAbstractOperator);              
/*	abstractOp1.add("Constraints.Engine", "Postgres");
        abstractOp1.add("Constraints.Input.number","1");
	abstractOp1.add("Constraints.OpSpecification.Algorithm.name","move");
	abstractOp1.add("Constraints.Output.number", "1");
        ocli.addAbstractOperator(abstractOp1);
/*	File filename1 = new File(directory_library + "abstractOperators/" + abstractOp1.opName);
        abstractOp1.readPropertiesFromFile(filename1);
//        abstractOp1.writeToPropertiesFile(directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/operators/" + abstractOp1.opName);     
	ocli.addAbstractOperator(abstractOp1);
*/	
	WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp1);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp1);
             
        Operator mop2 = new Operator(Op6,directory_operator+Op6);       
//        mop2.readFromDir();
//	mop2.writeToPropertiesFile(directory_operator+"Op2");
//        mop2.writeModels(directory_operator+"Op2");
//        mop2.reConfigureModel();
	
	Dataset d2 = new Dataset(InPutData2);        
//        d2.readPropertiesFromFile(directory_datasets+InPutData2);
        materializedDatasets.add(d2);
//	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d2.datasetName;
//        d2.writeToPropertiesFile(filedataset);
//	d2.writeToPropertiesFile(directory_datasets + d2.datasetName);
	d2.inputFor(mop2, 0);       
        WorkflowNode t2 = new WorkflowNode(false,false,InPutData2);
	t2.setDataset(d2);
        
        AbstractOperator abstractOp2 = new AbstractOperator(AbstractOp6);//AopAbstractOperator);              
        ////////
/*        abstractOp2.add("Constraints.Engine", "Postgres");
        abstractOp2.add("Constraints.Input.number","1");
	abstractOp2.add("Constraints.OpSpecification.Algorithm.name","move");
	abstractOp2.add("Constraints.Output.number", "1");
        ocli.addAbstractOperator(abstractOp2);
        //////// 
/*	File filename2 = new File(directory_library + "abstractOperators/" + abstractOp2.opName);
        abstractOp2.readPropertiesFromFile(filename2);
//	abstractOp2.writeToPropertiesFile(directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/operators/" + abstractOp2.opName);   
        ocli.addAbstractOperator(abstractOp2);
*/
	WorkflowNode op2 = new WorkflowNode(true,true,AbstractOp6);//AopAbstractOperator);
	op2.setAbstractOperator(abstractOp2);
                
        Operator mop3 = new Operator(Op4,directory_operator+Op4);        
//        mop3.readFromDir();
//	mop3.writeToPropertiesFile(directory_operator+"Op3");
//        mop3.writeModels(directory_operator+"Op3");
//        mop3.reConfigureModel();

	Dataset d3 = new Dataset("d3");//InPutData3);
//        d3.readPropertiesFromFile(directory_datasets+InPutData3);
        materializedDatasets.add(d3);
//	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d3.datasetName;
//        d3.writeToPropertiesFile(filedataset);//d3.writeToPropertiesFile(directory_datasets + d3.datasetName);
//	d3.add("Constraints.Engine.SQL","PostgresMove_TPCH");
//        d3.add("Constraints.type","SQL");
//	d3.add("Execution.name",table1);
	d3.outputFor(mop1,0);
	d3.inputFor(mop3, 0);        
	WorkflowNode t3 = new WorkflowNode(false,true,"d3");//InPutData3);
	t3.setDataset(d3);
/*	
	operators.add(mop1);
	operators.add(mop2);
	operators.add(mop3);	
        ocli.addOperator(mop1);
	ocli.addOperator(mop2);
	ocli.addOperator(mop3);
*/
//        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
//	mop2.writeToPropertiesFile(directory_operator+mop2.opName);
//	mop3.writeToPropertiesFile(directory_operator+mop3.opName);
	Dataset d4 = new Dataset("d4");//InPutData4);
//        d4.readPropertiesFromFile(directory_datasets+InPutData4);
        materializedDatasets.add(d4);
//	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d4.datasetName;
//        d4.writeToPropertiesFile(filedataset);
/*	d4.add("Constraints.Engine.SQL","PostgresMove_TPCH");
        d4.add("Constraints.type","SQL");
	d4.add("Execution.name",table2);
*/	d3.outputFor(mop2,0);
	d4.inputFor(mop3, 1);
//	d4.writeToPropertiesFile(directory_datasets + d4.datasetName);
        WorkflowNode t4 = new WorkflowNode(false,true,"d4");//InPutData4);
	t4.setDataset(d4);
        
        Dataset d5 = new Dataset("d5");//InPutData5);
//        d5.readPropertiesFromFile(directory_datasets+InPutData5);
        materializedDatasets.add(d5);
//	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d5.datasetName;

//        d5.writeToPropertiesFile(filedataset);
//	d5.writeToPropertiesFile(directory_datasets + d5.datasetName);
/*	d5.add("Constraints.Engine.SQL","PostgresJoin_TPCH");
        d5.add("Constraints.type","SQL");
	d5.add("Execution.name",table1+table2);
*/	
	d5.outputFor(mop3, 0);	
        WorkflowNode t5 = new WorkflowNode(false,true,"d5");//InPutData5);
	t5.setDataset(d5);
        
        AbstractOperator abstractOp3 = new AbstractOperator(AbstractOp4);//AopAbstractOperator);              
/*	abstractOp3.add("Constraints.Engine", "Postgres");
        abstractOp3.add("Constraints.Input.number","1");
	abstractOp3.add("Constraints.OpSpecification.Algorithm.name","join");
	abstractOp3.add("Constraints.Output.number", "1");
        ocli.addAbstractOperator(abstractOp3);
/*        File filename3 = new File(directory_library + "abstractOperators/" + abstractOp3.opName);
        abstractOp3.readPropertiesFromFile(filename3);
//	abstractOp3.writeToPropertiesFile(directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/operators/" + abstractOp3.opName);   
        ocli.addAbstractOperator(abstractOp3);
*/	WorkflowNode op3 = new WorkflowNode(true,true,AbstractOp4);//AopAbstractOperator);
	op3.setAbstractOperator(abstractOp3);

        t1.addOutput(0,op1);
        t2.addOutput(0,op2);
                
	op1.addInput(0,t1);
        op2.addInput(0,t2);
        op1.addOutput(0,t3);
        op2.addOutput(0,t4);
        
        t3.addInput(0,op1);
        t4.addInput(0,op2);
        t3.addOutput(0,op3);
        t4.addOutput(0,op3);
        
	op3.addInput(0,t3);
        op3.addInput(1,t4);
        op3.addOutput(0,t5);
		
	t5.addInput(0,op3);

	abstractWorkflow.addTarget(t5);
//        cli.addAbstractWorkflow(abstractWorkflow);
        
        WorkflowClient wcli = new WorkflowClient();
	wcli.setConfiguration(conf);    
        wcli.addAbstractWorkflow(abstractWorkflow);

// To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow1 = new AbstractWorkflow(library);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets); 
	abstractWorkflow1.addInputEdge(d1,abstractOp1,0);
        abstractWorkflow1.addOutputEdge(abstractOp1,d3,0);
        
        abstractWorkflow1.addInputEdge(d2,abstractOp2,0);
        abstractWorkflow1.addOutputEdge(abstractOp2,d4,0);
               
        abstractWorkflow1.addInputEdge(d3,abstractOp3,0);
        abstractWorkflow1.addInputEdge(d4,abstractOp3,1);
        abstractWorkflow1.addOutputEdge(abstractOp3,d5,0);
	
        abstractWorkflow1.getWorkflow(d5);

//	abstractWorkflow1.writeToDir(directory_library + "abstractWorkflows/" + NameOfAbstractWorkflow);

        String NameOfWorkflow = NameOfAbstractWorkflow;
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
//	abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
        String materializedWorkflow = cli.materializeWorkflow(NameOfWorkflow, policy);
        copydata(NameOfAbstractWorkflow, materializedWorkflow);

	System.out.println(abstractWorkflow1);
//	abstractWorkflow1.writeToDir(directory_library+"/workflows/workflow1");	
/*        filedataset = directory_library + "workflows/"+materializedWorkflow+"/datasets/" + d1.datasetName;
        d1.writeToPropertiesFile(filedataset);
	filedataset = directory_library + "workflows/"+materializedWorkflow+"/datasets/" + d2.datasetName;
        d2.writeToPropertiesFile(filedataset);
	filedataset = directory_library + "workflows/"+materializedWorkflow+"/datasets/" + d3.datasetName;
        d3.writeToPropertiesFile(filedataset);
	filedataset = directory_library + "workflows/"+materializedWorkflow+"/datasets/" + d4.datasetName;
        d4.writeToPropertiesFile(filedataset);
	filedataset = directory_library + "workflows/"+materializedWorkflow+"/datasets/" + d5.datasetName;
        d5.writeToPropertiesFile(filedataset);

	abstractOp1.writeToPropertiesFile(directory_library + "workflows/"+materializedWorkflow+"/operators/" + abstractOp1.opName);   
	abstractOp2.writeToPropertiesFile(directory_library + "workflows/"+materializedWorkflow+"/operators/" + abstractOp2.opName);   
	abstractOp3.writeToPropertiesFile(directory_library + "workflows/"+materializedWorkflow+"/operators/" + abstractOp3.opName);   
	copydata(NameOfWorkflow,materializedWorkflow);
*/	
	Workflow workflow0 = abstractWorkflow1.getWorkflow(d5);

        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        System.out.println(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");

	System.out.println(materializedWorkflow);
//	abstractWorkflow1.writeToDir(directory_library+"/workflows/"+materializedWorkflow);
	
        Workflow workflow1 = abstractWorkflow1.optimizeWorkflow(d5);
        System.out.println("\nShowing of optimize workflow is here----------------------------------------------------------------:");
        System.out.println(workflow1);
        System.out.println("\nShowing of optimize workflow is ended--------------------------------------------------------------:");

//	workflow1.writeToDir(directory_library+"abstractWorkflows/Optimization"+NameOfWorkflow);
//	workflow1.writeToDir(directory_library+"workflows/Optimization"+NameOfWorkflow);
	//cli.executeWorkflow(materializedWorkflow);
        //cli.executeWorkflow(NameOfAbstractWorkflow);
        
    }
    public static void main(String args[]) throws Exception {
	createWorkflowJoin();
//	smallworkflow();
	workflow();
	}
public static void workflow() throws Exception   
        {
	ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient ocli = new OperatorClient();		
        ocli.setConfiguration(conf);
                
	WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);

        String NameOfAbstractWorkflow = "Workflow";
        List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();	

        //cli.removeAbstractWorkflow("pagerank");

        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);
/*		String dirWorkflow = directory_library+"workflows/smallworkflow";
        File directoryWorkflows = new File(dirWorkflow);
        if (!directoryWorkflows.exists()) {
                directoryWorkflows.mkdir();
                System.out.println("\nCreate directory:-------------------------"+dirWorkflow);
        }

        String dirDatasets = dirWorkflow + "/datasets";        
        File dirDataset = new File(dirDatasets);
        if (!dirDataset.exists()) {
                dirDataset.mkdir();
                System.out.println("\nCreate directory:-------------------------"+dirDatasets);
        }
        String dirOperators = dirWorkflow + "/operators";        
        File dirOperator = new File(dirOperators);
        if (!dirOperator.exists()) {
                dirOperator.mkdir();
                System.out.println("\nCreate directory:-------------------------"+dirOperators);
        }
*/      Operator mop1 = new Operator("Operator1","");

	File initialFile = new File(directory_operator+"Join_TPCH_Postgres_Postgres"+"/description");
        InputStream targetStream = new FileInputStream(initialFile);
        mop1.readPropertiesFromStream(targetStream);
	mop1.writeToPropertiesFile(directory_operator+"Join_TPCH_Postgres_Postgres");
	ocli.addOperator(mop1);

        System.out.println(mop1.toString());
	
	Dataset d11 = new Dataset("Input11");
//        d1.readPropertiesFromFile(directory_datasets+"Move_TPCH_Hive_Postgres_orders");
	d11.add("Execution.schema", "");//;=(O_ORDERKEY       INT,O_CUSTKEY        INT,O_ORDERSTATUS    CHAR(1),O_TOTALPRICE     DECIMAL(15,2),O_ORDERDATE      DATE ,O_ORDERPRIORITY  CHAR(15),O_CLERK          CHAR(15),O_SH$
	d11.add("Optimization.tuple","150000.0");
	d11.add("Optimization.page","2610.0");
	d11.add("Execution.path","hdfs://"+"/user/hive/warehouse"+"/"+"tpch100"+".db/"+"orders");
	d11.add("Optimization.random","0.9849681994073411");
	d11.add("Execution.name","orders");
	d11.add("Optimization.size","16.1");
        
	materializedDatasets.add(d11);
        WorkflowNode t11 = new WorkflowNode(false,false,"Input11");
        t11.setDataset(d11);
	d11.inputFor(mop1, 0);
	System.out.println(d11.toString());
	
	d11.writeToPropertiesFile(directory_datasets + d11.datasetName);
//		String filedataset = directory_library + "abstractWorkflows/smallworkflow/datasets/" + d1.datasetName;
//		d1.writeToPropertiesFile(filedataset);
//		filedataset = dirDatasets +"/" + d1.datasetName;
//		d1.writeToPropertiesFile(filedataset);

        AbstractOperator abstractOp = new AbstractOperator("Abstract_Operator1");
        File filename = new File(directory_library + "abstractOperators/" + "Abstract_Join_TPCH_Postgres_Postgres");
      	abstractOp.readPropertiesFromFile(filename);
	System.out.println(abstractOp.toString());
	ocli.addAbstractOperator(abstractOp);
	abstractOp.writeToPropertiesFile(directory_library + "abstractOperators/" + abstractOp.opName);
//        	abstractOp.writeToPropertiesFile(directory_library + "abstractWorkflows/smallworkflow/operators/" + abstractOp.opName);     
//        	abstractOp.writeToPropertiesFile(dirOperators + "/" + abstractOp.opName);     
        WorkflowNode op1 = new WorkflowNode(true,true,abstractOp.opName);
        op1.setAbstractOperator(abstractOp);
        //abstractOp.writeToPropertiesFile(abstractOp.opName);

//        AbstractOperator abstractOp1 = new AbstractOperator("Abstract_Move_TPCH_Postgres_Postgres");
//                File filename1 = new File(directory_library + "abstractOperators/" + abstractOp1.opName);
//        	abstractOp1.readPropertiesFromFile(filename1);
//        	abstractOp1.writeToPropertiesFile(directory_library + "abstractWorkflows/smallworkflow/operators/" + abstractOp1.opName);     
//        	abstractOp1.writeToPropertiesFile(dirOperators + "/" + abstractOp1.opName);     
//        WorkflowNode op2 = new WorkflowNode(true,true,abstractOp1.opName);
//        op2.setAbstractOperator(abstractOp1);
        //abstractOp1.writeToPropertiesFile(abstractOp1.opName);

        Dataset d22 = new Dataset("Input22");
//        d2.readPropertiesFromFile(directory_datasets+"Move_TPCH_Hive_Postgres_customer");
        d22.add("Execution.schema", "");//;=(O_ORDERKEY       INT,O_CUSTKEY        INT,O_ORDERSTATUS    CHAR(1),O_TOTALPRICE     DECIMAL(15,2),O_ORDERDATE      DATE ,O_ORDERPRIORITY  CHAR(15),O_CLERK          CHAR(15),O_SH$
	d22.add("Optimization.tuple","15000.0");
	d22.add("Optimization.page","360.0");
	d22.add("Execution.path","hdfs://"+"/user/hive/warehouse"+"/"+"tpch100"+".db/"+"orders");
	d22.add("Optimization.random","0.45513538999004943");
	d22.add("Execution.name","customer");
	d22.add("Optimization.size","2.3");
	
	materializedDatasets.add(d22);
        WorkflowNode t22 = new WorkflowNode(false,false,"Input22");
        t22.setDataset(d22);
	System.out.println(d22.toString());
	d22.inputFor(mop1, 1);
	d22.writeToPropertiesFile(directory_datasets + d22.datasetName);
//		filedataset = directory_library + "abstractWorkflows/smallworkflow/datasets/" + d2.datasetName;
//                d2.writeToPropertiesFile(filedataset);
//		filedataset = dirDatasets + "/" + d2.datasetName;
//                d2.writeToPropertiesFile(filedataset);

        Dataset d33 = new Dataset("d33");
        WorkflowNode t33 = new WorkflowNode(false,true,"d33");
	materializedDatasets.add(d33);
        t33.setDataset(d33);
	d33.outputFor(mop1, 0);

        t11.addOutput(0,op1);
        t22.addOutput(0,op1);

        op1.addInput(0,t11);
        op1.addInput(1,t22);
        op1.addOutput(0,t33);

	t33.addInput(0,op1);

        abstractWorkflow.addTarget(t33);

        wcli.addAbstractWorkflow(abstractWorkflow);
        //cli.removeAbstractWorkflow("abstractTest1");

        String policy ="metrics,cost,execTime\n"+
                                        "groupInputs,execTime,max\n"+
                                        "groupInputs,cost,sum\n"+
                                        "function,2*execTime+3*cost,min";
    // To show in Materialized Workflow
/*        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow1 = new AbstractWorkflow(library);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets); 
        abstractWorkflow1.addInputEdge(d1,abstractOp,0);
        abstractWorkflow1.addInputEdge(d2,abstractOp,1);
        abstractWorkflow1.addOutputEdge(abstractOp,d3,0);

        abstractWorkflow1.getWorkflow(d3);

//          abstractWorkflow1.writeToDir(directory_library + "workflows/" + "smallworkflow");
/////////////////////////////////////////////////////////////////////////////                        
        String materializedWorkflow = wcli.materializeWorkflow(NameOfAbstractWorkflow, policy);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
        System.out.println(abstractWorkflow1);
        System.out.println(materializedWorkflow);
*/
//        wcli.executeWorkflow(materializedWorkflow);
    }
    
	public static void smallworkflow() throws Exception   
{
String NameOfAbstractWorkflow = "Workflow";
List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();	
		ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
                WorkflowClient cli = new WorkflowClient();
                cli.setConfiguration(conf);
                
                //cli.removeAbstractWorkflow("pagerank");
                
                AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1("smallworkflow");
		String dirWorkflow = directory_library+"workflows/smallworkflow";
                File directoryWorkflows = new File(dirWorkflow);
		if (!directoryWorkflows.exists()) {
			directoryWorkflows.mkdir();
                        System.out.println("\nCreate directory:-------------------------"+dirWorkflow);
		}
                
        	String dirDatasets = dirWorkflow + "/datasets";        
        	File dirDataset = new File(dirDatasets);
		if (!dirDataset.exists()) {
			dirDataset.mkdir();
                        System.out.println("\nCreate directory:-------------------------"+dirDatasets);
		}
        	String dirOperators = dirWorkflow + "/operators";        
        	File dirOperator = new File(dirOperators);
		if (!dirOperator.exists()) {
			dirOperator.mkdir();
                        System.out.println("\nCreate directory:-------------------------"+dirOperators);
		}
                Dataset d1 = new Dataset("orders_1G");
                d1.readPropertiesFromFile(directory_datasets+"orders_1G");
        	materializedDatasets.add(d1);
		WorkflowNode t1 = new WorkflowNode(false,false,"orders_1G");
                t1.setDataset(d1);
		String filedataset = directory_library + "abstractWorkflows/smallworkflow/datasets/" + d1.datasetName;
		d1.writeToPropertiesFile(filedataset);
		filedataset = dirDatasets +"/" + d1.datasetName;
		d1.writeToPropertiesFile(filedataset);

                AbstractOperator abstractOp = new AbstractOperator("Filter_Join");
                File filename = new File(directory_library + "abstractOperators/" + abstractOp.opName);
        	abstractOp.readPropertiesFromFile(filename);
        	abstractOp.writeToPropertiesFile(directory_library + "abstractWorkflows/smallworkflow/operators/" + abstractOp.opName);     
        	abstractOp.writeToPropertiesFile(dirOperators + "/" + abstractOp.opName);     
		WorkflowNode op1 = new WorkflowNode(true,true,"Filter_Join");
                op1.setAbstractOperator(abstractOp);
                //abstractOp.writeToPropertiesFile(abstractOp.opName);

                AbstractOperator abstractOp1 = new AbstractOperator("GroupBy_Sort");
                File filename1 = new File(directory_library + "abstractOperators/" + abstractOp1.opName);
        	abstractOp1.readPropertiesFromFile(filename1);
        	abstractOp1.writeToPropertiesFile(directory_library + "abstractWorkflows/smallworkflow/operators/" + abstractOp1.opName);     
        	abstractOp1.writeToPropertiesFile(dirOperators + "/" + abstractOp1.opName);     
		WorkflowNode op2 = new WorkflowNode(true,true,"GroupBy_Sort");
                op2.setAbstractOperator(abstractOp1);
                //abstractOp1.writeToPropertiesFile(abstractOp1.opName);
                
                Dataset d2 = new Dataset("customers");
                d2.readPropertiesFromFile(directory_datasets+"customers");
        	materializedDatasets.add(d2);
                WorkflowNode t2 = new WorkflowNode(false,false,"customers");
		t2.setDataset(d2);
		filedataset = directory_library + "abstractWorkflows/smallworkflow/datasets/" + d2.datasetName;
                d2.writeToPropertiesFile(filedataset);
		filedataset = dirDatasets + "/" + d2.datasetName;
                d2.writeToPropertiesFile(filedataset);
                
                Dataset d3 = new Dataset("d3");
                WorkflowNode t3 = new WorkflowNode(false,true,"d3");
                t3.setDataset(d3);
		filedataset = directory_library + "abstractWorkflows/smallworkflow/datasets/" + d3.datasetName;
                d3.writeToPropertiesFile(filedataset);
		filedataset = dirDatasets + "/" + d3.datasetName;
                d3.writeToPropertiesFile(filedataset);

		Dataset d4 = new Dataset("d4");
                WorkflowNode t4 = new WorkflowNode(false,true,"d4");
                t4.setDataset(d4);
		filedataset = directory_library + "abstractWorkflows/smallworkflow/datasets/" + d4.datasetName;
                d4.writeToPropertiesFile(filedataset);
		filedataset = dirDatasets + "/" + d4.datasetName;
                d4.writeToPropertiesFile(filedataset);

		t1.addOutput(0,op1);
		t2.addOutput(0,op1);
                
                op1.addInput(0,t1);
                op1.addInput(1,t2);
                op1.addOutput(0,t3);
                
		t3.addInput(0,op1);
                t3.addOutput(0,op2);

	        op2.addInput(0,t3);
                op2.addOutput(0,t4);
                
                t4.addInput(0,op2);
                abstractWorkflow.addTarget(t4);
                
                cli.addAbstractWorkflow(abstractWorkflow);
                //cli.removeAbstractWorkflow("abstractTest1");
                
                String policy ="metrics,cost,execTime\n"+
                                                "groupInputs,execTime,max\n"+
                                                "groupInputs,cost,sum\n"+
                                                "function,2*execTime+3*cost,min";
		// To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow1 = new AbstractWorkflow(library);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets); 
        abstractWorkflow1.addInputEdge(d1,abstractOp,0);
        abstractWorkflow1.addInputEdge(d2,abstractOp,1);
        abstractWorkflow1.addOutputEdge(abstractOp,d3,0);
        
        abstractWorkflow1.addInputEdge(d3,abstractOp1,0);
        abstractWorkflow1.addOutputEdge(abstractOp1,d4,0);
               
        abstractWorkflow1.getWorkflow(d4);

        abstractWorkflow1.writeToDir(directory_library + "workflows/" + "smallworkflow");
/////////////////////////////////////////////////////////////////////////////                        
                String materializedWorkflow = cli.materializeWorkflow("smallworkflow", policy);
		abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
        	System.out.println(abstractWorkflow1);
                System.out.println(materializedWorkflow);
                //cli.executeWorkflow(materializedWorkflow);
    } 
}
