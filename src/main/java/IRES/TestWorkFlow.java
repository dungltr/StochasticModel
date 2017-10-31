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
import java.io.FileNotFoundException;
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
    private static int numberOfSize_Postgres_Postgres = 7;
    private static int numberOfSize_Hive_Hive = 3;
    
    private static int numberOfSize_Move_Hive_Hive = 2;   
    private static int numberOfSize_Move_Hive_Postgres = 4;
    private static int numberOfSize_Move_Postgres_Hive = 4;
    private static int numberOfSize_Move_Postgres_Postgres = 4;
	
    private static List<Operator> operators;
     public static void copydata(String AbstractWorkflow, String NameMaterialize) throws IOException{
        String folderName = OperatorFolder;// + NameOp;
        String folderWorkflow = directory_workflow + NameMaterialize+"/operators";
        
        File folderSource1 = new File(folderName);
        File[] listOfSource1 = folderSource1.listFiles();
        
        File folderSource2 = new File(folderWorkflow);
        File[] listOfSource2 = folderSource2.listFiles();
        
        File folderDest = new File(folderWorkflow);
  
        for (int i = 0; i < listOfSource2.length; i++) {
 	       for (int j = 0; j < listOfSource1.length; j++) {
		if (listOfSource1[j].isDirectory()&&listOfSource2[i].toString()
                        .replace(folderWorkflow, "")
                        .contains(listOfSource1[j].toString()
                                .replace(folderName, ""))){                   
                    FileUtils.copyDirectory(FileUtils.getFile(listOfSource1[j]
                            .toString()), 
                            FileUtils.getFile(folderDest
                                    .toString()+"/"+listOfSource2[i].toString()
                                .replace(folderWorkflow, "")));
                }
            }
        }
    }
    public static void workflowMove(Move_Data Data, String Size_tpch, String SQL, YarnValue yarnValue, String KindOfMoving, String KindOfRunning) throws Exception   
        {    
        String oldName = KindOfMoving+"_TPCH_"+Data.get_From()+"_"+Data.get_To();    
        String newName = "Operator_"+KindOfMoving+"_"+Data.get_From()+"_"+Data.get_To();
        String Abstract ="Abstract"; 
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient ocli = new OperatorClient();             
        ocli.setConfiguration(conf);
                
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        
        runWorkFlowIRES IRES = new runWorkFlowIRES();

        String NameOfAbstractWorkflow = newName+"_Workflow";
        List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();        

        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);

        IRES.createDataMove2(Data, SQL, yarnValue);
        String new_OP1 = newName;
        String new_Abstract_OP1 = Abstract+"_"+new_OP1;
        String old_OP1 = oldName;
        Operator mop1 = setupOperator(new_OP1, old_OP1);
                       
        System.out.println(mop1.toString());
 	String DataIn = Data.get_From()+"_"+Data.get_DatabaseIn()+"_"+Data.get_DataIn();	
	Dataset d11 = new Dataset(DataIn);
        d11.readPropertiesFromFile(directory_datasets+DataIn);
        System.out.println(d11.toString());
//        d11.writeToPropertiesFile(directory_datasets + d11.datasetName);

        materializedDatasets.add(d11);

        WorkflowNode t11 = new WorkflowNode(false,false,"Input"+new_OP1);
        t11.setDataset(d11);
        d11.inputFor(mop1, 0);
        AbstractOperator abstractOp1 = setupAbstractOperator(new_Abstract_OP1, old_OP1);
	
	WorkflowNode op1 = new WorkflowNode(true,true,abstractOp1.opName);
        op1.setAbstractOperator(abstractOp1);
 	

        Dataset d33 = new Dataset("Output"+new_OP1);

        materializedDatasets.add(d33);

        WorkflowNode t33 = new WorkflowNode(false,true,d33.datasetName);
	t33.setDataset(d33);
        d33.outputFor(mop1, 0);

        t11.addOutput(0,op1);
//        t22.addOutput(0,op1);

        op1.addInput(0,t11);
//        op1.addInput(1,t22);
        op1.addOutput(0,t33);

        t33.addInput(0,op1);
 	abstractWorkflow.addTarget(t33);
        wcli.addAbstractWorkflow(abstractWorkflow);
        String policy ="metrics,cost,execTime\n"+
                                        "groupInputs,execTime,max\n"+
                                        "groupInputs,cost,sum\n"+
                                        "function,2*execTime+3*cost,min";
    // To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();
        AbstractWorkflow abstractWorkflow1 = new AbstractWorkflow(library);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets); 
        abstractWorkflow1.addInputEdge(d11,abstractOp1,0);
        abstractWorkflow1.addOutputEdge(abstractOp1,d33,0);
        abstractWorkflow1.getWorkflow(d33);

        String materializedWorkflow = wcli.materializeWorkflow(NameOfAbstractWorkflow, policy);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
        copydata(NameOfAbstractWorkflow, materializedWorkflow);
        System.out.println(abstractWorkflow1);
        System.out.println(materializedWorkflow);

        Workflow workflow0 = abstractWorkflow1.getWorkflow(d33);
        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        System.out.println(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");


        Workflow workflow1 = abstractWorkflow1.optimizeWorkflow(d33);
        System.out.println("\nShowing of optimize workflow is here----------------------------------------------------------------:");
        System.out.println(workflow1);
        System.out.println("\nShowing of optimize workflow is ended--------------------------------------------------------------:");
	
        String[] randomQuery = createRandomQuery(KindOfRunning, Size_tpch);    
        double[] size = calculateSize(randomQuery, Data.get_From(), Data.get_To(), Size_tpch, KindOfMoving);

	double Time_Cost = IRES.runWorkflow(Data, size, NameOfAbstractWorkflow, policy);
        //wcli.executeWorkflow(materializedWorkflow);
    }
    public static void workflowJoin(Move_Data Data, String Size_tpch, String SQL, YarnValue yarnValue, String KindOfMoving, String KindOfRunning) throws Exception   
        {
        String oldName = KindOfMoving+"_TPCH_"+Data.get_From()+"_"+Data.get_To();    
        String newName = "Operator_"+KindOfMoving+"_"+Data.get_From()+"_"+Data.get_To();
        String Abstract ="Abstract"; 
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient ocli = new OperatorClient();             
        ocli.setConfiguration(conf);
                
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        
        runWorkFlowIRES IRES = new runWorkFlowIRES();

        String NameOfAbstractWorkflow = newName+"_Workflow";
        List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();        

        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);
        
        IRES.createDataMove2(Data, SQL, yarnValue);
        
/*        Operator mop1 = new Operator("Operator_"+KindOfRunning,"");
        String Dest = directory_operator + mop1.opName;
        String Source = directory_operator + OP1; 
        File initialFile = new File(directory_operator + OP1 + "/description");
        InputStream targetStream = new FileInputStream(initialFile);
        mop1.readPropertiesFromStream(targetStream);
        mop1.add("Execution.LuaScript",mop1.opName+".lua");  
        FileUtils.copyDirectory(FileUtils.getFile(directory_operator+OP1), 
                            FileUtils.getFile(directory_operator+mop1.opName));
        FileUtils.copyFile(FileUtils.getFile(directory_operator+OP1+"/"+OP1+".lua"), 
                            FileUtils.getFile(directory_operator+mop1.opName+"/"+mop1.opName+".lua"));
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);    
        ocli.addOperator(mop1);
*/        
        String new_OP1 = newName;
        String new_Abstract_OP1 = Abstract+"_"+new_OP1;
        String old_OP1 = oldName;
        Operator mop1 = setupOperator(new_OP1, old_OP1);
        System.out.println(mop1.toString());
 	
        String DataIn1 = Data.get_From()+"_"+Data.get_DatabaseIn()+"_"+Data.get_DataIn();	
	Dataset d1 = new Dataset(DataIn1);
        d1.readPropertiesFromFile(directory_datasets+DataIn1);
        System.out.println(d1.toString());
        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
        materializedDatasets.add(d1);

        String DataIn2 = Data.get_From()+"_"+Data.get_DatabaseIn()+"_"+Data.get_DataOut();	
	Dataset d2 = new Dataset(DataIn2);
        d2.readPropertiesFromFile(directory_datasets+DataIn2);
        System.out.println(d2.toString());
        d2.writeToPropertiesFile(directory_datasets + d2.datasetName);
        materializedDatasets.add(d2);
        
        WorkflowNode t1 = new WorkflowNode(false,false,"Input1"+new_OP1);
        t1.setDataset(d1);
        d1.inputFor(mop1, 0);
        
        WorkflowNode t2 = new WorkflowNode(false,false,"Input2"+new_OP1);
        t2.setDataset(d2);
        d2.inputFor(mop1, 1);
        AbstractOperator abstractOp1 = setupAbstractOperator(new_Abstract_OP1, old_OP1);
/*        AbstractOperator abstractOp1 = new AbstractOperator("Abstract_Operator_"+KindOfRunning);
        File filename1 = new File(directory_library + "abstractOperators/Abstract_" + OP1);
        abstractOp1.readPropertiesFromFile(filename1);
        System.out.println(abstractOp1.toString());
        ocli.addAbstractOperator(abstractOp1);
        abstractOp1.writeToPropertiesFile(directory_library + "abstractOperators/" + abstractOp1.opName);
*/	
	WorkflowNode op1 = new WorkflowNode(true,true,abstractOp1.opName);
        op1.setAbstractOperator(abstractOp1);
 	
//        Dataset d3 = new Dataset("d3");
        Dataset d3 = new Dataset("Output"+new_OP1);
//        d33.readPropertiesFromFile(directory_datasets+);
//        System.out.println(d33.toString());
//        d33.writeToPropertiesFile(directory_datasets + d33.datasetName);

        materializedDatasets.add(d3);

        WorkflowNode t3 = new WorkflowNode(false,true,d3.datasetName);
	t3.setDataset(d3);
        d3.outputFor(mop1, 0);

        t1.addOutput(0,op1);
        t2.addOutput(0,op1);

        op1.addInput(0,t1);
        op1.addInput(1,t2);
        op1.addOutput(0,t3);

        t3.addInput(0,op1);
 	abstractWorkflow.addTarget(t3);
        wcli.addAbstractWorkflow(abstractWorkflow);
        String policy ="metrics,cost,execTime\n"+
                                        "groupInputs,execTime,max\n"+
                                        "groupInputs,cost,sum\n"+
                                        "function,2*execTime+3*cost,min";
    // To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();
        AbstractWorkflow abstractWorkflow1 = new AbstractWorkflow(library);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets); 
        abstractWorkflow1.addInputEdge(d1,abstractOp1,0);
        abstractWorkflow1.addInputEdge(d2,abstractOp1,1);
        abstractWorkflow1.addOutputEdge(abstractOp1,d3,0);
        abstractWorkflow1.getWorkflow(d3);

        String materializedWorkflow = wcli.materializeWorkflow(NameOfAbstractWorkflow, policy);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
        copydata(NameOfAbstractWorkflow, materializedWorkflow);
        System.out.println(abstractWorkflow1);
        System.out.println(materializedWorkflow);

        Workflow workflow0 = abstractWorkflow1.getWorkflow(d3);
        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        System.out.println(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");


        Workflow workflow1 = abstractWorkflow1.optimizeWorkflow(d3);
        System.out.println("\nShowing of optimize workflow is here----------------------------------------------------------------:");
        System.out.println(workflow1);
        System.out.println("\nShowing of optimize workflow is ended--------------------------------------------------------------:");
	
        String[] randomQuery = createRandomQuery(KindOfRunning, Size_tpch);    
        double[] size = calculateSize(randomQuery, Data.get_From(), Data.get_To(), Size_tpch, KindOfRunning);

	double Time_Cost = IRES.runWorkflow(Data, size, NameOfAbstractWorkflow, policy);
        //wcli.executeWorkflow(materializedWorkflow);
    }
    public static Operator setupOperator(String newOP, String OP_Source) throws FileNotFoundException, IOException, Exception{
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient ocli = new OperatorClient();             
        ocli.setConfiguration(conf);
        Operator mop1 = new Operator(newOP,"");
        File initialFile = new File(directory_operator + OP_Source + "/description");
        InputStream targetStream = new FileInputStream(initialFile);
        mop1.readPropertiesFromStream(targetStream);
        mop1.add("Execution.LuaScript",mop1.opName+".lua"); 
        mop1.add("Optimization.model.execTime",  "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        FileUtils.copyDirectory(FileUtils.getFile(directory_operator+OP_Source), 
                            FileUtils.getFile(directory_operator+mop1.opName));
        FileUtils.copyFile(FileUtils.getFile(directory_operator+OP_Source+"/"+OP_Source+".lua"), 
                            FileUtils.getFile(directory_operator+mop1.opName+"/"+mop1.opName+".lua"));
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);    
        ocli.addOperator(mop1);
        System.out.println(mop1.toString());
        return mop1;
    }
    public static AbstractOperator setupAbstractOperator(String new_AbstractOp, String OP_Source) throws IOException, Exception{
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient ocli = new OperatorClient();             
        ocli.setConfiguration(conf);
        AbstractOperator abstractOp = new AbstractOperator(new_AbstractOp);
        File filename1 = new File(directory_library + "abstractOperators/Abstract_" + OP_Source);
        abstractOp.readPropertiesFromFile(filename1);
        System.out.println(abstractOp.toString());
        ocli.addAbstractOperator(abstractOp);
        abstractOp.writeToPropertiesFile(directory_library + "abstractOperators/" + abstractOp.opName);
        return abstractOp;
    }
    public static void workflowJoinMove(Move_Data Data, String KindOfRunning, String Size_tpch, String SQL, YarnValue yarnValue) throws Exception   
        {
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient ocli = new OperatorClient();             
        ocli.setConfiguration(conf);
                
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        
        runWorkFlowIRES IRES = new runWorkFlowIRES();

        String NameOfAbstractWorkflow = "Join_Move"+"_"+Data.get_From()+"_"+Data.get_To()+"_Workflow";
        List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();        

        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);
        
        
        String temp = Data.get_To();
        Data.set_To(Data.get_From());
        IRES.createDataMove2(Data, SQL, yarnValue);
        String new_OP1 = "Operator_"+"Join";
        String new_Abstract_OP1 = "Abstract_"+new_OP1;
        String old_OP1 = "Join_TPCH_"+Data.get_From()+"_"+Data.get_To();
        Operator mop1 = setupOperator(new_OP1, old_OP1);
        
        String Operator = "Move_TPCH";
        Data.set_To(temp);
        Data.set_Operator(Operator);
        IRES.createDataMove2(Data, SQL, yarnValue);        
        String new_OP2 = "Operator_"+"Move";
        String new_Abstract_OP2 = "Abstract_"+new_OP2;
        String old_OP2 = "Move_TPCH_"+Data.get_From()+"_"+Data.get_To();
        Operator mop2 = setupOperator(new_OP2, old_OP2);
//////////////////////////  	        
        String DataIn1 = Data.get_From()+"_"+Data.get_DatabaseIn()+"_"+Data.get_DataIn();	
	Dataset d1 = new Dataset(DataIn1);
        d1.readPropertiesFromFile(directory_datasets+DataIn1);
        System.out.println(d1.toString());
        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
        materializedDatasets.add(d1);

        String DataIn2 = Data.get_From()+"_"+Data.get_DatabaseIn()+"_"+Data.get_DataOut();	
	Dataset d2 = new Dataset(DataIn2);
        d2.readPropertiesFromFile(directory_datasets+DataIn2);
        System.out.println(d2.toString());
        d2.writeToPropertiesFile(directory_datasets + d2.datasetName);
        materializedDatasets.add(d2);
        
        WorkflowNode t1 = new WorkflowNode(false,false,"Input11");
        t1.setDataset(d1);
        d1.inputFor(mop1, 0);
        
        WorkflowNode t2 = new WorkflowNode(false,false,"Input22");
        t2.setDataset(d2);
        d2.inputFor(mop1, 1);
        
        AbstractOperator abstractOp1 = setupAbstractOperator(new_Abstract_OP1, old_OP1);
        AbstractOperator abstractOp2 = setupAbstractOperator(new_Abstract_OP2, old_OP2);
	
	WorkflowNode op1 = new WorkflowNode(true,true,abstractOp1.opName);
        op1.setAbstractOperator(abstractOp1);
        
        WorkflowNode op2 = new WorkflowNode(true,true,abstractOp2.opName);
        op2.setAbstractOperator(abstractOp2);
 	
        String DataIn3 = Data.get_From()+"_"+Data.get_DatabaseIn()+"_"+Data.get_DataIn()+"_"+Data.get_DataOut();	
	Dataset d3 = new Dataset(DataIn3);
        d3.readPropertiesFromFile(directory_datasets+DataIn3);
        
//        d3.add("Constraints.Engine.SQL", "HiveMove_TPCH");
        if (Data.get_To().toLowerCase().equals("postgres")){
                d3.add("Optimization.page","10");
                d3.add("Optimization.tuple","10");           
            }
        d3.add("Optimization.random","1");
        System.out.println(d3.toString());
        d3.writeToPropertiesFile(directory_datasets + d3.datasetName);
        materializedDatasets.add(d3);
//        d33.readPropertiesFromFile(directory_datasets+);
//        System.out.println(d33.toString());
//        d33.writeToPropertiesFile(directory_datasets + d33.datasetName);
        d3.outputFor(mop1, 0);
        d3.inputFor(mop2, 0);
        
        Dataset d4 = new Dataset("d4");
        materializedDatasets.add(d4);
        d4.outputFor(mop2, 0);

        WorkflowNode t3 = new WorkflowNode(false,true,d3.datasetName);
	t3.setDataset(d3);
        WorkflowNode t4 = new WorkflowNode(false,true,d4.datasetName);
	t4.setDataset(d4);
        
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
        
        
        wcli.addAbstractWorkflow(abstractWorkflow);
        String policy ="metrics,cost,execTime\n"+
                                        "groupInputs,execTime,max\n"+
                                        "groupInputs,cost,sum\n"+
                                        "function,2*execTime+3*cost,min";
    // To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();
        AbstractWorkflow abstractWorkflow1 = new AbstractWorkflow(library);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets); 
        abstractWorkflow1.addInputEdge(d1,abstractOp1,0);
        abstractWorkflow1.addInputEdge(d2,abstractOp1,1);
        abstractWorkflow1.addOutputEdge(abstractOp1,d3,0);
        abstractWorkflow1.addInputEdge(d3,abstractOp2,0);
        abstractWorkflow1.addOutputEdge(abstractOp2,d4,0);
        abstractWorkflow1.getWorkflow(d4);

        String materializedWorkflow = wcli.materializeWorkflow(NameOfAbstractWorkflow, policy);
        abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
        copydata(NameOfAbstractWorkflow, materializedWorkflow);
        System.out.println(abstractWorkflow1);
        System.out.println(materializedWorkflow);

        Workflow workflow0 = abstractWorkflow1.getWorkflow(d4);
        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        System.out.println(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");


        Workflow workflow1 = abstractWorkflow1.optimizeWorkflow(d4);
        System.out.println("\nShowing of optimize workflow is here----------------------------------------------------------------:");
        System.out.println(workflow1);
        System.out.println("\nShowing of optimize workflow is ended--------------------------------------------------------------:");
	
        String[] randomQuery = createRandomQuery(KindOfRunning, Size_tpch);    
        double[] size = calculateSize(randomQuery, Data.get_From(), Data.get_To(), Size_tpch, KindOfRunning);
	double Time_Cost = IRES.runWorkflow(Data, size, NameOfAbstractWorkflow, policy);
    } 
    public static void main(String args[]) throws Exception {
//	smallworkflow();
//	workflow();
	}
}
