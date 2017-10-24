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

/**
 *
 * @author letrung
 */
public class TestWorkFlow {
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
    public static void Preapre(double TimeOfDay, String DB, String Size, String from, String to, String Join) throws Exception {
        String Size_tpch = Size;
        String database = DB;
        String KindOfRunning = "training";
        String SQL_folder = new App().readhome("SQL");
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery(KindOfRunning, Size_tpch);
        String From = from;
        String To   = to;
        
        double[] size = calculateSize(randomQuery, From, To, Size_tpch, KindOfRunning);
	double[] Yarn = testQueryPlan.createRandomYarn();
        ////////////////////////////////////////////
        size[size.length-1]=TimeOfDay;
        ///////////////////////////////////////////       
        String Operator = Join+"_TPCH";// +"_"+ randomQuery[2];           
        //String DataIn = Table;      
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        
        String DatabaseIn = database + Size_tpch;
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[3]; 
        String DataOutSize = Double.toString(size[1]); 
        if (to.toLowerCase().equals("postgres")) DataOutSize = Double.toString(testQueryPlan.tupleDataset(randomQuery[3],Size_tpch));
        String DatabaseOut = database + Size_tpch;       
       
        String SQL_fileName = ""; 
        if (KindOfRunning.equals("training"))
	SQL_fileName = SQL_folder + randomQuery[2];
        else {
		if (To.toLowerCase().equals("postgres")) SQL_fileName = SQL_folder + randomQuery[2];
		else SQL_fileName = SQL_folder+randomQuery[2]; 
        }
	String SQL = "";
        SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
 //               + "drop table " + Table + ";";//SQL_Postgres;
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
               
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
        Data.set_DataOut(DataOut);
        Data.set_DataOutSize(DataOutSize);
        
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        
        YarnValue yarnValue = new YarnValue(Yarn[0], Yarn[1]);
        yarnValue.set_Ram(Yarn[0]);
        yarnValue.set_Core(Yarn[1]);
                
        String realValue, parameter, estimate, directory;
        directory = testWriteMatrix2CSV.getDirectory(Data);
        String delay_ys = "";
	if (TimeOfDay<1) delay_ys = "no_delay_";
        realValue = directory + "/data/"+delay_ys+KindOfRunning+"_realValue.csv";
        Path filePathRealValue = Paths.get(realValue); 
        if (!Files.exists(filePathRealValue))
        {   Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,KindOfRunning);
            IRES.createOperatorJoin(Data, SQL, 0);                       
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay, size, KindOfRunning);
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
        
//	String OutputData1 = InPutData1 + "_OUT";
//        String OutputData2 = InPutData2 + "_OUT";
        
//        String OutputData3 = "data3";//table1+table2+Op4;

        String NameOfAbstractWorkflow = "Test_Workflow";
      
//        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
//        OperatorClient cli = new OperatorClient();		
//        cli.setConfiguration(conf);
	List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();
        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
	WorkflowClient cli = new WorkflowClient();
	cli.setConfiguration(conf);

	OperatorClient ocli = new OperatorClient();		
        ocli.setConfiguration(conf);
        
//        cli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        
        Operator mop1 = new Operator(Op1,directory_operator+Op1);       
        mop1.readFromDir();
	Dataset d1 = new Dataset(InPutData1);        
        d1.readPropertiesFromFile(directory_datasets+InPutData1);
        materializedDatasets.add(d1); 
	String filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d1.datasetName;
        d1.writeToPropertiesFile(filedataset);


	d1.inputFor(mop1, 0); 
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData1);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp1 = new AbstractOperator(AbstractOp1);//AopAbstractOperator);              
	File filename1 = new File(directory_library + "abstractOperators/" + abstractOp1.opName);
        abstractOp1.readPropertiesFromFile(filename1);
        abstractOp1.writeToPropertiesFile(directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/operators/" + abstractOp1.opName);     
	ocli.addAbstractOperator(abstractOp1);
	
	WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp1);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp1);
             
        Operator mop2 = new Operator(Op6,directory_operator+Op6);       
        mop2.readFromDir();
	Dataset d2 = new Dataset(InPutData2);        
        d2.readPropertiesFromFile(directory_datasets+InPutData2);
        materializedDatasets.add(d2);
	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d2.datasetName;
        d2.writeToPropertiesFile(filedataset);
	d2.writeToPropertiesFile(directory_datasets + d2.datasetName);
	d2.inputFor(mop2, 0);       
        WorkflowNode t2 = new WorkflowNode(false,false,InPutData2);
	t2.setDataset(d2);
        
        AbstractOperator abstractOp2 = new AbstractOperator(AbstractOp6);//AopAbstractOperator);              
        File filename2 = new File(directory_library + "abstractOperators/" + abstractOp2.opName);
        abstractOp2.readPropertiesFromFile(filename2);
	abstractOp2.writeToPropertiesFile(directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/operators/" + abstractOp2.opName);   
        ocli.addAbstractOperator(abstractOp2);

	WorkflowNode op2 = new WorkflowNode(true,true,AbstractOp6);//AopAbstractOperator);
	op2.setAbstractOperator(abstractOp2);
                
        Operator mop3 = new Operator(Op4,directory_operator+Op4);        
        mop3.readFromDir();
	Dataset d3 = new Dataset(InPutData3);
        d3.readPropertiesFromFile(directory_datasets+InPutData3);
        materializedDatasets.add(d3);
	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d3.datasetName;
        d3.writeToPropertiesFile(filedataset);//d3.writeToPropertiesFile(directory_datasets + d3.datasetName);
	d3.inputFor(mop3, 0);        
	WorkflowNode t3 = new WorkflowNode(false,true,InPutData3);
	t3.setDataset(d3);
	
	operators.add(mop1);
	operators.add(mop2);
	operators.add(mop3);	
        ocli.addOperator(mop1);
	ocli.addOperator(mop2);
	ocli.addOperator(mop3);
        //mop1.writeToPropertiesFile(directory_operator+mop1.opName);
	//mop2.writeToPropertiesFile(directory_operator+mop2.opName);
	//mop3.writeToPropertiesFile(directory_operator+mop3.opName);
	Dataset d4 = new Dataset(InPutData4);
        d4.readPropertiesFromFile(directory_datasets+InPutData4);
        materializedDatasets.add(d4);
	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d4.datasetName;
        d4.writeToPropertiesFile(filedataset);
	d4.inputFor(mop3, 1);
	d4.writeToPropertiesFile(directory_datasets + d4.datasetName);
        WorkflowNode t4 = new WorkflowNode(false,true,InPutData4);
	t4.setDataset(d4);
        
        Dataset d5 = new Dataset(InPutData5);
        d5.readPropertiesFromFile(directory_datasets+InPutData5);
        materializedDatasets.add(d5);
	filedataset = directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/datasets/" + d5.datasetName;
        d5.writeToPropertiesFile(filedataset);
	d5.writeToPropertiesFile(directory_datasets + d5.datasetName);
	d5.outputFor(mop3, 0);	
        WorkflowNode t5 = new WorkflowNode(false,true,InPutData5);
	t5.setDataset(d5);
        
        AbstractOperator abstractOp3 = new AbstractOperator(AbstractOp4);//AopAbstractOperator);              
        File filename3 = new File(directory_library + "abstractOperators/" + abstractOp3.opName);
        abstractOp3.readPropertiesFromFile(filename3);
	abstractOp3.writeToPropertiesFile(directory_library + "abstractWorkflows/"+NameOfAbstractWorkflow+"/operators/" + abstractOp3.opName);   
        ocli.addAbstractOperator(abstractOp3);
	WorkflowNode op3 = new WorkflowNode(true,true,AbstractOp4);//AopAbstractOperator);
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
        cli.addAbstractWorkflow(abstractWorkflow);
        
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

	abstractWorkflow1.writeToDir(directory_workflow + NameOfAbstractWorkflow);
/////////////////////////////////////////////////////////////////////////////        
        String NameOfWorkflow = NameOfAbstractWorkflow;
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
        String materializedWorkflow = cli.materializeWorkflow(NameOfWorkflow, policy);
	abstractWorkflow1.addMaterializedDatasets(materializedDatasets);
	System.out.println(abstractWorkflow1);
//	abstractWorkflow1.writeToDir(directory_library+"/workflows/workflow1");	
        //cli.executeWorkflow("workflow1");
//executeWorkflow(NameOfWorkflow);
	//materializeWorkflow(NameOfWorkflow, policy);
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
*/	
	System.out.println(materializedWorkflow);
	abstractWorkflow1.writeToDir(directory_library+"/workflows/"+materializedWorkflow);	
        Workflow workflow1 = abstractWorkflow1.optimizeWorkflow(d5);
		System.out.println(workflow1);
		workflow1.writeToDir(directory_library+"abstractWorkflows/Optimization"+NameOfWorkflow);
		workflow1.writeToDir(directory_library+"workflow/Optimization"+NameOfWorkflow);
	//cli.executeWorkflow(materializedWorkflow);
        //cli.executeWorkflow(NameOfAbstractWorkflow);
        
    }
    public static void main() throws Exception 
{
	createWorkflowJoin();
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
                d1.readPropertiesFromFile(directory_datasets+"customers");
        	materializedDatasets.add(d1);
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
                
                String materializedWorkflow = cli.materializeWorkflow("smallworkflow", policy);
                System.out.println(materializedWorkflow);
                //cli.executeWorkflow(materializedWorkflow);
    } 
}
