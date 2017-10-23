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
        
        String InPutData1 = table1+"_"+Op1;
        String InPutData2 = table2+"_"+Op6;
        String InPutData3 = table1+"_out_"+Op1;
        String InPutData4 = table2+"_out_"+Op6;
	String InPutData5 = table1+table2+"_"+Op4;

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
	//d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
	d1.inputFor(mop1, 0); 
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData1);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp1 = new AbstractOperator(AbstractOp1);//AopAbstractOperator);              
        File filename1 = new File(directory_library + "abstractOperators/" + abstractOp1.opName);
        abstractOp1.readPropertiesFromFile(filename1);
	ocli.addAbstractOperator(abstractOp1);
	
	WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp1);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp1);
             
        Operator mop2 = new Operator(Op6,directory_operator+Op6);       
        mop2.readFromDir();
	Dataset d2 = new Dataset(InPutData2);        
        d2.readPropertiesFromFile(directory_datasets+InPutData2);
        materializedDatasets.add(d2);
	//d2.writeToPropertiesFile(directory_datasets + d2.datasetName);
	d2.inputFor(mop2, 0);       
        WorkflowNode t2 = new WorkflowNode(false,false,InPutData2);
	t2.setDataset(d2);
        
        AbstractOperator abstractOp2 = new AbstractOperator(AbstractOp6);//AopAbstractOperator);              
        File filename2 = new File(directory_library + "abstractOperators/" + abstractOp2.opName);
        abstractOp2.readPropertiesFromFile(filename2);
        ocli.addAbstractOperator(abstractOp2);

	WorkflowNode op2 = new WorkflowNode(true,true,AbstractOp6);//AopAbstractOperator);
	op2.setAbstractOperator(abstractOp2);
                
        Operator mop3 = new Operator(Op4,directory_operator+Op4);        
        mop3.readFromDir();
	Dataset d3 = new Dataset(InPutData3);
        d3.readPropertiesFromFile(directory_datasets+InPutData3);
        materializedDatasets.add(d3);
	//d3.writeToPropertiesFile(directory_datasets + d3.datasetName);
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
	d4.inputFor(mop3, 1);
	//d4.writeToPropertiesFile(directory_datasets + d4.datasetName);
        WorkflowNode t4 = new WorkflowNode(false,true,InPutData4);
	t4.setDataset(d4);
        
        Dataset d5 = new Dataset(InPutData5);
        d5.readPropertiesFromFile(directory_datasets+InPutData5);
        materializedDatasets.add(d5);
	//d5.writeToPropertiesFile(directory_datasets + d5.datasetName);
	d5.outputFor(mop3, 0);	
        WorkflowNode t5 = new WorkflowNode(false,true,InPutData5);
	t5.setDataset(d5);
        
        AbstractOperator abstractOp3 = new AbstractOperator(AbstractOp4);//AopAbstractOperator);              
        File filename3 = new File(directory_library + "abstractOperators/" + abstractOp3.opName);
        abstractOp3.readPropertiesFromFile(filename3);
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

	//abstractWorkflow1.writeToDir(directory_workflow + NameOfAbstractWorkflow);
/////////////////////////////////////////////////////////////////////////////        
        String NameOfWorkflow = NameOfAbstractWorkflow;
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
        String materializedWorkflow = cli.materializeWorkflow(NameOfWorkflow, policy);
	//executeWorkflow(NameOfWorkflow);
	//materializeWorkflow(NameOfWorkflow, policy);
        System.out.println(materializedWorkflow);
        cli.executeWorkflow(materializedWorkflow);
        cli.executeWorkflow(NameOfAbstractWorkflow);
        
    }
    public static void main() throws Exception {

		ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
		WorkflowClient cli = new WorkflowClient();
		cli.setConfiguration(conf);
		
		cli.removeAbstractWorkflow("pagerank");
		
		AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1("pagerank");
		
		Dataset d1 = new Dataset("graph");
		WorkflowNode t1 = new WorkflowNode(false,false,"graph");
		t1.setDataset(d1);

		AbstractOperator abstractOp = new AbstractOperator("PageRank");
		WorkflowNode op1 = new WorkflowNode(true,true,"TF_IDF");
		op1.setAbstractOperator(abstractOp);
		//abstractOp.writeToPropertiesFile(abstractOp.opName);

		AbstractOperator abstractOp1 = new AbstractOperator("k-Means");
		WorkflowNode op2 = new WorkflowNode(true,true,"k-Means");
		op2.setAbstractOperator(abstractOp1);
		//abstractOp1.writeToPropertiesFile(abstractOp1.opName);
		
		Dataset d2 = new Dataset("d2");
		WorkflowNode t2 = new WorkflowNode(false,true,"d2");
		t2.setDataset(d2);
		
		Dataset d3 = new Dataset("d3");
		WorkflowNode t3 = new WorkflowNode(false,true,"d3");
		t3.setDataset(d3);
		
		op1.addInput(0,t1);
		op1.addOutput(0, t2);
		
		t2.addInput(0,op1);
		
		op2.addInput(0,t2);
		op2.addOutput(0,t3);
		
		t3.addInput(0,op2);
		abstractWorkflow.addTarget(t3);
		
		cli.addAbstractWorkflow(abstractWorkflow);
		//cli.removeAbstractWorkflow("abstractTest1");
		
		String policy ="metrics,cost,execTime\n"+
						"groupInputs,execTime,max\n"+
						"groupInputs,cost,sum\n"+
						"function,2*execTime+3*cost,min";
		
		String materializedWorkflow = cli.materializeWorkflow("abstractTest1", policy);
		System.out.println(materializedWorkflow);
		//cli.executeWorkflow(materializedWorkflow);
		
	}
   public static void workflow () throws Exception{
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
        String InPutData3 = InPutData1+"_out";
        String InPutData4 = InPutData2+"_out";
	String InPutData5 = Op4+"_"+InPutData3+InPutData4;

        String AbstractOp1 = "Abstract_"+Op1;
        String AbstractOp2 = "Abstract_"+Op2;
        String AbstractOp3 = "Abstract_"+Op3;
        String AbstractOp4 = "Abstract_"+Op4;
        String AbstractOp5 = "Abstract_"+Op5;
        String AbstractOp6 = "Abstract_"+Op6;
        
        String NameOfAbstractWorkflow = "Workflow";
        String Size_tpch = "100m";
        String database = "tpch";
        
        double TimeOfDay = 0;
        
        String From ="Hive";
        String To = "Postgres";
        String Operator = "Move";// +"_"+ randomQuery[2];                
        String DataIn = table1;
        String DataInSize = Double.toString(100);       
        String DatabaseIn = database + Size_tpch;
        String Schema = Schema(DataIn);
        String DataOut = DataIn.toUpperCase();
        String DataOutSize = DataInSize;
        String DatabaseOut = DatabaseIn;
        
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        String SQL = "";
        
        double[] size = new double[numberOfSize_Move_Hive_Postgres];
                size[0] = testQueryPlan.sizeDataset(table1,Size_tpch);
                size[1] = testQueryPlan.pageDataset(table1,Size_tpch);
                size[2] = testQueryPlan.tupleDataset(table1,Size_tpch);
                size[3] = 0;
        YarnValue yarnValue = new YarnValue(1024, 1);
        yarnValue.set_Ram(1024);
        yarnValue.set_Core(1);        
                
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        IRES.createOperatorMove(Data, SQL, 10);
        IRES.createDatasetMove_Hive_Postgres(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
        IRES.createDataMove2(Data, SQL, yarnValue);
        IRES.createAbstractOperatorMove(Data, SQL);
        
        From ="Postgres";
        To = "Postgres";
        Operator = "Move";// +"_"+ randomQuery[2];                
        DataIn = table2;
        DataInSize = Double.toString(1000);       
        DatabaseIn = database + Size_tpch;
        Schema = Schema(DataIn);
        DataOut = DataIn.toUpperCase();
        DataOutSize = DataInSize;
        DatabaseOut = DatabaseIn;
        
        //Move_Data Data2 = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        SQL = "";
        
        double[] size2 = new double[numberOfSize_Postgres_Postgres];
                size2[0] = testQueryPlan.sizeDataset(table1,Size_tpch);
                size2[1] = testQueryPlan.pageDataset(table1,Size_tpch);
                size2[2] = testQueryPlan.tupleDataset(table1,Size_tpch);
                size2[3] = testQueryPlan.pageDataset(table2,Size_tpch);
                size2[4] = testQueryPlan.tupleDataset(table2,Size_tpch);
                size2[5] = 0;       
                
        //IRES = new runWorkFlowIRES();
        IRES.createOperatorMove(Data, SQL, 10);
        IRES.createDatasetMove_Hive_Postgres(Data, size2, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
        IRES.createDataMove2(Data, SQL, yarnValue);
        IRES.createAbstractOperatorMove(Data, SQL);
        
        
        From ="Postgres";
        To = "Postgres";
        Operator = "Join";// +"_"+ randomQuery[2];                
        DataIn = table1;
        DataInSize = Double.toString(1000);       
        DatabaseIn = database + Size_tpch;
        Schema = Schema(DataIn);
        DataOut = table2;
        DataOutSize = DataInSize;
        DatabaseOut = DatabaseIn;
        
        //Move_Data Data2 = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        String SQL_folder = new App().readhome("SQL");
        String query = "query4";
        String SQL_fileName = SQL_folder + query;
        SQL = "DROP TABLE IF EXISTS "+ query+"_"+From+"_"+To+"; "
                + "CREATE TABLE "+ query+"_"+From+"_"+To+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
        
        double[] size3 = new double[numberOfSize_Postgres_Postgres];
                size3[0] = testQueryPlan.sizeDataset(table1,Size_tpch);
                size3[1] = testQueryPlan.pageDataset(table1,Size_tpch);
                size3[2] = testQueryPlan.tupleDataset(table1,Size_tpch);
                size3[3] = testQueryPlan.pageDataset(table2,Size_tpch);
                size3[4] = testQueryPlan.tupleDataset(table2,Size_tpch);
                size3[5] = 0;       
                
        //IRES = new runWorkFlowIRES();
        IRES.createOperatorJoin(Data, SQL, 10);
        //IRES.createDatasetJoin(Data, size3, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
        //IRES.createDataMove2(Data, SQL, yarnValue);
        //IRES.createAbstractOperatorMove(Data, SQL);
        
//        createWorkflowJoin();
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
	WorkflowClient cli = new WorkflowClient();
	cli.setConfiguration(conf);
        
//        cli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
        AbstractWorkflow1 abstractWorkflow = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        
        Operator mop1 = new Operator(Op1,"");       
        Dataset d1 = new Dataset(InPutData1);        
        d1.inputFor(mop1, 0); 
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData1);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp1 = new AbstractOperator(AbstractOp1);//AopAbstractOperator);              
        WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp1);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp1);
             
        Operator mop2 = new Operator(Op1,"");       
        Dataset d2 = new Dataset(InPutData2);        
        d2.inputFor(mop2, 0);       
        WorkflowNode t2 = new WorkflowNode(false,false,InPutData2);
	t2.setDataset(d2);
        
        AbstractOperator abstractOp2 = new AbstractOperator(AbstractOp6);//AopAbstractOperator);              
        WorkflowNode op2 = new WorkflowNode(true,true,AbstractOp6);//AopAbstractOperator);
	op2.setAbstractOperator(abstractOp2);
                
        Operator mop3 = new Operator(Op4,"");        
        Dataset d3 = new Dataset(InPutData3);
        d3.inputFor(mop3, 0);        
	WorkflowNode t3 = new WorkflowNode(false,true,InPutData3);
	t3.setDataset(d3);
        
        Dataset d4 = new Dataset(InPutData4);
        d4.inputFor(mop3, 1);	
        WorkflowNode t4 = new WorkflowNode(false,true,InPutData4);
	t4.setDataset(d4);
        
        Dataset d5 = new Dataset(InPutData5);
        d5.outputFor(mop3, 0);	
        WorkflowNode t5 = new WorkflowNode(false,true,InPutData5);
	t5.setDataset(d5);
        
        AbstractOperator abstractOp3 = new AbstractOperator(AbstractOp4);//AopAbstractOperator);              
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
        abstractWorkflow1.addInputEdge(d1,abstractOp1,0);
        abstractWorkflow1.addOutputEdge(abstractOp1,d3,0);
        
        abstractWorkflow1.addInputEdge(d2,abstractOp2,0);
        abstractWorkflow1.addOutputEdge(abstractOp2,d4,0);
               
        abstractWorkflow1.addInputEdge(d3,abstractOp3,0);
        abstractWorkflow1.addInputEdge(d4,abstractOp3,1);
        abstractWorkflow1.addOutputEdge(abstractOp3,d5,0);
	
        abstractWorkflow1.getWorkflow(d5);
/////////////////////////////////////////////////////////////////////////////        
        String NameOfWorkflow = NameOfAbstractWorkflow;
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
        String materializedWorkflow = cli.materializeWorkflow(NameOfWorkflow, policy);
        System.out.println(materializedWorkflow);
        //cli.executeWorkflow(materializedWorkflow);    
    } 
}
