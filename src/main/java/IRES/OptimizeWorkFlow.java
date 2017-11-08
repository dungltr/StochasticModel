/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import static IRES.runWorkFlowIRES.datasetout;
import LibraryIres.Move_Data;
import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.Workflow;
import java.util.ArrayList;
import java.util.List;
import net.sourceforge.jeval.EvaluationException;

/**
 *
 * @author letrung
 */
public class OptimizeWorkFlow {
    static int int_localhost = 1323;
    static String name_host = "localhost";
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
    
    public static void OptimizeWorkFlow(Move_Data Data, String policy) throws Exception {
        List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();
        Dataset d1 = new Dataset(Data.get_DataIn());
        Dataset d2 = new Dataset(Data.get_DataOut());
        materializedDatasets.add(d1);
        materializedDatasets.add(d2);
        
        MaterializedOperators library =  new MaterializedOperators();
        AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
        AbstractOperator abstractOp = new AbstractOperator(runWorkFlowIRES.AbstractOp(Data));
        
        abstractWorkflow.addInputEdge(d1,abstractOp,0);
        abstractWorkflow.addOutputEdge(abstractOp,d2,0);
        abstractWorkflow.getWorkflow(d2);
        
        System.out.println("\nShowing of abstractWorkflow is here----------------------------------------------------------------:");
        abstractWorkflow.addMaterializedDatasets(materializedDatasets);               
        System.out.println("\n----------------------------------------------------------------:");
        System.out.println(abstractWorkflow.getWorkflow(d1));
        System.out.println("\nShowing of abstractWorkflow is finished------------------------------------------------------------:");
		
        materializedDatasets.add(d2);                
        
        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        Workflow workflow0 = abstractWorkflow.getWorkflow(d2);
        System.out.println("\n----------------------------------------------------------------:");        
        System.out.print(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");

        Workflow workflow1 = abstractWorkflow.optimizeWorkflow(d2);
        System.out.println("\nHere is optimization workflow is here-----------------------------------------------------------------------:");
        System.out.println(workflow1);
//	System.out.println(workflow1.toString());
        System.out.println("\nEnd of optimization workflow------------------------------------------------------------------------:");
        System.out.println();        
//
//        System.out.println("\nCall for the new workflow******************************************************************************************--------:");
//        TestWorkFlow(Data, policy);
/*  
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        Double Cost = workflow1.optimalCost;
	///////////////////////////////////////////////////////////////////////////////////////
	//String materializedWorkflow = wcli.materializeWorkflow(workflow1.toString(), policy);
	///////////////////////////////////////////////////////////////////////////////////////
        //System.out.println(materializedWorkflow);
        System.out.println("The cost of workflow of-------------------"+workflow1.toString()+" is "+ Cost+"-------------------------");
	System.out.println("----------------------------"+workflow1.toString());
        ////Execution 
        //String w = wcli.executeWorkflow(materializedWorkflow);
*//////////////////////////////////////////////////////////////////////////////////////// 
    }
    public static void TestWorkFlow(Move_Data Data, String policy) throws Exception {
	String Table1 = "orders";
	String Table2 = "lineitem";
        String Op1 = "Move_TPCH_Hive_Postgres";
        String Op2 = "Move_TPCH_Postgres_Hive";
        String Op3 = "Join_TPCH_Hive_Hive";
        String Op4 = "Join_TPCH_Postgres_Postgres";
        String Op5 = "Move_TPCH_Hive_Hive";
        String Op6 = "Move_TPCH_Postgres_Postgres";
        
        String InPutData1 = Table1+"_"+Op1;
        String InPutData2 = Table2+"_"+Op6;
	String InPutData3 = Table1+"_out_"+Op1;
	String InPutData4 = Table2+"_out_"+Op6;
        String InPutData5 = InPutData3+InPutData4+"_"+Op4;
        String AbstractOp1 = "Abstract_"+Op1;
        String AbstractOp2 = "Abstract_"+Op2;
        String AbstractOp3 = "Abstract_"+Op3;
        String AbstractOp4 = "Abstract_"+Op4;
        String AbstractOp5 = "Abstract_"+Op5;
        String AbstractOp6 = "Abstract_"+Op6;
        
        String NameOfAbstractWorkflow = "Test_Workflow";
        List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();
        Dataset d1 = new Dataset(InPutData1);
        Dataset d2 = new Dataset(InPutData2);
        
        Dataset d3 = new Dataset(InPutData3);
        Dataset d4 = new Dataset(InPutData4);
        Dataset d5 = new Dataset(InPutData5);
        
        materializedDatasets.add(d1);
        materializedDatasets.add(d2);
	materializedDatasets.add(d3);
	materializedDatasets.add(d4);
	materializedDatasets.add(d5);        
        
	MaterializedOperators library =  new MaterializedOperators();
        AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
        
        AbstractOperator abstractOp1 = new AbstractOperator(AbstractOp1);
        AbstractOperator abstractOp2 = new AbstractOperator(AbstractOp6);
        AbstractOperator abstractOp3 = new AbstractOperator(AbstractOp4);
        
        abstractWorkflow.addInputEdge(d1,abstractOp1,0);
        abstractWorkflow.addOutputEdge(abstractOp1,d3,0);
        
        abstractWorkflow.addInputEdge(d2,abstractOp2,0);
        abstractWorkflow.addOutputEdge(abstractOp2,d4,0);
        
        abstractWorkflow.addInputEdge(d3,abstractOp3,0);
        abstractWorkflow.addInputEdge(d4,abstractOp3,1);
        abstractWorkflow.addOutputEdge(abstractOp3,d5,0);

        abstractWorkflow.getWorkflow(d5);

        
        
        abstractWorkflow.addMaterializedDatasets(materializedDatasets);               
        System.out.println("\nShowing of abstractWorkflow is here----------------------------------------------------------------:");
        System.out.println(abstractWorkflow.getWorkflow(d5));
        System.out.println("\nShowing of abstractWorkflow is finished------------------------------------------------------------:");
		
        materializedDatasets.add(d5);                

        Workflow workflow0 = abstractWorkflow.getWorkflow(d5);

        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        System.out.print(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");

        Workflow workflow1 = abstractWorkflow.optimizeWorkflow(d5);
        System.out.println("\nHere is optimization workflow is here-----------------------------------------------------------------------:");
        System.out.println(workflow1);
//	System.out.println(workflow1.toString());
        System.out.println("\nEnd of optimization workflow------------------------------------------------------------------------:");
        System.out.println();        
        
  
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        //Double Cost = workflow1.optimalCost;
	///////////////////////////////////////////////////////////////////////////////////////
	// String materializedWorkflow;// = wcli.materializeWorkflow(workflow1.toString(), policy);
        //materializedWorkflow = wcli.materializeWorkflow(NameOfAbstractWorkflow, policy);
	///////////////////////////////////////////////////////////////////////////////////////
        //System.out.println(materializedWorkflow);
        //System.out.println("The cost of workflow of-------------------"+workflow1.toString()+" is "+ Cost+"-------------------------");
	//System.out.println("----------------------------"+workflow1.toString());
        ////Execution 
        //String w = wcli.executeWorkflow(materializedWorkflow);
        System.out.println("\nCall for the second new workflow******************************************************************************************");
//////////////////////////////////////////////////////////////////////////////////////// 
    }
}
