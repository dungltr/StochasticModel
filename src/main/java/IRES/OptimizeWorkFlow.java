/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

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
    
    public void OptimizeWorkFlow(Move_Data Data, String policy) throws Exception {
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

        abstractWorkflow.addMaterializedDatasets(materializedDatasets);               
/*        System.out.println("\nShowing of abstractWorkflow is here----------------------------------------------------------------:");
        System.out.println(abstractWorkflow.getWorkflow(d1));
        System.out.println("\nShowing of abstractWorkflow is finished------------------------------------------------------------:");
		
        materializedDatasets.add(d2);                
*/
        Workflow workflow0 = abstractWorkflow.getWorkflow(d2);

        System.out.println("\nShowing of original workflow is here----------------------------------------------------------------:");
        System.out.print(workflow0);
        System.out.println("\nShowing of original workflow is ended--------------------------------------------------------------:");

        Workflow workflow1 = abstractWorkflow.optimizeWorkflow(d2);
        System.out.println("\nHere is optimization workflow is here-----------------------------------------------------------------------:");
        System.out.println(workflow1);
        System.out.println("\nEnd of optimization workflow------------------------------------------------------------------------:");
        System.out.println();        
        
/*  
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
        String materializedWorkflow = wcli.materializeWorkflow(workflow1, policy);
        System.out.println(materializedWorkflow);
        System.out.println("----------------------------"+workflow1.toString());
        ////Execution 
        String w = wcli.executeWorkflow(materializedWorkflow);
*/////////////////////////////////////////////////////////////////////////////////////// 
    }
}
