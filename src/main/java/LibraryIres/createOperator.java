/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import static IRES.runWorkFlowIRES.datasetout;
import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;

/**
 *
 * @author letrungdung
 */
public class createOperator {
    int int_localhost = 1323;
    String name_host = "localhost";
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String IRES_HOME = new App().readhome("IRES_HOME");
    String ASAP_HOME = IRES_HOME;
    String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    String directory_library = IRES_library+"/target/asapLibrary/";
    String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    String directory_datasets = IRES_library+"/target/asapLibrary/datasets/";
    String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    public void createOperator(Move_Data Data, String SQL) throws Exception {
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Operator mop1 = new Operator(NameOp,"");
        mop1.add("Constraints.Engine",Data.get_To());
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Input.number","1");
        mop1.add("Constraints.OpSpecification.Algorithm.name", "SQL_query");
        mop1.add("Optimization.model.execTime", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.outputSpace.execTime", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");
        mop1.add("Optimization.cost", "1.0");
        mop1.add("Optimization.execTime", "1.0");
 
        mop1.add("Execution.Arguments.number", "2");
        mop1.add("Execution.Argument0", "In0.path.local");
        mop1.add("Execution.Argument1","lines.out");
        mop1.add("Execution.Output0.path", "$HDFS_OP_DIR/lines.out");
        mop1.add("Execution.copyFromLocal","lines.out");
        mop1.add("Execution.copyToLocal", "In0.path");
        mop1.add("Execution.LuaScript",NameOp+".lua");
        mop1.add("Execution.command",NameOp+".sh");
        cli.addOperator(mop1); 
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
    }

    /**
     *
     * @param Data
     * @param SQL
     * @throws java.lang.Exception
     */
    public void createOperatorSQL(Move_Data Data, String SQL) throws Exception {
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        Operator mop1 = new Operator(NameOp,"");
/*        mop1.add("Constraints.Engine",Data.get_To());
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Input.number","1");
        mop1.add("Constraints.OpSpecification.Algorithm.name", "SQL_query");
        mop1.add("Optimization.model.execTime", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.outputSpace.execTime", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");
        mop1.add("Optimization.cost", "1.0");
        mop1.add("Optimization.execTime", "1.0");
 
        mop1.add("Execution.Arguments.number", "2");
        mop1.add("Execution.Argument0", "In0.path.local");
        mop1.add("Execution.Argument1","lines.out");
        mop1.add("Execution.Output0.path", "$HDFS_OP_DIR/lines.out");
        mop1.add("Execution.copyFromLocal","lines.out");
        mop1.add("Execution.copyToLocal", "In0.path");
        mop1.add("Execution.LuaScript",NameOp+".lua");
        mop1.add("Execution.command",NameOp+".sh");
*/        
        mop1.add("Constraints.Engine",Data.get_To());
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Input.number","1");
        mop1.add("Constraints.OpSpecification.Algorithm.name", "SQL_query");
        mop1.add("Optimization.model.execTime", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.outputSpace.execTime", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");
        mop1.add("Optimization.cost", "1.0");
        mop1.add("Optimization.execTime", "1.0");
 
        mop1.add("Execution.Arguments.number", "2");
        mop1.add("Execution.Argument0", "In0.path.local");
        mop1.add("Execution.Argument1","lines.out");
        mop1.add("Execution.Output0.path", "$HDFS_OP_DIR/lines.out");
        mop1.add("Execution.copyFromLocal","lines.out");
        mop1.add("Execution.copyToLocal", "In0.path");
//        mop1.add("Execution.LuaScript",NameOp+".lua");
//        mop1.add("Execution.command",NameOp+".sh");
        
        cli.addOperator(mop1); 
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
    }

    /**
     *
     * @param Data
     * @param SQL
     * @throws Exception
     */
    public void createAbstractOpSQL(Move_Data Data, String SQL) throws Exception {
        String InPutData = "asapServerLog";//Data.get_DataIn();
        String OutPutData = datasetout(Data)+"_OUT";
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
        String NameOfAbstractWorkflow = NameOp+"_Workflow";
        String AlgorithmsName = "SQL_query";
       
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
        AbstractOperator op = new AbstractOperator(AbstractOp);//AopAbstractOperator);//AopAbstractOperator);
        op.add("Constraints.Engine", Data.get_To());
        op.add("Constraints.Input.number","1");
	op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
	op.add("Constraints.Output.number", "1");
        op.writeToPropertiesFile(directory_library + "abstractOperators/" + op.opName);               
        
        cli.addAbstractOperator(op);
        op.writeToPropertiesFile(op.opName);
    }
}
