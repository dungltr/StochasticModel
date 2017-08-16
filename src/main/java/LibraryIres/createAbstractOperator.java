/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import java.io.IOException;

/**
 *
 * @author letrungdung
 */
public class createAbstractOperator {
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
    public void createAbstractOperator(Move_Data Data, String SQL) throws IOException, Exception {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
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
