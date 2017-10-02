/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sparkexample;

import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.Workflow;
import gr.ntua.cslab.asap.workflow.WorkflowNode;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author letrung
 */
public class MyTestMove_Postgres_Hive_IRES {

    String username = System.getProperty("user.name");
    String SPARK_HOME=new App().readhome("SPARK_HOME");
    String HADOOP_HOME=new App().readhome("HADOOP_HOME");
    String HIVE_HOME=new App().readhome("HIVE_HOME");
    
    public void testMove_Postgres_Hive_IRES(String IRES_library, String NameOfHost, int int_localhost) throws Exception {
        String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
        String AlgorithmsName = "SQL_query";
        String NameOfAbstractWorkflow = "Move_Postgres_Hive_IRES_Workflow";
        String NameOfAbstractOperator = "Abstract_Postgres_Hive_IRES";
        String NameOp = "Move_Postgres_Hive_IRES";
        String AbstractOp = NameOfAbstractOperator;
//        String InPutData1 = "asapServerLog";
        String InPutData1= "database_part_hive";
//        String InPutData2 = "customers";
        String Database = "mydb";
        String Table = "company";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";

      
        ClientConfiguration conf = new ClientConfiguration(NameOfHost,int_localhost);		
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
                MaterializedOperators library =  new MaterializedOperators();                
                AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
                            
                AbstractOperator op = new AbstractOperator(NameOfAbstractOperator);//AopAbstractOperator);//AopAbstractOperator);
		op.add("Constraints.Engine","Hive");
                op.add("Constraints.Input.number","1");
		op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
		op.add("Constraints.Output.number", "1");
                
                cli.addAbstractOperator(op);
		WorkflowClient wcli = new WorkflowClient();
		wcli.setConfiguration(conf);		
//		wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
		
		AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);
		
                Dataset d1 = new Dataset(InPutData1);
		WorkflowNode t1 = new WorkflowNode(false,false,InPutData1);
		t1.setDataset(d1);
                
                AbstractOperator abstractOp = new AbstractOperator(AbstractOp);//AopAbstractOperator);
                WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp);//AopAbstractOperator);
		op1.setAbstractOperator(abstractOp);
		
		Dataset d2 = new Dataset("d2");
		WorkflowNode t2 = new WorkflowNode(false,true,"d2");
		t2.setDataset(d2);

                t1.addOutput(0,op1);
                
		op1.addInput(0,t1);
		op1.addOutput(0,t2);
		
		t2.addInput(0,op1);
		
		abstractWorkflow1.addTarget(t2);

		wcli.addAbstractWorkflow(abstractWorkflow1);
                
                Operator mop1 = new Operator(NameOp,"");
                mop1.add("Constraints.Engine", "Hive");
                mop1.add("Constraints.Output.number","1");
                mop1.add("Constraints.Input.number","1");
                mop1.add("Constraints.OpSpecification.Algorithm.name", AlgorithmsName);
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
                mop1.add("Execution.LuaScript", NameOp+".lua");
//                mop1.add("Execution.command","./Move_Postgres_Hive_IRES.sh");

                cli.addOperator(mop1);               
                // To show in Materialized Workflow
                abstractWorkflow.addInputEdge(d1,abstractOp,0);
		abstractWorkflow.addOutputEdge(abstractOp,d2,0);
                abstractWorkflow.getWorkflow(d2);
                
                 
                createDocuments(OperatorFolder, mop1.opName, Database, Table, Schema);      
		
		String policy ="metrics,cost,execTime\n"+
						"groupInputs,execTime,max\n"+
						"groupInputs,cost,sum\n"+
						"function,execTime,min";
		

		String[] nodes = {"1000", "10000", "100000", "1000000"};//, "10000000"};
		String[] avgDeg = {"10", "50", "100"};
		String[] iterations = {"10", "20", "30", "40", "50", "60", "70" ,"80", "90", "100"};
		String[] memory = {"512", "1024", "2048", "3072","4096", "5120", "6144"};
		
		int count=0;
		Random r = new Random();
		double abs_error=0, rel_error=0,abs_error_sum=0, rel_error_sum=0;
		PrintWriter writer = new PrintWriter("resultsNew.txt", "UTF-8");

               while(true){
			String node = nodes[r.nextInt(nodes.length)];
			String aD = avgDeg[r.nextInt(avgDeg.length)];
			String mem = memory[r.nextInt(memory.length)];
			String it = iterations[r.nextInt(iterations.length)];
			
			if(Integer.parseInt(node)*Integer.parseInt(aD)>10000000)
				continue;
			writer.println("V: "+node+" E: "+aD+" mem: "+mem+" it:"+it);
			writer.flush();

                        String materializedWorkflow = wcli.materializeWorkflow(NameOfAbstractWorkflow, policy);
//                                materializeWorkflowWithoutParameters(NameOfAbstractWorkflow, policy);
			System.out.println(materializedWorkflow);
                        System.out.println("Add materializedWorkflow successful:"+NameOfAbstractWorkflow);
//			double estimatedTime = Double.parseDouble(wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator("Move_Postgres_Hive_IRES").getExecTime());
//                           double estimatedTime;
//            estimatedTime = Double.parseDouble(wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator(NameOp).getExecTime());
                    //wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator(NameOp).getExecTime();
//                        double estimatedTime;			
//                      I Would like to run this line but it often have an error
                        System.out.println("It is OK---------------------------------------------------------------------");
                        System.out.println(wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator(NameOp));
                        String w = wcli.executeWorkflow(materializedWorkflow);                                            
                        long start = System.currentTimeMillis();
			wcli.waitForCompletion(w);
			long stop = System.currentTimeMillis();
			double actualTime = (double)(stop-start)/1000.0;// -12.0;
                        writer.println(" Actual Time: " +actualTime);
                        System.out.println("Actual Time: "+actualTime+"---------------------------------------------------------------------------------");
/*                        estimatedTime = actualTime;
                        writer.println("Estimated: "+estimatedTime);
			writer.println("Estimated: "+estimatedTime+" Actual: " +actualTime);
			writer.flush();
			
			abs_error=Math.abs(estimatedTime-actualTime);
			rel_error=Math.abs(estimatedTime-actualTime)/Math.abs(actualTime);
*/
                        count++;
/*			abs_error_sum+=abs_error;
			rel_error_sum+=rel_error;
			writer.println("Step: "+count+" abs_error: "+abs_error+" rel_error: "+rel_error);
			writer.println("Step: "+count+" abs_error_total: "+abs_error_sum/(double)count+" rel_error_total: "+rel_error_sum/(double)count);

                      writer.flush();
*/			if(count>=1)// old value is 1000
				break;
//			Thread.sleep(100000);
		}
		writer.close();
                
                List<gr.ntua.cslab.asap.operators.Dataset> materializedDatasets = new ArrayList<gr.ntua.cslab.asap.operators.Dataset>();
		materializedDatasets.add(d1);
//		materializedDatasets.add(d2);
		abstractWorkflow.addMaterializedDatasets(materializedDatasets);               
		System.out.println("Showing of abstractWorkflow is here----------------------------------------------------------------:");
                System.out.println(abstractWorkflow);
                System.out.println("Showing of abstractWorkflow is finished------------------------------------------------------------:");
                
                Workflow workflow = abstractWorkflow.getWorkflow(d2);
//		workflow.writeToDir(IRES_library+"/target/asapLibrary/" + "workflows/MyTestKmeans");
		System.out.println("Showing of original workflow is here----------------------------------------------------------------:");
		System.out.print(workflow);
                System.out.println("Showing of original workflow is ended--------------------------------------------------------------:");
                
                Workflow workflow1 = abstractWorkflow.optimizeWorkflow(d2);
                System.out.println("Here is optimization workflow is here-----------------------------------------------------------------------:");
		System.out.println(workflow1);
                System.out.println("End of optimization workflow------------------------------------------------------------------------:");
//		workflow1.writeToDir(IRES_library+"/target/asapLibrary/" + "workflows/MyTestKmeans");
		System.out.println();
    } 
    public void createlua(String OperatorFolder,String filename, String content){
        
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
    public void createDocuments(String OperatorFolder,String Name, String Database, String Table, String Schema) throws UnknownHostException{
        String Database_Hive = "mydb";
        String Database_Postgres = "mydb";
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);               
        String node = new App().getComputerName();
        
        
        
        String lua = "-- Specific configuration of operator\n" +
"ENGINE = \"Hive\"\n" +
"OPERATOR = \"Move_Postgres_\" .. ENGINE .. \"_IRES\"\n" +
"SCRIPT = OPERATOR .. \".sh\"\n" +
"SHELL_COMMAND = \"./\" .. SCRIPT\n" +
"-- Home directory of operator\n" +
"OPERATOR_LIBRARY = \"asapLibrary/operators\"\n" +
"OPERATOR_HOME = OPERATOR_LIBRARY .. \"/\" .. OPERATOR\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"	name = \"Execute \" .. OPERATOR .. \" Operator\",\n" +
"	labels = \"postgres-hive\",\n" +
"	nodes = \""+node+"\",\n" +
"       memory = 1024,\n" +                
"	container = { \n" +
"		instances = 1,\n" +
"		command = { \n" +
"			base = SHELL_COMMAND\n" +
"		},\n" +
"		resources = { \n" +
"			[ SCRIPT] = { \n" +
"				file = OPERATOR_HOME .. \"/\" .. SCRIPT,\n" +
"				type = \"file\",               -- other value: 'archive'\n" +
"				visibility = \"application\",  -- other values: 'private', 'public'\n" +
"			}   \n" +
"		}\n" +
"       }\n" +
"\n" +
"}";
    String sh = "#!/bin/bash\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +
"\n" +
"HDFS=/user/hive/warehouse\n" +
"DATABASE=mydb\n" +
"TABLE=customer\n" +
"SCHEMA=\"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))\"\n" +
"\n" +
"echo -e \"$DATABASE\"\n" +
"echo -e \"$TABLE\"\n" +
"echo -e \"$SCHEMA\"\n" +
"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"fi\n" +
"echo \"exporting table from postgres\"\n" +
"#sudo -u letrung psql -d $DATABASE -c \"COPY (SELECT * FROM TABLE) TO '//mnt/Data/tmp/tmp.csv' WITH CSV (DELIMITER '|')\"\n" +
"#sudo -u letrung psql -d $DATABASE -c \"COPY (SELECT * FROM $TABLE) TO '/mnt/Data/tmp/$TABLE/$TABLE.csv' WITH  (DELIMITER '|', FORMAT 'csv')\"\n" +
"psql -U "+username+" -d $DATABASE -c \"COPY (SELECT * FROM $TABLE) TO '/mnt/Data/tmp/tmp.csv' WITH (DELIMITER '|', FORMAT csv)\"\n" +
"#chown -f /mnt/Data/tmp/tmp.csv\n" +
"cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE\n" +
"#sudo cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/\n" +            
"mv /mnt/Data/tmp/$TABLE/tmp.csv /mnt/Data/tmp/$TABLE/$TABLE.csv\n" + 
"mv /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE.csv\n" +              
"#chown -f /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#chown -f /mnt/Data/tmp/$TABLE.csv\n" +
"#sudo scp /mnt/Data/tmp/$TABLE/$TABLE.csv master:/mnt/Data/\n" +
"#ssh master \"ls -lah /mnt/Data/\"\n" +
"#ssh master \"$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/$TABLE.csv $HDFS/$TABLE.csv\"\n" +
"#chown -R /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE\n" +
"#cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/\n" +
"#ls -lah /mnt/Data/\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"#$HADOOP_HOME/bin/hdfs dfs -mkdir $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE/$TABLE.csv $HDFS\n" +
"#$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE.csv $HDFS\n" +
            
"#$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE/$TABLE.csv $HDFS/$TABLE.csv\n" +
"#$HADOOP_HOME/bin/hdfs dfs -chmod -R a+wrx $HDFS/$TABLE\n" +
"echo \"loading table to hive\"\n" +
"cd $HIVE_HOME\n" +
"#$HIVE_HOME/bin/hive -e \"DROP TABLE IF EXISTS $TABLE; CREATE TABLE IF NOT EXISTS $TABLE $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; LOAD DATA LOCAL INPATH '/mnt/Data/tmp/$TABLE.csv' OVERWRITE INTO TABLE $TABLE\"\n" +
"$HIVE_HOME/bin/hive -e \"DROP TABLE IF EXISTS $TABLE; CREATE TABLE IF NOT EXISTS $TABLE $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; LOAD DATA INPATH '$HDFS/$TABLE.csv' OVERWRITE INTO TABLE $TABLE;\"\n" +
"\n" +
"rm -r /mnt/Data/tmp/$TABLE\n" + 
"rm -r /mnt/Data/tmp/$TABLE.csv";
    
        createlua(OperatorFolder + "/" + Name, Name + ".lua", lua); 
        createlua(OperatorFolder + "/" + Name, Name + ".sh", sh);
//        createlua(OperatorFolder + "/" + Name, "convertCSV2Parquet.py", py);
    }
}
