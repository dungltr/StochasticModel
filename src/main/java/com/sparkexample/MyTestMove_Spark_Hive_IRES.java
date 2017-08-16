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
public class MyTestMove_Spark_Hive_IRES {
    String HOME=System.getenv().get("HOME");
    String SPARK_HOME=new App().readhome("SPARK_HOME");
    String HADOOP_HOME=new App().readhome("HADOOP_HOME");
    String HIVE_HOME=new App().readhome("HIVE_HOME");
    
    public void testMove_Spark_Hive_IRES(String IRES_library, String NameOfHost, int int_localhost) throws Exception {
        String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
        String AlgorithmsName = "SQL_query";
        String NameOfAbstractWorkflow = "Move_Spark_Hive_IRES_Workflow";
        String NameOfAbstractOperator = "Spark_Hive_IRES";
        String NameOp = "Move_Spark_Hive_IRES";
        String AbstractOp = NameOfAbstractOperator;
        String InPutData1 = "asapServerLog";
        String InPutData2 = "customers";

      
        ClientConfiguration conf = new ClientConfiguration(NameOfHost,int_localhost);		
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
                MaterializedOperators library =  new MaterializedOperators();                
                AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
                            
                AbstractOperator op = new AbstractOperator(NameOfAbstractOperator);//AopAbstractOperator);//AopAbstractOperator);
		op.add("Constraints.Engine","PostgreSQL");
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
                mop1.add("Constraints.Engine", "PostgreSQL");
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
//                mop1.add("Execution.command","./PostgreSQL_IRES.sh");

                cli.addOperator(mop1);               
                // To show in Materialized Workflow
                abstractWorkflow.addInputEdge(d1,abstractOp,0);
		abstractWorkflow.addOutputEdge(abstractOp,d2,0);
                abstractWorkflow.getWorkflow(d2);
                
                 
                createDocuments(OperatorFolder, mop1.opName);      
		
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
                        System.out.println("Add materializedWorkflow successful"+NameOfAbstractWorkflow);
//			double estimatedTime = Double.parseDouble(wcli.getMaterializedWorkflowDescription(materializedWorkflow).getOperator("PageRank_Spark").getExecTime());
//                        double estimatedTime;			
//                      I Would like to run this line but it often have an error
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
*/
//                      writer.flush();
			if(count>=1)// old value is 1000
				break;
//			Thread.sleep(100000);
		}
		writer.close();
/*                
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
*/    } 
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
    public void createDocuments(String OperatorFolder,String Name) throws UnknownHostException{
        String Database_Hive = "mydb";
        String Database_Postgres = "mydb";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table = "customer";
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String node = new App().getComputerName();
    String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \"Move_Spark_Hive\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +
"HDFS=/user/hive/warehouse\n" +
"TABLE=customer\n" +
"SCHEMA=\"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))\"\n" +
"\n" +
"SPARK_PORT=local[*]\n" +
"SQL_QUERY=\"DROP TABLE IF EXISTS $TABLE; CREATE TABLE $TABLE $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY ','; LOAD DATA INPATH '/user/hive/warehouse/$TABLE.csv' OVERWRITE INTO TABLE $TABLE\"\n" +
"\n" +
"echo \"Exporting table $TABLE from Spark\"\n" +
"echo \"Converting Parquet $TABLE.parquet to CSV\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertParquet2CSV.py $HADOOP_HOME $TABLE\n" +
"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"fi\n" +
"$HADOOP_HOME/bin/hdfs dfs -getmerge $HDFS/$TABLE.csv /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#ls -lah /mnt/Data/tmp/$TABLE/\n" +
"#from parquet file double quotes have been added to string values\n" +
"#and for this their length is changed. To restore their length the\n" +
"#extra double quotes are removed with sed\n" +
"sed -i 's/|\\\"/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"sed -i 's/\\\"|/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#ls -ltr /mnt/Data/tmp/$TABLE\n" +
"cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE.csv\n"  +            
"#to avoid local disk overflow while loading the file to the\n" +
"#corresponding Hive table, the file is loaded through master\n" +
"#cluster node\n" +
"echo \"Loading $TABLE.csv to Hive\"\n" +
"#scp /mnt/Data/tmp/$TABLE/$TABLE.csv master:/mnt/Data/\n" +
"#ssh master \"ls -lah /mnt/Data/\"\n" +
"#ssh master \"$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/$TABLE.csv $HDFS/$TABLE.csv\"\n" +
"#rm /mnt/Data/tmp/$TABLE\n" +
"cd $HIVE_HOME\n" +
"#$HIVE_HOME/bin/hive -e \"DROP TABLE IF EXISTS $TABLE; CREATE TABLE $TABLE $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY ','; LOAD DATA LOCAL INPATH '/mnt/Data/tmp/$TABLE/$TABLE.csv' OVERWRITE INTO TABLE $TABLE\"\n" +
"$HIVE_HOME/bin/hive -e \"$SQL_QUERY\"\n" +
"#cd -\n" +
"\n" +
"#clean\n" +
"rm -r /mnt/Data/tmp/$TABLE\n" +
"rm -r /mnt/Data/tmp/$TABLE.csv\n" +            
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv";
    String py = "from pyspark import SparkContext, SparkConf\n" +
"from pyspark.sql import SQLContext, HiveContext\n" +
"from pyspark.sql.types import *\n" +
"\n" +
"import os\n" +
"import sys\n" +
"import subprocess\n" +
"\n" +
"def main():\n" +
"	conf = SparkConf().setAppName( \"Convert Parquet2CSV\")\n" +
"	sc = SparkContext( conf=conf)\n" +
"	sqlContext = SQLContext(sc)\n" +
"\n" +
"	yarn_home = sys.argv[ 1]\n" +
"        file = sys.argv[ 2]\n" +
"	print( yarn_home, file)\n" +
"        namenode = \"hdfs://localhost:9000\"\n" +
"        warehouse = \"/user/hive/warehouse\"\n" +
"        output = warehouse + \"/\" + file + \".csv\"\n" +
"        df = sqlContext.read.parquet( namenode + warehouse + \"/\" + file  + \".parquet\")\n" +
"        df.printSchema()\n" +
"		\n" +
"\n" +
"#        if os.path.exists( namenode + output):\n" +
"#            try:\n" +
"#                os.system( \"/opt/hadoop-2.7.0/bin/hdfs dfs -rm -r \" + output)\n" +
"#            except OSError:\n" +
"#                raise\n" +
"\n" +
"#        df.write.format( \"com.databricks.spark.csv\").options( delimiter='|').save( namenode  + output)\n" +
"        df.write.save(namenode + output, format=\"com.databricks.spark.csv\")\n" +
"        #merge output file to one\n" +
"        #fnames = subprocess.check_output( \"/opt/hadoop-2.7.0/bin/hdfs dfs -ls \" + output, shell = True)\n" +
"        #fnames = [ x for x in fnames.split() if x.startswith( warehouse) and not x.endswith( \"_SUCCESS\")]\n" +
"        #for file_part in fnames:\n" +
"         #   print( \"FILE IS:\", file_part)\n" +
"            #forschema = file.split( \"/\")[-1].split( \".\")[0]\n" +
"            #fileSchema = fileSchemas[ forschema]\n" +
"            #print( \"FILESCHEMA IS \", fileSchema)\n" +
"          #  df = sqlContext.read.format( \"com.databricks.spark.csv\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\").load( file_part)\n" +
"           # df.printSchema()\n" +
"            #df.write.format( \"com.databricks.spark.csv\").save( output + \"/\" + file + \".csv\", mode = \"append\")\n" +
"\n" +
"if __name__ == \"__main__\":\n" +
"    main()";
    String lua = "-- Specific configuration of operator\n" +
"ENGINE = \"Hive\"\n" +
"OPERATOR = \"Move_Spark_\" .. ENGINE .. \"_IRES\"\n" +
"SCRIPT = OPERATOR .. \".sh\"\n" +
"SHELL_COMMAND = \"./\" .. SCRIPT\n" +
"-- Home directory of operator\n" +
"OPERATOR_LIBRARY = \"asapLibrary/operators\"\n" +
"OPERATOR_HOME = OPERATOR_LIBRARY .. \"/\" .. OPERATOR\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"  name = \"Execute \" .. OPERATOR .. \" Operator\",\n" +
"  labels = \"hive\",\n" +
"  nodes = \""+node+"\",\n" +
"  memory = 1024,\n" +
"  container = {\n" +
"    instances = 1,\n" +
"    command = {\n" +
"		base = SHELL_COMMAND\n" +
"  	},\n" +
"  	resources = {\n" +
"		[ \"Move_Spark_Hive_IRES.sh\"] = {\n" +
"			file = OPERATOR_HOME .. \"/\" .. SCRIPT,\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\"  -- other values: 'private', 'public'\n" +
"		},\n" +
"		[ \"convertParquet2CSV.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertParquet2CSV.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\"  -- other values: 'private', 'public'\n" +
"		}\n" +
"  	}\n" +
"  }\n" +
"}";
    
        createlua(OperatorFolder + "/" + Name, Name + ".lua", lua); 
        createlua(OperatorFolder + "/" + Name, Name + ".sh", sh);
        createlua(OperatorFolder + "/" + Name, "convertParquet2CSV.py", py);
    }
}
