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
public class MyTestMove_Hive_Spark_IRES {
    String HOME=System.getenv().get("HOME");
    String username = System.getProperty("user.name");
    String SPARK_HOME=new App().readhome("SPARK_HOME");
    String HADOOP_HOME=new App().readhome("HADOOP_HOME");
    String HIVE_HOME=new App().readhome("HIVE_HOME");
    
    public void testMove_Hive_Spark_IRES(String IRES_library, String NameOfHost, int int_localhost) throws Exception {
        String directory_library = IRES_library+"/target/asapLibrary/";
        String directory_operator = IRES_library+"/target/asapLibrary/operators/";
        String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
        String AlgorithmsName = "SQL_query";
        String NameOfAbstractWorkflow = "Move_Hive_Spark_IRES_Workflow";
        String NameOfAbstractOperator = "Move_Hive_Spark";
        String NameOp = "Move_Hive_Spark_IRES";
        String AbstractOp = NameOfAbstractOperator;
        String InPutData1 = "asapServerLog";
        String InPutData2 = "customers";
        String Database = "mydb";
        String Table = "customer";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";

      
        ClientConfiguration conf = new ClientConfiguration(NameOfHost,int_localhost);		
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
                MaterializedOperators library =  new MaterializedOperators();                
                AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
                            
                AbstractOperator op = new AbstractOperator(NameOfAbstractOperator);//AopAbstractOperator);//AopAbstractOperator);
		op.add("Constraints.Engine","Spark");
                op.add("Constraints.Input.number","1");
		op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
		op.add("Constraints.Output.number", "1");
                
                cli.addAbstractOperator(op);
                op.writeToPropertiesFile(op.opName);
		
                
                Operator mop1 = new Operator(NameOp,"");
                mop1.add("Constraints.Engine", "Spark");
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
//                mop1.add("Execution.command","./Move_Hive_Spark_IRES.sh");

                cli.addOperator(mop1);   
                mop1.writeToPropertiesFile(directory_operator+mop1.opName);
                
                
                WorkflowClient wcli = new WorkflowClient();
		wcli.setConfiguration(conf);		
//		wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
		
		AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);
		
                Dataset d1 = new Dataset(InPutData1);
                d1.add("Constraints.DataInfo.Attributes.number","2");
		d1.add("Constraints.DataInfo.Attributes.Atr1.type","ByteWritable");
		d1.add("Constraints.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.key","Atr1");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.value","Atr2");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.location","127.0.0.1");
		d1.add("Optimization.size","1TB");
		d1.add("Optimization.uniqueKeys","1.3 billion"); 
                d1.inputFor(mop1, 0);
                d1.writeToPropertiesFile(d1.datasetName);

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
    public void createDocuments(String OperatorFolder,String Name, String Database, String Table, String Schema) throws UnknownHostException{
        String Database_Hive = "mydb";
        String Database_Postgres = "mydb";
        String node = new App().getComputerName();
        
        
       
    String lua = "-- Specific configuration of operator\n" +
"ENGINE = \"Spark\"\n" +
"OPERATOR = \"Move_Hive_\" .. ENGINE .. \"_IRES\"\n" +
"SCRIPT = OPERATOR .. \".sh\"\n" +
"SHELL_COMMAND = \"./\" .. SCRIPT\n" +
"-- Home directory of operator\n" +
"OPERATOR_LIBRARY = \"asapLibrary/operators\"\n" +
"OPERATOR_HOME = OPERATOR_LIBRARY .. \"/\" .. OPERATOR\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"  name = \"Execute \" .. OPERATOR .. \" Operator\",\n" +
"  labels = \"hive-spark\",\n" +
"  nodes = \""+node+"\",\n" +
"  memory = 1024,\n" +
"  container = {\n" +
"    instances = 1,\n" +
"    command = {\n" +
"		base = SHELL_COMMAND\n" +
"  	},\n" +
"    resources = {\n" +
"		[ SCRIPT] = {\n" +
"			file = OPERATOR_HOME .. \"/\" .. SCRIPT,\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		},\n" +
"		[ \"convertCSV2Parquet.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertCSV2Parquet.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		}\n" +
"    }\n" +
"  }\n" +
"}";
    String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \"Move_Hive_Spark_IRES\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"HDFS=/user/hive/warehouse\n" +
"BASE=/mnt/Data/tmp\n" +
"DATABASE="+Database+"\n" +
"TABLE="+Table+"\n" +
"SPARK_PORT=local[*]\n" +
"SCHEMA=\""+Schema+"\"\n" +
"\n" +
"\n" +
"echo \"exporting table from HIVE\"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"else\n" +
"	rm /mnt/Data/tmp/$TABLE/*\n" +
"fi\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyToLocal $HDFS/$DATABASE.db/$TABLE/* /mnt/Data/tmp/$TABLE\n" +
"if [ ! -f /mnt/Data/tmp/$TABLE/$TABLE.csv ]\n" +
"then\n" +
"	for x in $(ls /mnt/Data/tmp/$TABLE/*);\n" +
"	do\n" +
"		#echo $x\n" +
"		cat $x >> /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"	done\n" +
"fi\n" +
"chmod a+rw /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +            
"cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE.csv\n" +
"#chmod a+rw /mnt/Data/tmp/$TABLE.csv\n" +            
"#chown -R letrung /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"ls -ltr\n" +
"echo -e \"Uploading $TABLE.csv to HDFS\"\n" +
"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"#$HADOOP_HOME/bin/hdfs dfs -mkdir $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyFromLocal /mnt/Data/tmp/$TABLE/$TABLE.csv $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyFromLocal /mnt/Data/tmp/$TABLE.csv $HDFS\n" +
"\n" +
"echo -e \"Converting $TABLE.csv to parquet\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.parquet\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertCSV2Parquet.py $TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +            
"rm -r /mnt/Data/tmp/$TABLE\n" + 
"rm -r /mnt/Data/tmp/$TABLE.csv";
    
    String py= "from pyspark import SparkContext, SparkConf\n" +
"from pyspark.sql import SQLContext, HiveContext\n" +
"from pyspark.sql.types import *\n" +
"\n" +
"import os\n" +
"import sys\n" +
"import argparse\n" +
"import traceback\n" +
"import subprocess\n" +
"\n" +
"def main():\n" +
"\n" +
"	#define command line arguments\n" +
"	parser = argparse.ArgumentParser( description='Converts CSV files to Parquet and vice versa.')\n" +
"\n" +
"	parser.add_argument( 'src', metavar='sources', type=str, nargs='+',\n" +
"						 help='the directory where the files to be converted reside or a series of files'\n" +
"						 		+ ' to be converted.\\n\\n')\n" +
"\n" +
"	parser.add_argument( '--d', metavar='conversiondirection', type=str, nargs='*', choices=( 'parquet', 'csv'), default='parquet',\n" +
"						 help='define which conversion will take place i.e. from csv to parquet or vice versa. By default,'\n" +
"						 		+ ' the conversion is from csv to parquet. For the opposite conversion user should pass the' \n" +
"						 		+ ' the value \"csv\"\\n\\n')\n" +
"\n" +
"	parser.add_argument( '--sep', metavar='separator', type=str, default=' ',\n" +
"						 help='the column separator of each data file. Default -> \" \" i.e. space')\n" +
"\n" +
"	parser.add_argument( '--out', metavar='output', type=str, nargs='?', default='.',\n" +
"						 help='the output files will be saved in folder \"converted\" but the parent directory of this folder'\n" +
"						 		+ ' can be altered by user through this flag. Default -> . i.e. the output path is the current working'\n" +
"						 		+ ' directory\\n\\n')\n" +
"\n" +
"	args = parser.parse_args()\n" +
"\n" +
"	#results exportation\n" +
"	#path = args.out + \"/converted\"\n" +
"	#if not os.path.exists( path):\n" +
"	#	try: \n" +
"	#		os.makedirs( path)\n" +
"	#	except OSError:\n" +
"	#		if not os.path.isdir( path):\n" +
"	#			raise\n" +
"	#			exit( 1)\n" +
"\n" +
"	#read data files\n" +
"	#datafiles = []\n" +
"	#for file in args.src:\n" +
"		#check if it is a file or a directory\n" +
"	#	if os.path.isdir( file):\n" +
"	#		dfs = os.listdir( file)\n" +
"			#file -> directory, f -> actual file\n" +
"	#		dfs = [ file.rstrip( \"/\") + \"/\" + f for f in dfs if not os.path.isdir( f)]\n" +
"	#		datafiles = datafiles + dfs\n" +
"	#	else:\n" +
"	#		datafiles.append( file)\n" +
"\n" +
"\n" +
"	fileSchemas = { \"customer\": StructType([	StructField( \"c_custkey\", IntegerType(), True),\n" +
"    						  					StructField( \"c_name\", StringType(), True),\n" +
"											  	StructField( \"c_address\", StringType(), True),\n" +
"					  						  	StructField( \"c_nationkey\", DecimalType( 38, 0), True),\n" +
"					  						  	StructField( \"c_phone\", StringType(), True),\n" +
"					  						  	StructField( \"c_acctbal\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"c_mktsegment\", StringType(), True),\n" +
"					  						  	StructField( \"c_comment\", StringType(), True)]),\n" +
"      				\"lineitem\": StructType([	StructField( \"l_orderkey\",DecimalType( 38, 0), True),\n" +
"											  	StructField( \"l_partkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"l_suppkey\", DecimalType( 38, 0), True),\n" +
"					  						  	StructField( \"l_linenumber\", IntegerType(), True),\n" +
"					  						  	StructField( \"l_quantity\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_extendedprice\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_discount\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_tax\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_returnflag\",StringType(), True),\n" +
"					  						  	StructField( \"l_linestatus\", StringType(), True),\n" +
"					  						  	StructField( \"l_shipdate\", DateType(), True),\n" +
"					  						  	StructField( \"l_commitdate\", DateType(), True),\n" +
"					  						  	StructField( \"l_receiptdate\", DateType(), True),\n" +
"					  						  	StructField( \"l_shipinstruct\", StringType(), True),\n" +
"					  						  	StructField( \"l_shipmode\", StringType(), True),\n" +
"					  						  	StructField( \"l_comment\", StringType(), True)]),\n" +
"      				\"nation\":   StructType([ 	StructField( \"n_nationkey\", IntegerType(), True),\n" +
"											  	StructField( \"n_name\", StringType(), True),\n" +
"											  	StructField( \"n_regionkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"n_comment\", StringType(), True)]),\n" +
"					\"orders\":   StructType([	StructField( \"o_orderkey\", IntegerType(), True),\n" +
"											  	StructField( \"o_custkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"o_orderstatus\", StringType(), True),\n" +
"					  						  	StructField( \"o_totalprice\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"o_orderdate\", DateType(), True),\n" +
"					  						  	StructField( \"o_orderpriority\", StringType(), True),\n" +
"					  						  	StructField( \"o_clerk\", StringType(), True),\n" +
"					  						  	StructField( \"o_shippriority\", IntegerType(), True),\n" +
"					  						  	StructField( \"o_comment\", StringType(), True)]),\n" +
"      				\"part\":     StructType([	StructField( \"p_partkey\", IntegerType(), True),\n" +
"											  	StructField( \"p_name\", StringType(), True),\n" +
"											  	StructField( \"p_mfgr\", StringType(), True),\n" +
"					  						  	StructField( \"p_brand\", StringType(), True),\n" +
"					  						  	StructField( \"p_type\", StringType(), True),\n" +
"					  						  	StructField( \"p_size\", IntegerType(), True),\n" +
"					  						  	StructField( \"p_container\", StringType(), True),\n" +
"					  						  	StructField( \"p_retailprice\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"p_comment\", StringType(), True)]),\n" +
"      				\"partsupp\": StructType([	StructField( \"ps_partkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"ps_suppkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"ps_availqty\", IntegerType(), True),\n" +
"					  						  	StructField( \"ps_supplycost\", StringType(), True),\n" +
"					  						  	StructField( \"ps_comment\", StringType(), True)]),\n" +
"      				\"region\":   StructType([	StructField( \"r_regionkey\", IntegerType(), True),\n" +
"											  	StructField( \"r_name\", StringType(), True),\n" +
"											  	StructField( \"r_comment\", StringType(), True)]),\n" +
"					\"supplier\":	StructType([	StructField( \"s_suppkey\", IntegerType(), True),\n" +
"                                              						  	StructField( \"s_name\", StringType(), True),\n" +
"                                                                                                StructField( \"s_address\", StringType(), True),\n" +
"					  						  	StructField( \"s_nationkey\", DecimalType( 38, 0), True),\n" +
"					  						  	StructField( \"s_phone\", StringType(), True),\n" +
"					  						  	StructField( \"s_acctbal\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"s_comment\", StringType(), True)]),\n" +
"      				\"part_agg\":   StructType([	StructField( \"agg_partkey\", DecimalType( 38, 0), True),\n" +
"                                                                StructField( \"avg_quantity\", DecimalType( 10, 2), True),\n" +
"                                                                StructField( \"agg_extendedprice\", DecimalType( 10, 2), True)])\n" +
"    			  }		  \n" +
"    			  \n" +
"	conf = SparkConf().setAppName( \"Convert CSV to Parquet\")\n" +
"	sc = SparkContext(conf=conf)\n" +
"	sqlContext = SQLContext(sc)\n" +
"        namenode = \"hdfs://localhost:9000\"\n" +
"        warehouse = \"/user/hive/warehouse\"\n" +
"        print( args.src)\n" +
"        forschema = args.src[ 0].split( \"/\")[-1].split( \".\")[0]\n" +
"        print( forschema)\n" +
"        output = args.src[ 0]\n" +
"        inputdir = warehouse \n" +
"        print(\"-------------------------------------------------\")\n" +
"        fnames = subprocess.check_output( \"hdfs dfs -ls \" + inputdir, shell = True)\n" +
"        fnames = [ x for x in fnames.split() if x.startswith( warehouse)]\n" +
"        for file in fnames:\n" +
"                print( \"FILE IS:\", file)\n" +
"                fileSchema = fileSchemas[ forschema]\n" +
"                print( \"FILESCHEMA IS \", fileSchema)\n" +
"#               df = sqlContext.read.format( \"com.databricks.spark.csv\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\").load( file, schema=fileSchema)\n" +
"                df = sqlContext.read.load(file, format =\"com.databricks.spark.csv\")\n" +
"                #, schema=fileSchema)\n" +
"                df.printSchema()\n" +
"                try:\n" +
"                	df.write.save(namenode + inputdir + \"/\" + output + \".parquet\", format = \"parquet\")\n" +
"#                    df.write.format(\".parquet\").save( namenode + warehouse + \"/\" + output, mode = \"append\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\")\n" +
"                except Exception:\n" +
"                    exc_type, exc_value, exc_traceback = sys.exc_info()\n" +
"                    formatted_lines = traceback.format_exc().splitlines()\n" +
"                    print(formatted_lines[0])\n" +
"                    print(formatted_lines[-1])\n" +
"                    print(\"*** format_exception:\")\n" +
"                    print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))\n" +
"                    pass\n" +
"\n" +
"if __name__ == \"__main__\":\n" +
"	main()";
        createlua(OperatorFolder + "/" + Name, Name + ".lua", lua); 
        createlua(OperatorFolder + "/" + Name, Name + ".sh", sh);
        createlua(OperatorFolder + "/" + Name, "convertCSV2Parquet.py", py);
    }
}
