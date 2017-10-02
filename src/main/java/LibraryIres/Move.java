/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import com.sparkexample.App;
import com.sparkexample.TestPostgreSQLDatabase;
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
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Random;

/**
 *
 * @author letrung
 */
public class Move {
    int int_localhost = 1323;
    String name_host = "localhost";
    String SPARK_HOME = new App().readhome("SPARK_HOME");
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String IRES_HOME = new App().readhome("IRES_HOME");
    String ASAP_HOME = IRES_HOME;
    String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
//    String directory_library = IRES_library+"/target/asapLibrary/";
//    String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    public void Move (Move_Data Data) throws Exception {
        String directory_library = IRES_library+"/target/asapLibrary/";
        String directory_operator = IRES_library+"/target/asapLibrary/operators/";
        String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
        
        String InPutData = Data.get_DataIn();
        String OutPutData = Data.get_DataOut();
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
        String NameOfAbstractWorkflow = NameOp+"_Workflow";
        String AlgorithmsName = "SQL_query";
        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);		
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
                MaterializedOperators library =  new MaterializedOperators();                
                AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
                            
                AbstractOperator op = new AbstractOperator(AbstractOp);//AopAbstractOperator);//AopAbstractOperator);
		op.add("Constraints.Engine","PostgreSQL");
                op.add("Constraints.Input.number","1");
		op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
		op.add("Constraints.Output.number", "1");
                op.writeToPropertiesFile(directory_library + "abstractOperators/" + op.opName);               
                cli.addAbstractOperator(op);
                op.writeToPropertiesFile(op.opName);
		WorkflowClient wcli = new WorkflowClient();
		wcli.setConfiguration(conf);		

                
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
                mop1.add("Execution.LuaScript",NameOp+".lua");
                mop1.add("Execution.command",NameOp+".sh");

                cli.addOperator(mop1); 
                mop1.writeToPropertiesFile(directory_operator+mop1.opName);               
//                op.writeToPropertiesFile(NameOp);            
//                gr.ntua.cslab.asap.operators.Dataset d1 = new gr.ntua.cslab.asap.operators.Dataset(InPutData1);
/*		d1.add("Constraints.DataInfo.Attributes.number","2");
		d1.add("Constraints.DataInfo.Attributes.Atr1.type","ByteWritable");
		d1.add("Constraints.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.key","Atr1");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.value","Atr2");
		d1.add("Constraints.Engine.DB.NoSQL.HBase.location","127.0.0.1");
		d1.add("Optimization.size","1TB");
		d1.add("Optimization.uniqueKeys","1.3 billion"); 
                d1.inputFor(mop1, 0);
*/
//		d1.writeToPropertiesFile(d1.datasetName);
//                d1.writeToPropertiesFile(directory_library + "datasets/" + d1.datasetName);                
                AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);		
                Dataset d1 = new Dataset(InPutData);
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
		WorkflowNode t1 = new WorkflowNode(false,false,InPutData);
		t1.setDataset(d1);
                
                AbstractOperator abstractOp = new AbstractOperator(AbstractOp);//AopAbstractOperator);
                WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp);//AopAbstractOperator);
		op1.setAbstractOperator(abstractOp);
		
		Dataset d2 = new Dataset("d3");
		WorkflowNode t2 = new WorkflowNode(false,true,"d3");
		t2.setDataset(d2);

                t1.addOutput(0,op1);
                
		op1.addInput(0,t1);
		op1.addOutput(0,t2);
		
		t2.addInput(0,op1);
		
		abstractWorkflow1.addTarget(t2);                
// To show in Materialized Workflow
                abstractWorkflow.addInputEdge(d1,abstractOp,0);
		abstractWorkflow.addOutputEdge(abstractOp,d2,0);
                abstractWorkflow.getWorkflow(d2);
                //		wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
		
		wcli.addAbstractWorkflow(abstractWorkflow1);
                 
                String node_pc = new App().getComputerName();
        String convertCSV2Parquet = "		[ \"convertCSV2Parquet.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertCSV2Parquet.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		}\n";
        String convertParquet2CSV ="		[ \"convertParquet2CSV.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertParquet2CSV.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\"  -- other values: 'private', 'public'\n" +
"		}\n";
        String convert = "";
        switch (Data.get_From()) {
            case "HIVE":  case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres"))
                    convert = "";
                else if ((Data.get_To() == "SPARK") || (Data.get_To() == "spark"))
                    convert = convertCSV2Parquet;
                }
                break;
            case "POSTGRES": case "postgres": 
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    convert = "";
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    convert = convertParquet2CSV;
                }
                break;
            case "SPARK":  case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    convert = "";
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "spark"))
                    convert = convertParquet2CSV;
                }
                break;            
            default:
                
                break;
        }
        String lua = "-- Specific configuration of operator\n" +
"ENGINE = \""+Data.get_To()+"\"\n" +
//"OPERATOR = \"Move_HIVE_POSTGRES\"\n" + 
"OPERATOR = \"Move_"+Data.get_From()+"_\" .. ENGINE\n" +
"SCRIPT = OPERATOR .. \".sh\"\n" +
"SHELL_COMMAND = \"./\" .. SCRIPT\n" +
"-- Home directory of operator\n" +
"OPERATOR_LIBRARY = \"asapLibrary/operators\"\n" +
"OPERATOR_HOME = OPERATOR_LIBRARY .. \"/\" .. OPERATOR\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"	name = \"Execute \" .. OPERATOR .. \" Operator\",\n" +
"	labels = \""+Data.get_From()+"-"+Data.get_To()+"\",\n" +
"	nodes = \""+node_pc+"\",\n" +
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
"			}   \n" + convert +
"		}\n" + 
"       }\n" +
"\n" +
"}";
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
        switch (Data.get_From()) {
            case "HIVE":  case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres"))
                    create_Data_Hive_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Hive_Spark(Data);
                }
                break;
            case "POSTGRES": case "postgres": 
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Postgres_Spark(Data);
                }
                break;
            case "SPARK": case "spark": 
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:
                
                break;
        }     
		
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
    }
    public void create_Data_Hive_Postgres(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);
                
        
    String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"HDFS=/user/hive/warehouse\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +            
"TABLE_OUT="+Table_Out+"\n" +            
"SCHEMA=\""+Schema+"\"\n" +
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
"cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE.csv\n" +  
"mv /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE_OUT.csv\n" +             
"#sudo chown -R postgres:postgres /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#sudo chown -R postgres:postgres /mnt/Data/tmp/$TABLE.csv\n" +   
"#sudo chown -R "+username+" /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"chown -R "+username+" /mnt/Data/tmp/$TABLE.csv\n" +             
"chown -R "+username+" /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"ls -ltr \n" +
"\n" +
"echo \"loading table to POSTGRES\"\n" +
"#psql -U "+username+" -d $DATABASE_OUT -c \"DROP TABLE IF EXISTS $TABLE_OUT;\"\n" +
"#psql -U "+username+" -d $DATABASE_OUT -c \"CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA;\"\n" +
"#psql -U "+username+" -d $DATABASE_OUT -c \"COPY $TABLE FROM '/mnt/Data/tmp/$TABLE.csv' WITH DELIMITER AS '|';\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"DROP TABLE IF EXISTS $TABLE_OUT;\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA;\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"COPY $TABLE_OUT FROM '/mnt/Data/tmp/$TABLE_OUT.csv' WITH DELIMITER AS '|';\"\n" +            
"#rm /mnt/Data/tmp/$TABLE.csv\n" +            
"#rm -r /mnt/Data/tmp/$TABLE";
         
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    public void create_Data_Hive_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);
                
        
    String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"HDFS=/user/hive/warehouse\n" +
"BASE=/mnt/Data/tmp\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
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
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertCSV2Parquet.py $TABLE_OUT\n" +
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
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", py);
    }
    
    public void create_Data_Postgres_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);
                
        
    String sh = "#!/bin/bash\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +
"\n" +
"HDFS=/user/hive/warehouse\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
"SCHEMA=\""+Schema+"\"\n" +
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
"$HIVE_HOME/bin/hive -e \"DROP TABLE IF EXISTS $TABLE_OUT; CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; LOAD DATA INPATH '$HDFS/$TABLE.csv' OVERWRITE INTO TABLE $TABLE_OUT;\"\n" +
"\n" +
"rm -r /mnt/Data/tmp/$TABLE\n" + 
"rm -r /mnt/Data/tmp/$TABLE.csv";
         
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    
    public void create_Data_Postgres_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);
                
        
   String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +
"\n" +
"HDFS=/user/hive/warehouse\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
"SPARK_PORT=local[*]\n" +
"SCHEMA=\""+Schema+"\"\n" +
"\n" +
"echo -e \"DATABASE = \" $DATABASE\n" +
"echo -e \"TABLE = \" $TABLE\n" +
"echo -e \"SCHEMA = \" $SCHEMA\n" +
"echo -e \"SPARK_PORT = \" $SPARK_PORT\n" +
"\n" +
"rm /mnt/Data/tmp/tmp.csv\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"fi\n" +
"echo \"exporting table from postgres\"\n" +
"psql -U "+username+" -d $DATABASE -c \"COPY (SELECT * FROM $TABLE) TO '/mnt/Data/tmp/tmp.csv' WITH (DELIMITER '|', FORMAT csv)\"\n" +
"#chown letrung /mnt/Data/tmp/tmp.csv\n" +
"cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE\n" +
"#sudo cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/\n" +            
"mv /mnt/Data/tmp/$TABLE/tmp.csv /mnt/Data/tmp/$TABLE/$TABLE.csv\n" + 
"mv /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE.csv\n" +              
"#chown letrung /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#chown letrung /mnt/Data/tmp/$TABLE.csv\n" +
"#head /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#ls -la /mnt/Data/tmp/$TABLE/*.csv\n" +
"#echo -e \"Uploading $TABLE.csv to HDFS\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"#$HADOOP_HOME/bin/hdfs dfs -mkdir $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE.csv $HDFS\n" +
"#$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE.csv $HDFS\n" +
"#$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE.csv $HDFS\n" +            
"#$HADOOP_HOME/bin/hdfs dfs -moveFromLocal /mnt/Data/tmp/$TABLE/$TABLE.csv $HDFS/$TABLE\n" +
"echo -e \"Converting $TABLE.csv to parquet\"\n" +
"#chmod -R a+wrx $SPARK_HOME\n" +
"#$HADOOP_HOME/bin/hdfs dfs -chmod -R a+wrx $HDFS/$TABLE\n" +            
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.parquet\n" +
"#$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertCSV2Parquet.py $TABLE\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertCSV2Parquet.py $TABLE_OUT\n" +
"#$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +            
"rm -r /mnt/Data/tmp/$TABLE" +
"rm -r /mnt/Data/tmp/$TABLE.csv";

    String py ="from pyspark import SparkContext, SparkConf\n" +
"from pyspark.sql import SQLContext, HiveContext\n" +
"from pyspark.sql.types import *\n" +
"\n" +
"import os\n" +
"import sys\n" +
"import argparse\n" +
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
"	path = args.out + \"/converted\"\n" +
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
"	conf = SparkConf().setAppName( \"Convert CSV to Parquet\")\n" +
"	sc = SparkContext(conf=conf)\n" +
"	#HiveContext is a superset of SparkContext and for this is preferred\n" +
"	sqlContext = SQLContext(sc)\n" +
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
"        namenode = \"hdfs://localhost:9000\"\n" +
"        warehouse = \"/user/hive/warehouse\"\n" +
"        for file in args.src:\n" +
"                print( \"FILE IS:\", file)\n" +
"		#fileSchema = fileSchemas[ file.split( \"/\")[ -1].split( \".\")[ 0]]\n" +
"                if not file in fileSchemas.keys():\n" +
"                    df = sqlContext.read.format( \"com.databricks.spark.csv\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\").load( namenode + warehouse + \"/\" + file + \".csv\")\n" +
"                else:\n" +
"                    fileSchema = fileSchemas[ file]\n" +
"                    print( \"FILESCHEMA IS \", fileSchema)\n" +
"                    df = sqlContext.read.format( \"com.databricks.spark.csv\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\").load( namenode + warehouse + \"/\" + file + \".csv\", schema=fileSchema)\n" +
"                df.printSchema()\n" +
"#		for row in df.collect():\n" +
"#			print( row)\n" +
"		#filename = file[ file.rfind( \"/\") + 1: -4]\n" +
"		#print( filename)\n" +
"\n" +
"                output = file + \".parquet\"\n" +
"\n" +
"                if os.path.exists( namenode + warehouse + \"/\" + output):\n" +
"                    try:\n" +
"                        os.system( \"/usr/local/Cellar/hadoop/2.8.0/libexec/bin/hdfs dfs -rm -r \" + warehouse + \"/\" + output)\n" +
"                    except OSError:\n" +
"                        raise\n" +
"\n" +
"                #df.write.format( \"parquet\").save( namenode + warehouse + \"/\" + output)\n" +
"                df.write.save(namenode + warehouse + \"/\" + output, format = \"parquet\")\n" +
"                #df.write.format( \"parquet\").save( namenode + warehouse + \"/\" + output)\n" +
"\n" +
"if __name__ == \"__main__\":\n" +
"	main()";
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", py);
    }
    
    public void create_Data_Spark_Postgres(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);
                
        
    String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +
"HDFS=/user/hive/warehouse\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
"SPARK_PORT=local[*]\n" +
"SCHEMA=\""+Schema+"\"\n" +
"SQL_QUERY=\"DROP TABLE IF EXISTS $TABLE_OUT; CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA; COPY $TABLE_OUT FROM '/mnt/Data/tmp/$TABLE/$TABLE.csv' WITH DELIMITER AS '|';\"\n" +
"\n" +
"echo \"Exporting table from Spark\"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R "+username+" a+wrx /mnt/Data/tmp\n" +
"else\n" +
"	rm -r /mnt/Data/tmp/$TABLE/*\n" +
"fi\n" +
"#convert parquet file to csv\n" +
"#$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertParquet2CSV.py $HADOOP_HOME $TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyToLocal $HDFS/$TABLE.csv/* /mnt/Data/tmp/$TABLE\n" +
"ls -lah /mnt/Data/tmp/$TABLE\n" +
"if [ -f /mnt/Data/tmp/$TABLE/$TABLE.csv ]\n" +
"then\n" +
"	rm /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"fi\n" +
"for x in $(ls /mnt/Data/tmp/$TABLE/part-*);\n" +
"do\n" +
"       echo \"Copying file \"$x\n" +
"       cat $x >> /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"done\n" +
"ls -ltr /mnt/Data/tmp/$TABLE\n" +
"#from parquet file double quotes have been added to string values\n" +
"#and for this their length is changed. To restore their length the\n" +
"#extra double quotes are removed with sed\n" +
"sed -i 's/|\\\"/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"sed -i 's/\\\"|/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#rm /mnt/Data/tmp/temp_sed\n" +
"#head /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#chown -R "+username+" /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"ls -ltr /mnt/Data/tmp/$TABLE\n" +
"\n" +
"echo \"Loading table to POSTGRES\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"$SQL_QUERY\"\n" +
"#psql -U "+username+" -d $DATABASE -c \"DROP TABLE $TABLE\"\n" +
"#psql -U "+username+" -d $DATABASE -c \"CREATE TABLE $TABLE $SCHEMA\"\n" +
"#psql -U "+username+" -d $DATABASE -c \"COPY $TABLE FROM '/mnt/Data/tmp/$TABLE/$TABLE.csv' WITH DELIMITER AS '|'\"\n" +
"#clean\n" +
"rm -r /mnt/Data/tmp/$TABLE\n" +
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
"        df = sqlContext.read.parquet( namenode + warehouse + \"/\" + file + \".parquet\")\n" +
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
"        df.write.save(output, format=\"com.databricks.spark.csv\")\n" +
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
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", py);
    }
    
    public void create_Data_Spark_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String FILENAME = HOME + "/Documents/password.txt";
        String password = new TestPostgreSQLDatabase().readpass(FILENAME);
                
        
    String sh = "#!/bin/bash\n" +
"\n" +
"echo -e \"Move_Spark_Hive\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +
"HDFS=/user/hive/warehouse\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
"SPARK_PORT=local[*]\n" +
"SCHEMA=\""+Schema+"\"\n" +
"SQL_QUERY=\"DROP TABLE IF EXISTS $DATABASE_OUT.$TABLE_OUT; CREATE TABLE IF NOT EXISTS $DATABASE_OUT.$TABLE_OUT $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; LOAD DATA INPATH '/user/hive/warehouse/$TABLE.csv' OVERWRITE INTO TABLE $DATABASE_OUT.$TABLE_OUT\"\n" +
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
    
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", py);
    }
    
    
    public void createfile(String OperatorFolder,String filename, String content){
        
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
    
}
    

