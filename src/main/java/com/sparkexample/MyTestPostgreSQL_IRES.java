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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author letrung
 */
public class MyTestPostgreSQL_IRES {
    String HOME=System.getenv().get("HOME");
    String Create_1 = "CREATE TABLE COMPANY " +
                      "(ID INT PRIMARY KEY     NOT NULL," +
                      " NAME           TEXT    NOT NULL, " +
                      " AGE            INT     NOT NULL, " +
                      " ADDRESS        CHAR(50), " +
                      " SALARY         REAL);";
    String Insert_1 = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" +
            "VALUES (1, 'Paul', 32, 'California', 20000.00 );"
            + "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)"
            + "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );"
            + "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)"
            + "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );"
            + "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)"
            + "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );";
    
    
    String Create_2 = "CREATE TABLE DEPARTMENT(" +
"   ID INT PRIMARY KEY      NOT NULL," +
"   DEPT           CHAR(50) NOT NULL," +
"   EMP_ID         INT      NOT NULL" +
");";
    String Insert_2 = "INSERT INTO DEPARTMENT (ID, DEPT, EMP_ID)" +
"VALUES (1, 'IT Billing', 1 );" +
"INSERT INTO DEPARTMENT (ID, DEPT, EMP_ID)" +
"VALUES (2, 'Engineering', 2 );" +
"INSERT INTO DEPARTMENT (ID, DEPT, EMP_ID)" +
"VALUES (3, 'Finance', 7 );";
    String CROSS_JOIN ="SELECT EMP_ID, NAME, DEPT FROM COMPANY CROSS JOIN DEPARTMENT;"; 
    String INNER_JOIN ="SELECT EMP_ID, NAME, DEPT FROM COMPANY INNER JOIN DEPARTMENT\n" +
"        ON COMPANY.ID = DEPARTMENT.EMP_ID;"; 
    String LEFT_OUTER_JOIN ="SELECT EMP_ID, NAME, DEPT FROM COMPANY LEFT OUTER JOIN DEPARTMENT\n" +
"        ON COMPANY.ID = DEPARTMENT.EMP_ID;";
    String RIGHT_OUTER_JOIN = "SELECT EMP_ID, NAME, DEPT FROM COMPANY RIGHT OUTER JOIN DEPARTMENT\n" +
"        ON COMPANY.ID = DEPARTMENT.EMP_ID;";
    String FULL_OUTER_JOIN ="SELECT EMP_ID, NAME, DEPT FROM COMPANY FULL OUTER JOIN DEPARTMENT\n" +
"        ON COMPANY.ID = DEPARTMENT.EMP_ID;";
    String SQL = "CREATE TABLE COMPANYANDDEPARTMENT AS "+CROSS_JOIN;
    public void testPostgreSQL_IRES(String IRES_library, String NameOfHost, int int_localhost) throws Exception {
        String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
        String AlgorithmsName = "SQL_query";
        String NameOfAbstractWorkflow = "PostgreSQL_IRES_Workflow";
        String NameOfAbstractOperator = "PgreSQL_IRES";
        String NameOp = "PostgreSQL_IRES";
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
    public void createDocuments(String OperatorFolder,String Name){
        String Database_Hive = "mydb";
        String Database_Postgres = "mydb";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table = "customer";
        String username = System.getProperty("user.name");
        String HOME=System.getenv().get("HOME");
        String lua ="operator = yarn {\n" +
"  name = \""+Name+"\",\n" +
"  timeout = 10000,\n" +
"  memory = 1024,\n" +
"  cores = 1,\n" +
"  container = {\n" +
"    instances = 1,\n" +
"    --env = base_env,\n" +
"    resources = {\n" +
"    [\""+Name+".sh\"] = {\n" +
"       file = \"asapLibrary/operators/"+Name+"/"+Name+".sh\",\n" +
"                type = \"file\",               -- other value: 'archive'\n" +
"                visibility = \"application\"  -- other values: 'private', 'public'\n" +
"        },\n" +
"	  [\"description\"] = {\n" +
"       file = \"asapLibrary/operators/"+Name+"/description\",\n" +
"                type = \"file\",               -- other value: 'archive'\n" +
"                visibility = \"application\"  -- other values: 'private', 'public'\n" +
"        }\n" +
"    },\n" +
"    command = {\n" +
"        base = \"./"+Name+".sh\"\n" +
"    }\n" +
"  }\n" +
"}";
//    String Postgres_GroupBy_Sort_sh = "#!/bin/bash\n" +
//"sudo -u postgres psql $1 -c \"SELECT NATIONKEY, SUM(TOTALPRICE) FROM $2 GROUP BY NATIONKEY ORDER BY SUM(TOTALPRICE)\" > $3";
    String sh = "#!/bin/bash\n"// +
//            + "sudo -u "+username+" hive -e 'CREATE DATABASE IF NOT EXISTS userdb;'\n"
//            + "hive\n"
/*            + "sudo -u letrung hive -e 'CREATE DATABASE IF NOT EXISTS familydungbinh;'\n" +
"sudo -u letrung hive -e 'DROP TABLE IF EXISTS family;'\n" +
"sudo -u letrung hive -e 'CREATE TABLE IF NOT EXISTS familydungbinh.family (age int, name string);'\n" +
"sudo -u letrung hive -e 'INSERT INTO familydungbinh.family VALUES (37, \"Le Trung Dung Test\");'\n" +
"sudo -u letrung hive -e 'INSERT INTO familydungbinh.family VALUES (36, \"Nguyen Thi Thanh Binh Test\");'\n" +
"sudo -u letrung hive -e 'INSERT INTO familydungbinh.family VALUES (9, \"Le Trung Tien Dat Test \");'\n" +
"sudo -u letrung hive -e 'INSERT INTO familydungbinh.family VALUES (4, \"Le Thanh An Test\");'"
//            + "create database if not exits userdb_line\n"
*/            +"psql -U "+username+" mydb -c \""+SQL+"\"\n";
    
        createlua(OperatorFolder + "/" + Name, Name + ".lua", lua); 
        createlua(OperatorFolder + "/" + Name, Name + ".sh", sh);
    }
}
