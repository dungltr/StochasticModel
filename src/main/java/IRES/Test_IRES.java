/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import Algorithms.Algorithms;
import Algorithms.Writematrix2CSV;
import Algorithms.testWriteMatrix2CSV;
import LibraryIres.Move_Data;
import LibraryIres.Move_IRES;
import LibraryIres.createWorkflow;
import LibraryIres.runWorkflow;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author letrung
 */
public class Test_IRES {
    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";
    //    @Test
    public void test_Move_IRES() throws Exception {
        Move_IRES testSQLEngine = new Move_IRES();
        String Operator = "Move";
        String DataIn = "customer"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        
        String DataOut = "customer"; 
        String DatabaseOut = "mydb";
        
        String From = "hive";
        String To   = "postgres";
        testSQLEngine.MoveData(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);        
        To   = "spark";    
//        testSQLEngine.MoveData(DataIn, DatabaseIn, Schema, From, To, DataOut, DatabaseOut); 
        
        From = "postgres";
        To   = "spark";
        DataOut = "customer_postgres"; 
//        testSQLEngine.MoveData(DataIn, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        To   = "hive";    
//        testSQLEngine.MoveData(DataIn, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        
        From = "spark";
        To   = "hive";
        DataOut = "customer_spark"; 
//        testSQLEngine.MoveData(DataIn, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        To   = "postgres";    
//        testSQLEngine.MoveData(DataIn, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        
    }
//        @Test 
        public void test_workflow_Move () throws Exception {
        Thread.sleep(500);
        createWorkflow CreateWorkflow = new createWorkflow();
        Move_IRES testSQL = new Move_IRES();
        String Operator = "Move";
        String DataIn = "customer"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        
        String DataOut = "customer_hive"; 
        String DataOutSize = "200";
        String DatabaseOut = "mydb";
        
        String From = "hive";
        String To   = "postgres";
        String SQL = "";
        
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        
        CreateWorkflow.createAbstractOperatorMove(Data, SQL);
        CreateWorkflow.createOperatorMove(Data, SQL);        
        CreateWorkflow.createDatasetMove(Data, SQL);
        CreateWorkflow.createWorkflowMove(Data, SQL);
        CreateWorkflow.createDataMove(Data, SQL);
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
    
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();   
        String NameOfWorkflow = NameOp+"_Workflow";
        
        runWorkflow workflow = new runWorkflow();
        workflow.runWorkflow(NameOfWorkflow, policy);
        Thread.sleep(500);
    }
        //    @Test 
        public void test_workflow_SQL () throws Exception {
        Thread.sleep(500);    
        createWorkflow CreateWorkflow = new createWorkflow();
        Move_IRES testSQL = new Move_IRES();
        String Operator = "Move";
        String DataIn = "customer_hive"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        
        String DataOut = "customer_out";
        String DataOutSize = "200";
        String DatabaseOut = "mydb";
        
        String From = "postgres";
        String To   = "postgres";
        String SQL = "CREATE TABLE IF NOT EXISTS "+DataOut+" AS SELECT * FROM "+DataIn+";";
        
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        
        CreateWorkflow.createAbstractOperatorSQL(Data, SQL);
        CreateWorkflow.createOperatorSQL(Data, SQL);        
        CreateWorkflow.createDatasetSQL(Data, SQL);
        CreateWorkflow.createWorkflowSQL(Data, SQL);
        CreateWorkflow.createDataSQL(Data, SQL);
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min";     
        
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();   
        String NameOfWorkflow = NameOp+"_Workflow";
        
        runWorkflow workflow = new runWorkflow();
        workflow.runWorkflow(NameOfWorkflow, policy);
    }
    

//        @ Test
    public void testcontrol() throws Exception {    
        createWorkflow CreateWorkflow = new createWorkflow();
        String Operator = "Move";
        String DataIn = "customer"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        
        String DataOut = "customer_hive"; 
        String DataOutSize = "200";
        String DatabaseOut = "mydb";
        
        String From = "HIVE";
        String To   = "POSTGRES";
        
        String SQL = "";
        int M = 5;
        double[] Data_size = {1,2,3,4,2};
        double[] Ram = {512,512,512,512,512};
        double[] Core = {1,2,1,2,2};
        double[] Time_Cost = new double[M];
//        String SQL = "CREATE TABLE IF NOT EXISTS "+DataOut+" AS SELECT * FROM "+DataIn+";";
        
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        
        CreateWorkflow.createAbstractOperatorMove(Data, SQL);
        CreateWorkflow.createOperatorMove(Data, SQL);        
        CreateWorkflow.createDatasetMove(Data, SQL);
        CreateWorkflow.createWorkflowMove(Data, SQL);
//        CreateWorkflow.createDataMove(Data, SQL);
        
        String realValue, parameter, directory;
        directory = testWriteMatrix2CSV.getDirectory(Data);
        realValue = directory + "/realValue.csv";
        parameter = directory + "/Parameter.csv";
        double[][] tmp = new double[M][4];
        double[][] Parameter = new double[1][4];
        for (int i = 0; i < M; i ++)
        {
        Data.set_DataIn(DataIn+i);
        Data.set_DataOut(DataOut+i);
        SQL = "CREATE TABLE IF NOT EXISTS "+Data.get_DataIn()+" AS SELECT * FROM "+Data.get_DataOut()+";";
        CreateWorkflow.createDataMove2(Data, SQL, Data_size[i], Ram[i], Core[i]);
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min"; 
    
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();   
        String NameOfWorkflow = NameOp+"_Workflow";
        
        runWorkflow workflow = new runWorkflow();
        
        Time_Cost[i] = workflow.runWorkflow2(NameOfWorkflow, policy);
        testWriteMatrix2CSV.store(Data, SQL, Data_size[i], Ram[i]/1000, Core[i], Time_Cost[i], realValue); 
        tmp[i][0]= Data_size[i];
        tmp[i][1]= Ram[i];
        tmp[i][2]= Core[i];
        tmp[i][3]= Time_Cost[i];       
        }  
        Path filePath = Paths.get(realValue);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
                int n = tmp[0].length;
                int i = 0;
                String FILE_HEADER = "";
                for (i = 0; n -1 >i; i++)
                FILE_HEADER = FILE_HEADER + "b[" + i + "]" + COMMA_DELIMITER;
                if (n - 1 == i) FILE_HEADER = FILE_HEADER + "b[" + i + "]";
                Writematrix2CSV.Writematrix2CSV(tmp, realValue, FILE_HEADER);
                }
        filePath = Paths.get(parameter);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
                int n = Parameter[0].length;
                int i = 0;
                String FILE_HEADER = "";
                for (i = 0; n -1 >i; i++)
                FILE_HEADER = FILE_HEADER + "b[" + i + "]" + COMMA_DELIMITER;
                if (n - 1 == i) FILE_HEADER = FILE_HEADER + "b[" + i + "]";
                Writematrix2CSV.Writematrix2CSV(Parameter, parameter, FILE_HEADER);
                }            
//        Algorithms.estimateCost(M, realValue, parameter);
    }
    
}
