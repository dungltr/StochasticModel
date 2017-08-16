/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import Algorithms.Algorithms;
import Algorithms.CreateDatabase;
import Algorithms.testScilab;
import Algorithms.testWriteMatrix2CSV;
import static IRES.TPCHQuery.calculateSize;
import static IRES.testQueryPlan.createRandomParameter;
import static IRES.testQueryPlan.createRandomQuery;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

/**
 *
 * @author letrung
 */
public class testCreateDatabase {
    //    @Test 
    public static void testCreateHiveDataBase(double TimeOfDay) throws Exception {
        
        String Size_tpch = "100m";
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery();
        String From = "Hive";
        String To   = "Hive";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        String Operator = "Move";
        String DataIn = "database"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Schema_Hive = "(KEY int, GENDER varchar(40))";
        String DataOut = "database_tmp"; 
        String DatabaseOut = "mydb";
        
        
//        String SQL_Hive = "DROP TABLE IF EXISTS "+DatabaseOut+"."+DataOut+"; CREATE TABLE "+DatabaseOut+"."+DataOut+" AS SELECT * FROM "+DatabaseIn+"."+DataIn+";";
//        String SQL_Postgres = "DROP TABLE IF EXISTS "+DataOut+"; CREATE TABLE "+DataOut+" AS SELECT * FROM "+DataIn+";";
        String SQL;// = SQL_Postgres;
//        SQL = CreateDatabase.setupDataBasePostgres(DataOut, Schema);
        SQL = CreateDatabase.setupDataBase_HIVE(DataOut, Schema_Hive);
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
       
              
       YarnValue yarnValue = new YarnValue(Yarn[0], Yarn[1]);
        yarnValue.set_Ram(Yarn[0]);
        yarnValue.set_Core(Yarn[1]);
                
        String realValue, parameter, estimate, directory;
        directory = testWriteMatrix2CSV.getDirectory(Data);
        
        realValue = directory + "/realValue.csv";
        parameter = directory + "/Parameter.csv";
        estimate = directory + "/Estimate.csv";
        Path filePathRealValue = Paths.get(realValue); 
        
//        IRES.create_Start_Workflow(Data);
//        IRES.create_Stop_Workflow(Data,size[0]);
        
        if (!Files.exists(filePathRealValue))
        {   IRES.createOperatorMove(Data, SQL, 0);            
//            Files.createFile(filePathRealValue);
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay);
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay,size);
    }
//    @Test 
    public static void testCreatePostgresDataBase(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery();
        String From = "Postgres";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        
        String Operator = "Move";
        String DataIn = "customer"; 
        String DatabaseIn = "mydb"; 
        String DataInSize  = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Schema_Hive = "(KEY int, GENDER varchar(40))";
        String DataOut = "database_part_postgres"; 
        String DatabaseOut = "mydb";
        
        
//        String SQL_Hive = "DROP TABLE IF EXISTS "+DatabaseOut+"."+DataOut+"; CREATE TABLE "+DatabaseOut+"."+DataOut+" AS SELECT * FROM "+DatabaseIn+"."+DataIn+";";
//        String SQL_Postgres = "DROP TABLE IF EXISTS "+DataOut+"; CREATE TABLE "+DataOut+" AS SELECT * FROM "+DataIn+";";
        String SQL;// = SQL_Postgres;
        SQL = CreateDatabase.setupDataBasePostgres(DataOut, Schema);
//        SQL = CreateDatabase.setupDataBase_HIVE(DataOut, Schema_Hive);
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        
        YarnValue yarnValue = new YarnValue(Yarn[0], Yarn[1]);
        yarnValue.set_Ram(Yarn[0]);
        yarnValue.set_Core(Yarn[1]);
                
        String realValue, parameter, estimate, directory;
        directory = testWriteMatrix2CSV.getDirectory(Data);
        
        realValue = directory + "/realValue.csv";
        parameter = directory + "/Parameter.csv";
        estimate = directory + "/Estimate.csv";
        Path filePathRealValue = Paths.get(realValue); 

        if (!Files.exists(filePathRealValue))
        {   IRES.createOperatorMove(Data, SQL, 0);            
//            Files.createFile(filePathRealValue);
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay);
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay,size);
    }
    
}
