/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Standalone;

import Algorithms.testWriteMatrix2CSV;
import static IRES.TPCHQuery.Schema;
import static IRES.TPCHQuery.calculateSize;
import static IRES.TPCHQuery.readSQL;
import IRES.testQueryPlan;
import static IRES.testQueryPlan.createRandomQuery;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import com.sparkexample.App;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author letrung
 */
public class TPCHStandalone {
//    private static int numberOfTmp = 5;
//    private static int YarnParamter = 2;
//    private static int numberOfSize = 3;
//    private static int numberOfSize_Hive_Postgres = 6;
//    private static int numberOfSize_Hive_Hive = 2;
    public static void TPCH_Standalone_Hive_Postgres(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        String database = "tpch";
        String SQL_folder = new App().readhome("SQL");
        String[] randomQuery = createRandomQuery("",Size_tpch);
        String From = "Hive";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        size[size.length-1] = TimeOfDay;
       
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        String DatabaseIn = database + Size_tpch; 
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;        
        
        
        String Operator = "TPCH_StandAlone_" + Size_tpch + "_" + randomQuery[2];

        String SQL_fileName = SQL_folder + randomQuery[2];
       
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"St"+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+"St"+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
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
    
        StandaloneAlgorithms Algor = new StandaloneAlgorithms();
        if (!Files.exists(filePathRealValue))
        {            
            Algor.setup(Data,yarnValue,size,Size_tpch,TimeOfDay);
        }
        Algor.mainStandalone(Data, SQL, yarnValue, TimeOfDay,size);       
    } 
    public static void TPCH_Standalone_Hive_Hive(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        String SQL_folder = new App().readhome("SQL");
        String[] randomQuery = createRandomQuery("",Size_tpch);
        String From = "Hive";
        String To   = "Hive";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
//        size[1] = 1024/Yarn[0];
        size[size.length-1] = TimeOfDay;    
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        String DatabaseIn = "tpch"; 
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = "tpch";       
        
        
        String Operator = "TPCH_StandAlone_"+ randomQuery[2];

        String SQL_fileName = SQL_folder + randomQuery[2];
       
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"St"+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+"St"+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
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
    
        StandaloneAlgorithms Algor = new StandaloneAlgorithms();
        if (!Files.exists(filePathRealValue))
        {            
            Algor.setup(Data,yarnValue,size,Size_tpch,TimeOfDay);
        }
        Algor.mainStandalone(Data, SQL, yarnValue, TimeOfDay,size);        
    }  
    public static void TPCH_Standalone_Postgres_Postgres(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        String database = "tpch";
        String SQL_folder = new App().readhome("SQL");
        String[] randomQuery = createRandomQuery("",Size_tpch);
        String From = "Postgres";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        size[size.length-1] = TimeOfDay;
       
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        String DatabaseIn = database + Size_tpch; 
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;       
        
        
        String Operator = "TPCH_StandAlone_"+ randomQuery[2];

        String SQL_fileName = SQL_folder + randomQuery[2];
       
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"St"+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+"St"+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
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
    
        StandaloneAlgorithms Algor = new StandaloneAlgorithms();
        if (!Files.exists(filePathRealValue))
        {            
            Algor.setup(Data,yarnValue,size,Size_tpch,TimeOfDay);
        }
        Algor.mainStandalone(Data, SQL, yarnValue, TimeOfDay,size);        
    }
    public static void TPCH_Standalone(double TimeOfDay, String DB, String Size, String from, String to) throws Exception {
        String Size_tpch = Size;
        String database = DB;
        String SQL_folder = new App().readhome("SQL");
        String[] randomQuery = createRandomQuery("",Size_tpch);
        String From = from;
        String To   = to;
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        size[size.length-1] = TimeOfDay;
       
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        String DatabaseIn = database + Size_tpch; 
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;        
        
        
        String Operator = "TPCH_StandAlone_" + Size_tpch + "_" + randomQuery[2];

        String SQL_fileName = SQL_folder + randomQuery[2];
       
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"St"+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+"St"+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
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
        String delay_ys = "";
	if (TimeOfDay < 1) delay_ys = "no_delay";
        realValue = directory + "/"+delay_ys+"realValue.csv";
        parameter = directory + "/"+delay_ys+"Parameter.csv";
        estimate = directory + "/"+delay_ys+"Estimate.csv";
        Path filePathRealValue = Paths.get(realValue); 
    
        StandaloneAlgorithms Algor = new StandaloneAlgorithms();
        if (!Files.exists(filePathRealValue))
        {            
            Algor.setup(Data,yarnValue,size,Size_tpch,TimeOfDay);
        }
        Algor.mainStandalone(Data, SQL, yarnValue, TimeOfDay,size);       
    }
}
