/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import Algorithms.Algorithms;
import Algorithms.testScilab;
import Algorithms.testWriteMatrix2CSV;
import static IRES.TPCHQuery.calculateSize;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

/**
 *
 * @author letrung
 */
public class testQueryPlan {
    //    @Test 
    
    public static void testQueryPlanIRES_Hive_Postgres(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery();
        String From = "Hive";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        String Operator = "SQL";
        
        String DataIn = "database_tmp"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";        
        String Schema_Hive = "(KEY int, GENDER varchar(40))";
        Schema = Schema_Hive;
        String DataOut = "database_part_hive"; 
        String DatabaseOut = "mydb";
        
        
//        String SQL_Hive = "DROP TABLE IF EXISTS "+DatabaseOut+"."+DataOut+"; CREATE TABLE "+DatabaseOut+"."+DataOut+" AS SELECT * FROM "+DatabaseIn+"."+DataIn+";";
        String SQL_Postgres = "DROP TABLE IF EXISTS database_hive_postgres; "
                + "CREATE TABLE database_hive_postgres "
                + "AS SELECT * FROM database_part_hive,database_part_postgres "
                + "where database_part_postgres.custkey=database_part_hive.key;";
         
/*      String result = "database_hive_selection";
        String SQL_Postgres = "DROP TABLE IF EXISTS "+result+"; "
                + "CREATE TABLE "+result+" "
                + "AS SELECT * FROM database_part_hive "
                + "where database_part_hive.key=50;";
*/        
        String SQL = SQL_Postgres;
        YarnValue yarnValue = new YarnValue(Yarn[0], Yarn[1]);
        yarnValue.set_Ram(Yarn[0]);
        yarnValue.set_Core(Yarn[1]);
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
                
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
//        }
    }
//    @Test 
    public static void testQueryPlanIRES_Postgres(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery();
        String From = "Postgres";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        String Operator = "SQL";
        
        String DataIn = "database_postgres"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";        
        String Schema_Hive = "(KEY int, GENDER varchar(40))";
        Schema = Schema_Hive;
        String DataOut = "database_postgres_result"; 
        String DatabaseOut = "mydb";

//        String SQL_Hive = "DROP TABLE IF EXISTS "+DatabaseOut+"."+DataOut+"; CREATE TABLE "+DatabaseOut+"."+DataOut+" AS SELECT * FROM "+DatabaseIn+"."+DataIn+";";
        String SQL_Postgres = "DROP TABLE IF EXISTS database_postgres_result; "
                + "CREATE TABLE database_postgres_result "
                + "AS SELECT * FROM database_part_postgres,database_part_postgres_2 "
                + "where database_part_postgres.custkey=database_part_postgres_2.key;";
/*        String result = "database_hive_selection";
        String SQL_Postgres = "DROP TABLE IF EXISTS "+result+"; "
                + "CREATE TABLE "+result+" "
                + "AS SELECT * FROM database_part_hive "
                + "where database_part_hive.key=50;";
*/        String SQL = SQL_Postgres;
        
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
//        }
    }
    public static double[] createRandomYarn() {
        Random rand = new Random();
        double [] ram = {1024,1024,1024,1024,1024};
        double [] core = {1.0,1.0,1.0,1.0,1.0};

        double Ram = ram[rand.nextInt(ram.length)];
        double Core = core[rand.nextInt(core.length)];
        double[] Yarn = new double[2];
                
        Yarn[0] = Ram;
        Yarn[1] = Core;
//        tmp[4] = 0; //init of response time
        return Yarn;        
    }
    public static double[] createRandomParameter(double TimeOfDay, int N) {
        Random rand = new Random();
        double [] size = {100,100,100,100,100};
        double [] ram = {1024,1024,1024,1024,1024};
        double [] core = {1.0,1.0,1.0,1.0,1.0};
        double [] time = {0,0,0,0,0};
        
        double Data_size =  size[rand.nextInt(size.length)];
        double Ram = ram[rand.nextInt(ram.length)];
        double Core = core[rand.nextInt(core.length)];
        double[] tmp = new double[N];
                
        tmp[0] = Data_size + rand.nextInt(size.length);//Data_size;
        tmp[1] = Ram;
        tmp[2] = Core;
        tmp[3] = TimeOfDay;
//        tmp[4] = 0; //init of response time
        return tmp;        
    }
    
    public static String[] createRandomQuery() {
        Random rand = new Random();
        
        String [] dataset_move  = {"orders","lineitem", "orders","lineitem",    "orders","lineitem",    "customer","orders", "part","lineitem",     "lineitem","part",    "part","lineitem",     "customer","orders"};
        String [] query         = {"query0","query0",   "query4","query4",    "query12","query12",      "query13","query13",   "query14","query14",  "query17","query17",  "query19","query19",  "query22","query22"};
        String [] dataset_up    = {"lineitem","orders", "lineitem","orders",  "lineitem","orders",       "orders","customer",   "lineitem","part",   "part","lineitem",     "lineitem","part",    "orders","customer"};
        double [] size = new double [dataset_move.length];
        int i = rand.nextInt(dataset_move.length);//rand.nextInt(4) + 6;//
        String[] tmp = new String[4];       
        tmp[0] = Double.toString(size[i]);
        tmp[1] = dataset_move[i];
        tmp[2] = query[i];  
        tmp[3] = dataset_up[i];
        return tmp;        
    }
    
    public static double sizeDataset(String dataset, String Size_tpch){
        double size = 0;
        if (Size_tpch.contains("1000m")){
            switch (dataset) {
            case "nation":
                {
                size = 240.1;
                }
                break;
            case "region":
                {
                size = 240.1;
                }
                break;
            case "part":
                {
                size = 26;
                }
                break;
            case "supplier":
                {
                size = 2.8;
                }
                break;
            case "partsupp":
                {
                size = 240.1;
                }
                break;
            case "customer":
                {
                size = 23;
                }
                break; 
            case "orders":
                {
                size = 172;
                }
                break;
            case "lineitem":
                {
                size = 843;
                }
                break; 
            default:
                size = 240.1;
                break;    
            }
        }
            else  {if (Size_tpch.contains("100m")) {
                        switch (dataset) {
                case "nation":
                    {
                    size = 240.1;
                    }
                    break;
                case "region":
                    {
                    size = 240.1;
                    }
                    break;
                case "part":
                    {
                    size = 2.6;
                    }
                    break;
                case "supplier":
                    {
                    size = 2.8;
                    }
                    break;
                case "partsupp":
                    {
                    size = 240.1;
                    }
                    break;
                case "customer":
                    {
                    size = 2.3;
                    }
                    break; 
                case "orders":
                    {
                    size = 17;
                    }
                    break;
                case "lineitem":
                    {
                    size = 83;
                    }
                    break; 
                default:
                    size = 240.1;
                    break;    
                    }

                } 
            else  {if (Size_tpch.contains("10m")) {
                        switch (dataset) {
                case "nation":
                    {
                    size = 24;
                    }
                    break;
                case "region":
                    {
                    size = 24;
                    }
                    break;
                case "part":
                    {
                    size = 0.26;
                    }
                    break;
                case "supplier":
                    {
                    size = 0.28;
                    }
                    break;
                case "partsupp":
                    {
                    size = 24;
                    }
                    break;
                case "customer":
                    {
                    size = 0.23;
                    }
                    break; 
                case "orders":
                    {
                    size = 1.7;
                    }
                    break;
                case "lineitem":
                    {
                    size = 8.3;
                    }
                    break; 
                default:
                    size = 240.1;
                    break;    
                    }

                } 
        }
        }
        return size;
    } 
    public static double pageDataset(String dataset, String Size_tpch){
        double page = 0;
        if (Size_tpch.contains("1000m")){
        switch (dataset) {
            case "nation":
                {
                page = 240.1;
                }
                break;
            case "region":
                {
                page = 240.1;
                }
                break;
            case "part":
                {
                page = 4097;
                }
                break;
            case "supplier":
                {
                page = 2.8;
                }
                break;
            case "partsupp":
                {
                page = 8193;
                }
                break;
            case "customer":
                {
                page = 3585;
                }
                break; 
            case "orders":
                {
                page = 26095;
                }
                break;
            case "lineitem":
                {
                page = 112503;
                }
                break; 
            default:
                page = 240.1;
                break;    
            }
        }
            else  {if (Size_tpch.contains("100m")) {
                switch (dataset) {
                case "nation":
                    {
                    page = 240.1;
                    }
                    break;
                case "region":
                    {
                    page = 240.1;
                    }
                    break;
                case "part":
                    {
                    page = 410;
                    }
                    break;
                case "supplier":
                    {
                    page = 2.8;
                    }
                    break;
                case "partsupp":
                    {
                    page = 240.1;
                    }
                    break;
                case "customer":
                    {
                    page = 360;
                    }
                    break; 
                case "orders":
                    {
                    page = 2610;
                    }
                    break;
                case "lineitem":
                    {
                    page = 11259;
                    }
                    break; 
                default:
                    page = 240.1;
                    break;    
                    }
                } 
            else  {if (Size_tpch.contains("10m")) {
                switch (dataset) {
                case "nation":
                    {
                    page = 2.4;
                    }
                    break;
                case "region":
                    {
                    page = 2.4;
                    }
                    break;
                case "part":
                    {
                    page = 41;
                    }
                    break;
                case "supplier":
                    {
                    page = 2.8;
                    }
                    break;
                case "partsupp":
                    {
                    page = 2.4;
                    }
                    break;
                case "customer":
                    {
                    page = 36;
                    }
                    break; 
                case "orders":
                    {
                    page = 261;
                    }
                    break;
                case "lineitem":
                    {
                    page = 1126;
                    }
                    break; 
                default:
                    page = 2.4;
                    break;    
                    }
                } 
        }
        }
            
        return page;
    }
    public static double tupleDataset(String dataset, String Size_tpch){
        double tuple = 0;
         if (Size_tpch.contains("1000m")){
        switch (dataset) {
            case "nation":
                {
                tuple = 240.1;
                }
                break;
            case "region":
                {
                tuple = 240.1;
                }
                break;
            case "part":
                {
                tuple = 200000;
                }
                break;
            case "supplier":
                {
                tuple = 2.8;
                }
                break;
            case "partsupp":
                {
                tuple = 400000;
                }
                break;
            case "customer":
                {
                tuple = 150000;
                }
                break; 
            case "orders":
                {
                tuple = 1500000;
                }
                break;
            case "lineitem":
                {
                tuple = 6001215;
                }
                break; 
            default:
                tuple = 240.1;
                break;    
        } 
         }
            else  {if (Size_tpch.contains("100m")) {
                switch (dataset) {
                case "nation":
                    {
                    tuple = 240.1;
                    }
                    break;
                case "region":
                    {
                    tuple = 240.1;
                    }
                    break;
                case "part":
                    {
                    tuple = 20000;
                    }
                    break;
                case "supplier":
                    {
                    tuple = 2.8;
                    }
                    break;
                case "partsupp":
                    {
                    tuple = 240.1;
                    }
                    break;
                case "customer":
                    {
                    tuple = 15000;
                    }
                    break; 
                case "orders":
                    {
                    tuple = 150000;
                    }
                    break;
                case "lineitem":
                    {
                    tuple = 600572;
                    }
                    break; 
                default:
                    tuple = 240.1;
                    break;    
                    }
                } 
            else if (Size_tpch.contains("10m")) {
                switch (dataset) {
                case "nation":
                    {
                    tuple = 2.4;
                    }
                    break;
                case "region":
                    {
                    tuple = 2.4;
                    }
                    break;
                case "part":
                    {
                    tuple = 2000;
                    }
                    break;
                case "supplier":
                    {
                    tuple = 0.28;
                    }
                    break;
                case "partsupp":
                    {
                    tuple = 0.24;
                    }
                    break;
                case "customer":
                    {
                    tuple = 1500;
                    }
                    break; 
                case "orders":
                    {
                    tuple = 15000;
                    }
                    break;
                case "lineitem":
                    {
                    tuple = 60057;
                    }
                    break; 
                default:
                    tuple = 24;
                    break;    
                    }
                }
            
        }
            
        return tuple;
    }

}
