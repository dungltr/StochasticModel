/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import Algorithms.Algorithms;
import Algorithms.testWriteMatrix2CSV;
import static IRES.testQueryPlan.createRandomParameter;
import static IRES.testQueryPlan.createRandomQuery;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import com.sparkexample.App;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author letrung
 */
public class TPCHQuery {
    private static int numberOfTmp = 5;
    private static int YarnParamter = 2;
    private static int numberOfSize = 3;
    private static int numberOfSize_Hive_Postgres = 6;
    private static int numberOfSize_Postgres_Postgres = 6;
    private static int numberOfSize_Hive_Hive = 3;
    
    public static void TPCH_Hive_Postgres(double TimeOfDay, String Table, String query, String KindOfRunning) throws Exception {
        String Size_tpch = "1000m";
        String database = "tpch";
        String SQL_folder = new App().readhome("SQL");
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery(KindOfRunning, Size_tpch);
        String From = "Hive";
        String To   = "Postgres";
        
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        ////////////////////////////////////////////
        size[size.length-1]=TimeOfDay;
        ///////////////////////////////////////////
        
        String Operator = "TPCH_"+ Size_tpch;// +"_"+ randomQuery[2];           
        //String DataIn = Table;      
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        
        String DatabaseIn = database + Size_tpch;
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;       
       
        //String SQL_fileName = SQL_folder + "tpch_" + query; 
        String SQL_fileName = SQL_folder + randomQuery[2];
       
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
 //               + "drop table " + Table + ";";//SQL_Postgres;
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
               
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
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
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,KindOfRunning);
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay,size,KindOfRunning);
    }
    public static void TPCH_Hive_Hive(double TimeOfDay, String Table, String KindOfRunning) throws Exception {
        String Size_tpch = "100m";
        String database = "tpch";
        String SQL_folder = new App().readhome("SQL");
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery(KindOfRunning,Size_tpch);
        String From = "Hive";
        String To   = "Hive";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        ////////////////////////////////////////////
        size[size.length-1]=TimeOfDay;
        ///////////////////////////////////////////
        String Operator = "TPCH_"+Size_tpch;//+randomQuery[2];        
        String DataIn = Table; 
               DataIn = randomQuery[1];
        
        String DataInSize = Double.toString(size[0]);       
               
        String DatabaseIn = database + Size_tpch; 
        String Schema = Schema(DataIn);
        String DataOut = Table.toUpperCase(); 
               DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;       
        
     
        String SQL_fileName = SQL_folder + randomQuery[2];
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+" "
                + "AS " 
                + readSQL(SQL_fileName);
        
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
        
        if (!Files.exists(filePathRealValue))
        if (!Files.exists(filePathRealValue))
        {   IRES.createOperatorMove(Data, SQL, 0);            
//            Files.createFile(filePathRealValue);
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,KindOfRunning);
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay, size, KindOfRunning);
    }
    public static void TPCH_Postgres_Postgres(double TimeOfDay, String Table, String KindOfRunning) throws Exception {
        String Size_tpch = "100m";
        String database = "tpch";
        String SQL_folder = new App().readhome("SQL");
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery(KindOfRunning,Size_tpch);
        String From = "Postgres";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        double[] Yarn = testQueryPlan.createRandomYarn();
        ////////////////////////////////////////////
        size[size.length-1]=TimeOfDay;
        ///////////////////////////////////////////
        String Operator = "TPCH_"+Size_tpch;// + randomQuery[2];       
        String DataIn = Table; 
                DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);        
        String DatabaseIn = database + Size_tpch; 
        String Schema = Schema(DataIn);
        String DataOut = Table.toUpperCase(); 
                DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;       
        
     
        String SQL_fileName = SQL_folder + randomQuery[2];
        String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+" "
                + "AS " 
                + readSQL(SQL_fileName);
               SQL = SQL.toUpperCase();
//                + "Drop Table " + DataIn + ";";;
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

        if (!Files.exists(filePathRealValue))
        if (!Files.exists(filePathRealValue))
        {   IRES.createOperatorMove(Data, SQL, 0);            
//            Files.createFile(filePathRealValue);
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,KindOfRunning);
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay, size, KindOfRunning);
    }
    public static void TPCH(double TimeOfDay, String DB, String Size, String from, String to, String KindOfRunning) throws Exception {
        String Size_tpch = Size;
        String database = DB;
        String SQL_folder = new App().readhome("SQL");
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery(KindOfRunning, Size_tpch);
        String From = from;
        String To   = to;
        
        double[] size = calculateSize(randomQuery, From, To, Size_tpch);
        if (KindOfRunning.equals("testing")&&(From.equals("hive"))&&(To.equals("hive"))) size[1] = Double.parseDouble(randomQuery[0]); 
	double[] Yarn = testQueryPlan.createRandomYarn();
        ////////////////////////////////////////////
        size[size.length-1]=TimeOfDay;
        ///////////////////////////////////////////
        
        String Operator = "TPCH_"+ Size_tpch;// +"_"+ randomQuery[2];           
        //String DataIn = Table;      
        String DataIn = randomQuery[1];
        String DataInSize = Double.toString(size[0]);
        
        String DatabaseIn = database + Size_tpch;
        String Schema = Schema(DataIn);
        //String DataOut = Table.toUpperCase(); 
        String DataOut = randomQuery[1].toUpperCase();
        String DatabaseOut = database + Size_tpch;       
       
        String SQL_fileName = ""; 
        if (KindOfRunning.equals("training"))
	SQL_fileName = SQL_folder + randomQuery[2];
        else {
		if (To.toLowerCase().equals("postgres")) SQL_fileName = SQL_folder + randomQuery[2];
		else SQL_fileName = SQL_folder+randomQuery[2]; 
        }
	String SQL = "DROP TABLE IF EXISTS "+ randomQuery[2]+"_"+From+"_"+To+"; "
                + "CREATE TABLE "+ randomQuery[2]+"_"+From+"_"+To+" "
                + "AS " 
                + readSQL(SQL_fileName);
        SQL = SQL.toUpperCase();
 //               + "drop table " + Table + ";";//SQL_Postgres;
        
        System.out.println("\n"+SQL_fileName + "----SQL:---" + SQL);
        System.out.println("\nThe dataset is " + randomQuery[1] + " and the query is " + randomQuery[2]);
        System.out.println("\nThe Schema is " + Schema);
               
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
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
	if (TimeOfDay<1) delay_ys = "no_delay_";
        realValue = directory + "/"+delay_ys+KindOfRunning+"_realValue.csv";
//        parameter = directory + "/"+delay_ys+"Parameter.csv";
//        estimate = directory + "/"+delay_ys+"Estimate.csv";
        Path filePathRealValue = Paths.get(realValue); 

        if (!Files.exists(filePathRealValue))
        {   IRES.createOperatorMove(Data, SQL, 0);            
//            Files.createFile(filePathRealValue);
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,KindOfRunning);
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay, size, KindOfRunning);
    }    
    public static String Schema(String Table) {
        String Schema = "";
        String nation= "(N_NATIONKEY  INT, "
                + "N_NAME       CHAR(25), "
                + "N_REGIONKEY  INT, "
                + "N_COMMENT    VARCHAR(152))";
        String region = "(R_REGIONKEY  INT, "
                + "R_NAME       CHAR(25), "
                + "R_COMMENT    VARCHAR(152))";
        String part= "(P_PARTKEY     INT,"
                + "P_NAME        VARCHAR(55),"
                + "P_MFGR        CHAR(25),"
                + "P_BRAND       CHAR(10),"
                + "P_TYPE        VARCHAR(25),"
                + "P_SIZE        INT,"
                + "P_CONTAINER   CHAR(10),"
                + "P_RETAILPRICE DECIMAL(15,2),"
                + "P_COMMENT     VARCHAR(23)  )";
        String supplier= "(S_SUPPKEY     INT,"
                + "S_NAME        CHAR(25),"
                + "S_ADDRESS     VARCHAR(40),"
                + "S_NATIONKEY   INT,"
                + "S_PHONE       CHAR(15),"
                + "S_ACCTBAL     DECIMAL(15,2),"
                + "S_COMMENT     VARCHAR(101) )";

        String partsupp= "(PS_PARTKEY     INT,"
                + "PS_SUPPKEY     INT,"
                + "PS_AVAILQTY    INT,"
                + "PS_SUPPLYCOST  DECIMAL(15,2)  ,"
                + "PS_COMMENT     VARCHAR(199)  )";

        String customer= "(C_CUSTKEY     INT,"
                + "C_NAME        VARCHAR(25),"
                + "C_ADDRESS     VARCHAR(40),"
                + "C_NATIONKEY   INT,"
                + "C_PHONE       CHAR(15),"
                + "C_ACCTBAL     DECIMAL(15,2)   ,"
                + "C_MKTSEGMENT  CHAR(10),"
                + "C_COMMENT     VARCHAR(117) )";

        String orders = "(O_ORDERKEY       INT,"
                + "O_CUSTKEY        INT,"
                + "O_ORDERSTATUS    CHAR(1),"
                + "O_TOTALPRICE     DECIMAL(15,2),"
                + "O_ORDERDATE      DATE ,"
                + "O_ORDERPRIORITY  CHAR(15),"
                + "O_CLERK          CHAR(15),"
                + "O_SHIPPRIORITY   INT,"
                + "O_COMMENT        VARCHAR(79) )";

        String lineitem= "(L_ORDERKEY    INT,"
                + "L_PARTKEY     INT,"
                + "L_SUPPKEY     INT,"
                + "L_LINENUMBER  INT,"
                + "L_QUANTITY    DECIMAL(15,2),"
                + "L_EXTENDEDPRICE  DECIMAL(15,2),"
                + "L_DISCOUNT    DECIMAL(15,2),"
                + "L_TAX         DECIMAL(15,2),"
                + "L_RETURNFLAG  CHAR(1),"
                + "L_LINESTATUS  CHAR(1),"
                + "L_SHIPDATE    DATE ,"
                + "L_COMMITDATE  DATE ,"
                + "L_RECEIPTDATE DATE ,"
                + "L_SHIPINSTRUCT CHAR(25),"
                + "L_SHIPMODE     CHAR(10),"
                + "L_COMMENT      VARCHAR(44) )";
        switch (Table) {
            case "nation":
                {
                Schema = nation;
                }
                break;
            case "region":
                {
                Schema = region;
                }
                break;
            case "part":
                {
                Schema = part;
                }
                break;
            case "supplier":
                {
                Schema = supplier;
                }
                break;
            case "partsupp":
                {
                Schema = partsupp;
                }
                break;
            case "customer":
                {
                Schema = customer;
                }
                break; 
            case "orders":
                {
                Schema = orders;
                }
                break;
            case "lineitem":
                {
                Schema = lineitem;
                }
                break; 
            default:
                Schema = lineitem;
                break;    
        }        
        return Schema;
    } 
    public static String readSQL(String filename) {
       String sCurrentLine = "";
       String SQL = sCurrentLine;
       try (BufferedReader br = new BufferedReader(new FileReader(filename+".sql"))) {
			while ((sCurrentLine = br.readLine()) != null) {
                            SQL = SQL + " " + sCurrentLine;                            
			}
                        return SQL;
		} catch (IOException e) {
			e.printStackTrace();                       
		}
       return SQL;
    }
    public static double[] calculateSize(String[] randomQuery, String From, String To, String Size_tpch) {
        double R1,R2;
        if ((From.toLowerCase().contains("hive"))&&(To.toLowerCase().contains("postgres"))) {
            double[] size = new double[numberOfSize_Hive_Postgres];
            size[0] = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);
            size[1] = testQueryPlan.pageDataset(randomQuery[1],Size_tpch);
            size[2] = testQueryPlan.tupleDataset(randomQuery[1],Size_tpch);
            size[3] = testQueryPlan.pageDataset(randomQuery[3],Size_tpch);
            size[4] = testQueryPlan.tupleDataset(randomQuery[3],Size_tpch);
            return size;
        }
        else {
            if ((From.toLowerCase().contains("hive"))&&(To.toLowerCase().contains("hive"))) {
            double[] size = new double[numberOfSize_Hive_Hive];
            size[0] = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);
            size[1] = testQueryPlan.sizeDataset(randomQuery[3],Size_tpch);
            return size;
            } 
            else {
                if ((From.toLowerCase().contains("postgres"))&&(To.toLowerCase().contains("postgres"))) {
                    double[] size = new double[numberOfSize_Postgres_Postgres];
                    size[0] = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);
                    size[1] = testQueryPlan.pageDataset(randomQuery[1],Size_tpch);
                    size[2] = testQueryPlan.tupleDataset(randomQuery[1],Size_tpch);
                    size[3] = testQueryPlan.pageDataset(randomQuery[3],Size_tpch);
                    size[4] = testQueryPlan.tupleDataset(randomQuery[3],Size_tpch);
                    return size;
                }
                else {
                double[] size = new double[numberOfSize];
                R1 = testQueryPlan.sizeDataset(randomQuery[1],Size_tpch);// Size of Data In R1
                R2 = testQueryPlan.sizeDataset(randomQuery[3],Size_tpch);// Size of Data In R2
                size[0] = R1;// + R2 + R1*Math.log(R1) + R2*Math.log(R2);
                size[1] = R1*R2;
                size[2] = R2;
                return size;
                }
            }    
        }
        
    }
    
}
