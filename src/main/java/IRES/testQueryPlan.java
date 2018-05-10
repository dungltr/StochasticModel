/**
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import Algorithms.Algorithms;
import Algorithms.testWriteMatrix2CSV;
import Irisa.Enssat.Rennes1.thesis.sparkSQL.ReadMatrixCSV;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

import static IRES.TPCHQuery.calculateSize;
/**
 *
 * @author letrung
 */
public class testQueryPlan {
    //    @Test 
    
    public static void testQueryPlanIRES_Hive_Postgres(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery("",Size_tpch);
        String From = "Hive";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch,"");
        double[] Yarn = testQueryPlan.createRandomYarn();
        String Operator = "SQL";
        
        String DataIn = "database_tmp"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";        
        String Schema_Hive = "(KEY int, GENDER varchar(40))";
        Schema = Schema_Hive;
        String DataOut = "database_part_hive"; 
        String DataOutSize = "200";
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
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
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
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,"");
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay,size,"");         
//        }
    }
//    @Test 
    public static void testQueryPlanIRES_Postgres(double TimeOfDay) throws Exception {
        String Size_tpch = "100m";
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        String[] randomQuery = createRandomQuery("",Size_tpch);
        String From = "Postgres";
        String To   = "Postgres";
        double[] size = calculateSize(randomQuery, From, To, Size_tpch,"");
        double[] Yarn = testQueryPlan.createRandomYarn();
        String Operator = "SQL";
        
        String DataIn = "database_postgres"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";        
        String Schema_Hive = "(KEY int, GENDER varchar(40))";
        Schema = Schema_Hive;
        String DataOut = "database_postgres_result"; 
        String DataOutSize = "200";
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
        
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DataOutSize, DatabaseOut);
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
            Algorithms.setup(Data,yarnValue,size,Size_tpch,TimeOfDay,"");
        }
        Algorithms.mainIRES(Data, SQL, yarnValue, TimeOfDay,size,"");         
//        }
    }
    public static double[] createRandomYarn() {
        Random rand = new Random();
        double [] ram = {1024,1024,1024,1024,1024};
        double [] core = {2.0,2.0,2.0,2.0,2.0};

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
    
    public static String[] createRandomQuery(String KindOfRunning, String Size_tpch) {
        Random rand = new Random();
//        String [] dataset_move  = {"orders","lineitem", "orders","lineitem",    "orders","lineitem",    "customer","orders", "part","lineitem",     "lineitem","part",    "part","lineitem",     "customer","orders"};
//        String [] query         = {"query0","query0",   "query4","query4",    "query12","query12",      "query13","query13",   "query14","query14",  "query17","query17",  "query19","query19",  "query22","query22"};
//        String [] dataset_up    = {"lineitem","orders", "lineitem","orders",  "lineitem","orders",       "orders","customer",   "lineitem","part",   "part","lineitem",     "lineitem","part",    "orders","customer"};
        String [] dataset_move  = {"orders","customer","part","lineitem"};
        String [] query         = {"query12","query13","query14","query17"};
        String [] dataset_up    = {"lineitem","orders","lineitem","part"};
        double [] size = new double [dataset_move.length];
        int i = rand.nextInt(dataset_move.length);//rand.nextInt(4) + 6;//
        int j,k;
	int [] SQLJoin2Table = {12,13,14,15,16,17,19,22};
	double size_multi = 0;
	String[] tmp = new String[4];
	if (KindOfRunning.equals("testing")){
            i = SQLJoin2Table[rand.nextInt(SQLJoin2Table.length)];
            tmp[1] = dataset_move(Integer.toString(i));
            //String [] SliceArray = Checkquery(Integer.toString(i));
            String [] SliceArray = sliceArray(Checkquery(Integer.toString(i)),tmp[1]);
            for (k = 0; k < SliceArray.length; k++)
                    size_multi = size_multi + testQueryPlan.sizeDataset(SliceArray[k],Size_tpch);
            tmp[0] = Double.toString(size_multi);
            System.out.println("Size of tables except:" + tmp[0]);
            //tmp[1] = dataset_move(Integer.toString(i));
            tmp[2] = "tpch_query"+Integer.toString(i);
            tmp[3] = SliceArray[0];
            System.out.println("Table 1" + tmp[1]);
            System.out.println("Table 2" + tmp[3]);
	}
	else{
	    tmp[0] = Double.toString(size[i]);
        tmp[1] = dataset_move[i];
        tmp[2] = query[i];  
        tmp[3] = dataset_up[i];
	}
        return tmp;        
    }
    public static String[] createRandomQuery(String DB, String KindOfRunning, String Size_tpch) {
        Random rand = new Random();
      if (DB.toLowerCase().contains("tpch")){
            return createRandomQuery(KindOfRunning,Size_tpch);
        }
        else {
            if (DB.toLowerCase().contains("dicom")){
                String [] dataset_move  = {
                        "generalseries_text"//good
                        , "studyall_text"//good
                        , "patientall_text"//good
                        , "clinicaltrial_text"//good
                        , "filemetaelement_text"
                        ,"sequenceattributes_text"
                        };
                        //
                        //,"generalinfotable_text"
                        //};
                String [] query         = {
                        "query31"
                        ,"query32"
                        ,"query33"
                        ,"query34"
                        ,"query35"
                        ,"query36"
                        ,"query37"};
                String [] dataset_up    = dataset_move;/*{
                        "generalseries_text"//good
                        , "studyall_text"//good
                        , "patientall_text"//good
                        , "clinicaltrial_text"//good
                        , "filemetaelement_text"//good
                        }*/
                //,"sequenceattributes_text"
                //,"generalinfotable_text"
                //};
                double [] size = new double [dataset_move.length];
                int i = rand.nextInt(dataset_move.length);//rand.nextInt(4) + 6;//
                int j,k;
                int [] SQLJoin2Table = {31,32,33,34,35,36,37};
                double size_multi = 0;
                String[] tmp = new String[4];
                if (KindOfRunning.equals("testing")){
                    i = SQLJoin2Table[rand.nextInt(SQLJoin2Table.length)];
                    tmp[1] = dataset_move(Integer.toString(i));
                    //String [] SliceArray = Checkquery(Integer.toString(i));
                    String [] SliceArray = sliceArray(Checkquery(Integer.toString(i)),tmp[1]);
                    for (k = 0; k < SliceArray.length; k++)
                        size_multi = size_multi + testQueryPlan.sizeDataset(SliceArray[k],Size_tpch);
                    tmp[0] = Double.toString(size_multi);
                    System.out.println("Size of tables except:" + tmp[0]);
                    //tmp[1] = dataset_move(Integer.toString(i));
                    tmp[2] = "tpch_query"+Integer.toString(i);
                    tmp[3] = SliceArray[0];
                    System.out.println("Table 1" + tmp[1]);
                    System.out.println("Table 2" + tmp[3]);
                }
                else{
                    tmp[0] = Integer.toString(i);// index for matrix value of dataset in dicom/dicom.csv
                    tmp[1] = dataset_move[i];
                    tmp[2] = query[i];
                    tmp[3] = dataset_up[i];
                }
                return tmp;
            }
            else return createRandomQuery(KindOfRunning,Size_tpch);
        }
    }
    public static String[] sliceArray(String[] originArray, String object){
	String [] newArray = new String [originArray.length];
	for (int i = 0; i < newArray.length; i++){newArray[i] = "";}
        int j = 0;
	if (originArray.length > 1){
		for (int i = 0; i < originArray.length; i++){
			if (!object.equals(originArray[i])){
				newArray[j] = originArray[i];
				j++;
			}  
	     	}
	System.out.println(Arrays.toString(originArray));
	System.out.println(Arrays.toString(newArray));
	return newArray;
	}
	else return originArray;
    }

    public static String[] Checkquery (String query){
	String [] query1 = {"lineitem"};
        String [] query2 = {"part","supplier","partsupp","nation","region"};
        String [] query3 = {"customer","orders","lineitem"};
        String [] query4 = {"orders"};
        String [] query5 = {"customer","orders","lineitem","supplier","nation","region"};
        String [] query6 = {"lineitem"};
        String [] query7 = {"supplier","lineitem","orders","customer","nation"};
        String [] query8 = {"part","supplier","lineitem","orders","customer","nation","region"};
        String [] query9 = {"part","supplier","lineitem","partsupp","orders","nation"};
        String [] query10 = {"lineitem","orders","customer","nation"};
        String [] query11 = {"partsupp","supplier","nation"};
        String [] query12 = {"lineitem","orders"};
        String [] query13 = {"orders","customer"};
        String [] query14 = {"lineitem","part"};
        String [] query15 = {"lineitem","supplier"};
        String [] query16 = {"part","partsupp"};
        String [] query17 = {"lineitem","part"};
        String [] query18 = {"lineitem","orders","customer"};
        String [] query19 = {"lineitem","part"};
        String [] query20 = {"lineitem","part","partsupp","supplier","nation"};
        String [] query21 = {"lineitem","orders","supplier","nation"};
        String [] query22 = {"orders","customer"};
	switch (query){
                case "1": {return query1;}
                        //break;
                case "2": {return query2;}
                        //break;
                case "3": {return query3;}
                        //break;
                case "4": {return query4;}
                        //break;
                case "5": {return query5;}
                        //break;
                case "6": {return query6;}
                        //break;
                case "7": {return query7;}
                        //break;
                case "8": {return query8;}
                        //break;
                case "9": {return query9;}
                        //break;
                case "10": {return query10;}
                        //break;
                case "11": {return query11;}
                        //break;
                case "12": {return query12;}
                        //break;
                case "13": {return query13;}
                        //break;
                case "14": {return query14;}
                        //break;
                case "15": {return query15;}
                        //break;
                case "16": {return query16;}
                        //break;
                case "17": {return query17;}
                        //break;
                case "18": {return query18;}
                        //break;
                case "19": {return query19;}
                        //break;
                case "20": {return query20;}
                        //break;
                case "21": {return query21;}
                        //break;
                case "22": {return query22;}
			//break;
		default: return query22; 
			//break;
	}
	//return query1;
    }
    public static String dataset_move(String query){
	Random rand = new Random();
	String [] query1 = {"lineitem"};
	String [] query2 = {"part","supplier","partsupp","nation","region"};
	String [] query3 = {"customer","orders","lineitem"};
	String [] query4 = {"orders"};
	String [] query5 = {"customer","orders","lineitem","supplier","nation","region"};
        String [] query6 = {"lineitem"};
        String [] query7 = {"supplier","lineitem","orders","customer","nation"};
        String [] query8 = {"part","supplier","lineitem","orders","customer","nation","region"};
	String [] query9 = {"part","supplier","lineitem","partsupp","orders","nation"};
	String [] query10 = {"lineitem","orders","customer","nation"};
	String [] query11 = {"partsupp","supplier","nation"};
	String [] query12 = {"lineitem","orders"};
	String [] query13 = {"orders","customer"};
	String [] query14 = {"lineitem","part"};
	String [] query15 = {"lineitem","supplier"};
	String [] query16 = {"part","partsupp"};
	String [] query17 = {"lineitem","part"};
	String [] query18 = {"lineitem","orders","customer"};
	String [] query19 = {"lineitem","part"};
	String [] query20 = {"lineitem","part","partsupp","supplier","nation"};
	String [] query21 = {"lineitem","orders","supplier","nation"};
	String [] query22 = {"orders","customer"};
	String query_select;
	switch (query){
		case "1": {query_select = query1[rand.nextInt(query1.length)];}
			break;
		case "2": {query_select = query2[rand.nextInt(query2.length)];}
                        break;
		case "3": {query_select = query3[rand.nextInt(query3.length)];}
                        break;
                case "4": {query_select = query4[rand.nextInt(query4.length)];}
                        break;
		case "5": {query_select = query5[rand.nextInt(query5.length)];}
                        break;
                case "6": {query_select = query6[rand.nextInt(query6.length)];}
                        break;
                case "7": {query_select = query7[rand.nextInt(query7.length)];}
                        break;
                case "8": {query_select = query8[rand.nextInt(query8.length)];}
                        break;
		case "9": {query_select = query9[rand.nextInt(query9.length)];}
                        break;
                case "10": {query_select = query10[rand.nextInt(query10.length)];}
                        break;
                case "11": {query_select = query11[rand.nextInt(query11.length)];}
                        break;
                case "12": {query_select = query12[rand.nextInt(query12.length)];}
                        break;
                case "13": {query_select = query13[rand.nextInt(query13.length)];}
                        break;
                case "14": {query_select = query14[rand.nextInt(query14.length)];}
                        break;
                case "15": {query_select = query15[rand.nextInt(query15.length)];}
                        break;
                case "16": {query_select = query16[rand.nextInt(query16.length)];}
                        break;
		case "17": {query_select = query17[rand.nextInt(query17.length)];}
                        break;
                case "18": {query_select = query18[rand.nextInt(query18.length)];}
                        break;
                case "19": {query_select = query19[rand.nextInt(query19.length)];}
                        break;
                case "20": {query_select = query20[rand.nextInt(query20.length)];}
                        break;
                case "21": {query_select = query21[rand.nextInt(query21.length)];}
                        break;
                case "22": {query_select = query22[rand.nextInt(query22.length)];}
                        break;
		default : {query_select = query1[0];}
                        break;
	}
	return query_select;
    }
    public static double sizeDataset10000m(String dataset, String Size_tpch){
    	double size = 0;
 	switch (dataset) {
            case "nation":   
                {
                size = 0.0022;
                }
                break;
            case "region":
                {
                size = 0.000389;
                }
                break;
            case "part":
                {
                size = 232.1;
                }
                break;
            case "supplier":
                {
                size = 13.5;
                }
                break;
            case "partsupp":
                {
                size = 113.5;
                }
                break;
            case "customer":
                {
                size = 233.5;
                }
	        break; 
            case "orders":
                {
                size = 1600;
                }
                break;
            case "lineitem":
                {
                size = 7200;
                }
                break; 
            default:
                size = 7200;
                break;    
            }
	return size;
    }
    public static double sizeDataset2000m(String dataset, String Size_tpch){
        double size = 0;
        switch (dataset) {
            case "nation":
                {
                size = 0.0022;
                }
                break;
            case "region":
                {
                size = 0.000389;
                }
                break;
            case "part":
                {
                size = 46.1;
                }
                break;
            case "supplier":
                {
                size = 2.7;
                }
                break;
            case "partsupp":
                {
                size = 228.1;
                }
                break;
            case "customer":
                {
                size = 46.5;
                }
                break;
	    case "orders":
                {
                size = 329.7;
                }
                break;
            case "lineitem":
                {
                size = 1400;
                }
                break;
            default:
                size = 1400;
                break;
        }
        return size;
    }
    public static double sizeDataset1000m(String dataset, String Size_tpch){
        double size = 0;
 	switch (dataset) {
            case "nation":
                {
                size = 0.0022;
                }
                break;
            case "region":
                {
                size = 0.000389;
                }
                break;
            case "part":
                {
                size = 23;
                }
                break;
            case "supplier":
                {
                size = 1.3;
                }
                break;
            case "partsupp":
                {
                size = 113.5;
                }
                break;
            case "customer":
                {
                size = 23.2;
                }
                break; 
            case "orders":
                {
                size = 164;
                }
                break;
            case "lineitem":
                {
                size = 724.7;
                }
                break; 
            default:
                size = 724.7;
		break;
	}
        return size;
    }
    public static double sizeDataset100m(String dataset, String Size_tpch){
        double size = 0;
	switch (dataset) {
                case "nation":
                    {
                    size = 0.0022;
                    }
                    break;
                case "region":
                    {
                    size = 0.000389;
                    }
                    break;
                case "part":
                    {
                    size = 2.3;
                    }
                    break;
                case "supplier":
                    {
                    size = 0.1364;
		    }
                    break;
                case "partsupp":
                    {
                    size = 11;
                    }
                    break;
                case "customer":
                    {
                    size = 2.32;
                    }
                    break; 
                case "orders":
                    {
                    size = 16.1;
                    }
                    break;
                case "lineitem":
                    {
                    size = 70.8;
                    }
                    break; 
                default:
                    size = 70.8;
		    break;
	}
        return size;
    }
    public static double[][] sizeDatasetDicom(String dataset, String Size_tpch) throws IOException {
        String matrix = new App().readhome("dicom");
        double [][] size = ReadMatrixCSV.readMatrix(matrix, CsvFileReader.count(matrix));
        return size;
    }
    public static double sizeDataset10m(String dataset, String Size_tpch){
        double size = 0;
        switch (dataset) {
                case "nation":
                    {
                    size = 0.0022;
                    }
                    break;
                case "region":
                    {
                    size = 0.000389;
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
                    size = 8.3;
                    break;    
                    }
        return size;
    }

    public static double sizeDataset(String dataset, String Size_tpch){
        double size = 0;
	if (Size_tpch.contains("10000m")){
		size = sizeDataset10000m(dataset, Size_tpch);
	}
	else{	if (Size_tpch.contains("1000m")){
			size = sizeDataset1000m(dataset, Size_tpch);
            	}
            	else{	if (Size_tpch.contains("100m")) {
				size = sizeDataset100m(dataset, Size_tpch);
			}
	   		else{	if (Size_tpch.contains("10m")) {
					size = sizeDataset10m(dataset, Size_tpch);
				}
				else{	if (Size_tpch.contains("2000m")) {
                                        size = sizeDataset2000m(dataset, Size_tpch);
                                	}
                                	else size  = sizeDataset1000m(dataset, Size_tpch);
				}
			}
		}
        }
        return size;
    } 
    public static double pageDataset10000m(String dataset, String Size_tpch){
    	double page = 0;
	switch (dataset) {
            case "nation":
                {
                page = 240.1;//no need
                }
                break;
            case "region":
                {
                page = 240.1;// no need
                }
                break;
            case "part":
                {
                page = 40962;
                }
                break;
            case "supplier":
                {
                page = 2215;// not yet
                }
                break;
            case "partsupp":
	        {
                page = 174510;// not yet
                }
                break;
            case "customer":
                {
                page = 35827;
                }   
                break; 
            case "orders":
                {
                page = 260912;
                }
                break;
            case "lineitem":
                {
                page = 1124542;
                }
                break; 
            default:
                page = 1124542;
                break;    
            }
	return page;
    }
public static double pageDataset2000m(String dataset, String Size_tpch){
        double page = 0;
        switch (dataset) {
            case "nation":
                {
                page = 240.1;//no need
                }
                break;
            case "region":
                {
                page = 240.1;//no need
                }
                break;
            case "part":
                {
                page = 8193;
                }
                break;
            case "supplier":
                {
                page = 443;
                }
                break;
            case "partsupp":
                {
                page = 34886;
                }
                break;
            case "customer":
                {
                page = 7169;
                }
                break;
            case "orders":
                {
                page = 52187;
                }
                break;
            case "lineitem":
                {
                page = 224920;
                }
                break;
            default:
                page = 224920;
                break;
        }
	return page;
    }
    public static double pageDataset1000m(String dataset, String Size_tpch){
        double page = 0;
        switch (dataset) {
            case "nation":
                {
                page = 240.1;//no need
                }
                break;
            case "region":
                {
                page = 240.1;//no need
                }
                break;
            case "part":
                {
                page = 4097;
                }
                break;
            case "supplier":
                {
                page = 222;
                }
		break;
            case "partsupp":
                {
                page = 17451;
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
                page = 112503;
		break;
	}                
        return page;
    }
    public static double pageDataset100m(String dataset, String Size_tpch){
        double page = 0;
        switch (dataset) {
                case "nation":
                    {
                    page = 240.1;//no need
                    }
                    break;
                case "region":
                    {
                    page = 240.1;//no need
                    }
                    break;
                case "part":
                    {
                    page = 410;
                    }
                    break;
                case "supplier":
                    {
                    page = 23;
                    }
                    break;
                case "partsupp":
                    {
                    page = 1744;
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
                    page = 11259;
                    break;    
        } 
        return page;
    }
    public static double pageDataset10m(String dataset, String Size_tpch){
        double page = 0;
        switch (dataset) {
                case "nation":
                    {
                    page = 2.4;// no need
                    }
                    break;
                case "region":
                    {
                    page = 2.4;// no need
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
                    page = 1126;
                    break;    
        }                
        return page;
    }

    public static double pageDataset(String dataset, String Size_tpch){
        double page = 0;
	if (Size_tpch.contains("10000m")){
		page = pageDataset10000m(dataset, Size_tpch);
        }
	else{	if (Size_tpch.contains("1000m")){
			page = pageDataset1000m(dataset, Size_tpch);
            	}
            	else{	if (Size_tpch.contains("100m")) {
				page = pageDataset100m(dataset, Size_tpch);
			}
            		else{	if (Size_tpch.contains("10m")) {
					page = pageDataset10m(dataset, Size_tpch);
				}
				else{   if (Size_tpch.contains("2000m")) {
                                        page = pageDataset2000m(dataset, Size_tpch);
                                	}
                                	else page  = pageDataset1000m(dataset, Size_tpch);
                        	}
			}
		}
        }
        return page;
    }
    public static double tupleDataset10000m(String dataset, String size_tpch){
	double tuple = 0;
	switch (dataset) {
            case "nation":
                {
                tuple = 25;
                }
                break;
            case "region":
                {
                tuple = 5;
                }
                break;
            case "part":
                {
                tuple = 2000000;
                }
                break;
            case "supplier":
                {
                tuple = 100000;
                }
                break;
            case "partsupp":
                {
                tuple = 8000000;
                }
                break;
            case "customer":
                {
                tuple = 1500000;
                }
                break;    
            case "orders":
                {
                tuple = 15000000;
                }
                break;
            case "lineitem":
                {
                tuple =59986052;
                }
                break; 
            default:
                tuple = 59986052;
		break;
	   }
	return tuple;
    }
    public static double tupleDataset2000m(String dataset, String Size_tpch){
        double tuple = 0;
        switch (dataset) {
            case "nation":
                {
                tuple = 25;
                }
                break;
            case "region":
                {
                tuple = 5;
                }
                break;
            case "part":
                {
                tuple = 400000;
                }
                break;
            case "supplier":
                {
                tuple = 20000;
                }
                break;
            case "partsupp":
                {
                tuple = 1600000;
                }
                break;
            case "customer":
                {
                tuple = 300000;
                }
                break;
            case "orders":
                {
                tuple = 3000000;
                }
                break;
            case "lineitem":
                {
                tuple = 11998600;
                }
                break;
            default:
		tuple = 11998600;
	}
	return tuple;
    }
    public static double tupleDataset1000m(String dataset, String Size_tpch){
	double tuple = 0;
	switch (dataset) {
            case "nation":
                {
                tuple = 25;
                }
                break;
            case "region":
                {
                tuple = 5;
                }
                break;
            case "part":
                {
                tuple = 200000;
                }
                break;
            case "supplier":
                {
                tuple = 10000;
                }
                break;
            case "partsupp":
                {
                tuple = 800000;
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
                tuple = 6001215;
                break;    
        } 
	return tuple;

    }
    public static double tupleDataset100m(String dataset, String size_tpch){
	double tuple = 0;
	switch (dataset) {
                case "nation":
                    {
                    tuple = 25;
                    }
                    break;
                case "region":
                    {
                    tuple = 5;
                    }
                    break;
                case "part":
                    {
                    tuple = 20000;
                    }
                    break;
                case "supplier":
                    {
                    tuple = 1000;
                    }
                    break;
                case "partsupp":
                    {
                    tuple = 80000;
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
                    tuple = 600572;
                    break;      
                    }
		return tuple;
    }
    public static double tupleDataset10m(String dataset, String size_tpch){
	double tuple = 0;
	switch (dataset) {
                case "nation":
                    {
                    tuple = 25;
                    }
                    break;
                case "region":
                    {
                    tuple = 5;
                    }
                    break;
                case "part":
                    {
                    tuple = 2000;
                    }
                    break;
                case "supplier":
                    {
                    tuple = 0.28;// not yet
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
                    tuple = 60057;
                    break;    
                    }
	return tuple;
    }
    public static double tupleDataset(String dataset, String Size_tpch){
        double tuple = 0;
	if (Size_tpch.contains("10000m")){
		tuple  = tupleDataset10000m(dataset, Size_tpch);
	}
	else{	if (Size_tpch.contains("1000m")){
        		tuple  = tupleDataset1000m(dataset, Size_tpch);
	        } 
                else{	if (Size_tpch.contains("100m")) {
                		tuple  = tupleDataset100m(dataset, Size_tpch);
                        }
                        else{	if (Size_tpch.contains("10m")) {
                			tuple  = tupleDataset10m(dataset, Size_tpch); 
				}
				else{	if (Size_tpch.contains("2000m")) {
                                        	tuple  = tupleDataset2000m(dataset, Size_tpch);
                                	}
                                	else tuple  = tupleDataset1000m(dataset, Size_tpch);
				}
        		}//else of 100m
		}// else of 1000m
	}//else of 10000m
        return tuple;
    }

}
