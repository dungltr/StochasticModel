/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import LibraryIres.*;
import com.sparkexample.App;
import java.net.UnknownHostException;

/**
 *
 * @author letrung
 */
public class LuaScript {
    String NODE_PC = new App().readhome("NODE_PC");
    public String LuaScript(Move_Data Data) throws UnknownHostException {
        String node_pc = NODE_PC;//new App().getComputerName();
        String operator = Data.get_Operator();
        if (Data.get_From()==Data.get_To()) operator = "SQL";
        String convertCSV2Parquet = "		[ \"convertCSV2Parquet.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertCSV2Parquet.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		}\n";
        String convertParquet2CSV ="		[ \"convertParquet2CSV.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertParquet2CSV.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		}\n";
        String SQL = "              [ \""+operator+"_"+Data.get_From()+"_"+"\" .. ENGINE .. \".sql\"] = {\n" +
"					file = OPERATOR_HOME .. \"/"+operator+"_"+Data.get_From()+"_"+"\" .. ENGINE .. \".sql\",\n" +
"      				type = \"file\",               -- other value: 'archive'\n" +
"      				visibility = \"application\",  -- other values: 'private', 'public'\n" +
"    			}";
        String convert = "";
        
        switch (Data.get_From()) {
            case "HIVE": case "Hive": case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "Postgres")|| (Data.get_To() == "postgres"))
                    convert = "\n";
                else if ((Data.get_To() == "SPARK") || (Data.get_To() == "Spark")|| (Data.get_To() == "spark"))
                    convert = ",\n"+convertCSV2Parquet;
                }
                break;    
            case "POSTGRES": case "Postgres": case "postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "Hive")|| (Data.get_To() == "hive"))
                    convert = "\n";
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "Spark")||(Data.get_To() == "spark"))
                    convert = ",\n"+convertCSV2Parquet;
                else if ((Data.get_To() == "POSTGRES")||(Data.get_To() == "Postgres")||(Data.get_To() == "postgres"))
                    {
                        convert = "\n";
//                        convert = ",\n"+SQL;
//                        operator = "SQL";
                    }    
                }
                break;   
            case "SPARK": case "Spark": case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "Postgres")|| (Data.get_To() == "postgres"))
                    convert = "\n";
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "Hive")||(Data.get_To() == "hive"))
                    convert = ",\n"+convertParquet2CSV;
                }
                break;   
            default:
                
                break;
        }
        String lua = "-- Specific configuration of operator\n" +
"ENGINE = \""+Data.get_To()+"\"\n" +
//"OPERATOR = \"Move_HIVE_POSTGRES\"\n" + 
"OPERATOR = \""+operator+"_"+Data.get_From()+"_\" .. ENGINE\n" +
"SCRIPT = OPERATOR .. \".sh\"\n" +
"SHELL_COMMAND = \"./\" .. SCRIPT\n" +
"-- Home directory of operator\n" +
"OPERATOR_LIBRARY = \"asapLibrary/operators\"\n" +
"OPERATOR_HOME = OPERATOR_LIBRARY .. \"/\" .. OPERATOR\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"	name = \"Execute \" .. OPERATOR .. \" Operator\",\n" +
"	labels = \""+Data.get_DataIn()+"-"+Data.get_From()+"-"+Data.get_DataOut()+"-"+Data.get_To()+"\",\n" +
"	nodes = \""+node_pc+"\",\n" +
"       memory = 512,\n" +                
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
"			}" + convert +
"		}\n" + 
"       }\n" +
"\n" +
"}";
        return lua;
    } 
    public String LuaScript2(Move_Data Data, YarnValue yarn) throws UnknownHostException {
        String node_pc = NODE_PC;//new App().getComputerName();
        String operator = Data.get_Operator();
//        if (Data.get_From()==Data.get_To()) operator = "SQL";
        String convertCSV2Parquet = "		[ \"convertCSV2Parquet.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertCSV2Parquet.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		}\n";
        String convertParquet2CSV ="		[ \"convertParquet2CSV.py\"] = {\n" +
"			file = OPERATOR_HOME .. \"/convertParquet2CSV.py\",\n" +
"			type = \"file\",               -- other value: 'archive'\n" +
"			visibility = \"application\",  -- other values: 'private', 'public'\n" +
"		}\n";
        String SQL = "              [ \""+operator+"_"+Data.get_From()+"_"+"\" .. ENGINE .. \".sql\"] = {\n" +
"					file = OPERATOR_HOME .. \"/"+operator+"_"+Data.get_From()+"_"+"\" .. ENGINE .. \".sql\",\n" +
"      				type = \"file\",               -- other value: 'archive'\n" +
"      				visibility = \"application\",  -- other values: 'private', 'public'\n" +
"    			}";
        String convert = "";
        
        switch (Data.get_From()) {
            case "HIVE": case "hive": case "Hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres")|| (Data.get_To() == "Postgres"))
                    convert = "\n";
                else if ((Data.get_To() == "SPARK") || (Data.get_To() == "spark")|| (Data.get_To() == "Spark"))
                    convert = ",\n"+convertCSV2Parquet;
                }
                break;    
            case "POSTGRES": case "postgres": case "Postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive")| (Data.get_To() == "Hive"))
                    convert = "\n";
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark")|| (Data.get_To() == "Spark"))
                    convert = ",\n"+convertCSV2Parquet;
                else if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres")|| (Data.get_To() == "Postgres"))
                    {
//                        convert = "\n";
                        convert = ",\n"+SQL;
//                        operator = "SQL";
                    }    
                }
                break;   
            case "SPARK": case "spark": case "Spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres")|| (Data.get_To() == "Postgres"))
                    convert = "\n";
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive")|| (Data.get_To() == "Hive"))
                    convert = ",\n"+convertParquet2CSV;
                }
                break;   
            default:
                
                break;
        }
        String lua = "-- Specific configuration of operator\n" +
"ENGINE = \""+Data.get_To()+"\"\n" +
//"OPERATOR = \"Move_HIVE_POSTGRES\"\n" + 
"OPERATOR = \""+operator+"_"+Data.get_From()+"_\" .. ENGINE\n" +
"SCRIPT = OPERATOR .. \".sh\"\n" +
"SHELL_COMMAND = \"./\" .. SCRIPT\n" +
"-- Home directory of operator\n" +
"OPERATOR_LIBRARY = \"asapLibrary/operators\"\n" +
"OPERATOR_HOME = OPERATOR_LIBRARY .. \"/\" .. OPERATOR\n" +
"\n" +
"-- The actual distributed shell job.\n" +
"operator = yarn {\n" +
"	name = \"Execute \" .. OPERATOR .. \" Operator\",\n" +
"	labels = \""+Data.get_DataIn()+"-"+Data.get_From()+"-"+Data.get_DataOut()+"-"+Data.get_To()+"\",\n" +
"	nodes = \""+node_pc+"\",\n" +
"       memory = "+yarn.get_Ram()+",\n" +                
"	container = { \n" +
"		instances = "+yarn.get_Core()+",\n" +
"		command = { \n" +
"			base = SHELL_COMMAND\n" +
"		},\n" +
"		resources = { \n" +
"			[ SCRIPT] = { \n" +
"				file = OPERATOR_HOME .. \"/\" .. SCRIPT,\n" +
"				type = \"file\",               -- other value: 'archive'\n" +
"				visibility = \"application\",  -- other values: 'private', 'public'\n" +
"			}" + convert +
"		}\n" + 
"       }\n" +
"\n" +
"}";
        return lua;
    }
    
}
