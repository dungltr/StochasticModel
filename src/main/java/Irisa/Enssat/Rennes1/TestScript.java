/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;
import Algorithms.LinearRegressionManual;
import IRES.TPCHQuery;
import Standalone.TPCHStandalone;
import com.sparkexample.App;
import java.io.IOException;
import java.io.Console;
import java.util.Scanner;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
/**
 *
 * @author letrung
 */
public class TestScript {
    int iExitValue;
    String sCommandString;

    public void runScript(String command){
        sCommandString = command;
        CommandLine oCmdLine = CommandLine.parse(sCommandString);
        DefaultExecutor oDefaultExecutor = new DefaultExecutor();
        oDefaultExecutor.setExitValue(0);
        try {
            iExitValue = oDefaultExecutor.execute(oCmdLine);
        } catch (ExecuteException e) {
            System.err.println("Execution failed.");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("permission denied.");
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception{
        createProperties();
        testall();
//        TestScript testScript = new TestScript();
//        testScript.runScript("sh /Users/letrung/testScript.sh");
    }
            //String SQL_fileName = SQL_folder + "tpch_" + query; 
    public static void createProperties(){
        new App().createasapproperties1();
        new App().createasapproperties2();
        new App().createReporter_config();
    }
    public static String [] Ask(){
        Console console = System.console();
        String[] tmp = new String [7];
        tmp[0] = KindCheck(console.readLine("Kind of operator (a-IRES; b-StandAlone): "));
        tmp[1] = DbCheck(console.readLine("Enter database of operator(default = tpch):"));   
        tmp[2] = SizeCheck(console.readLine("Enter database size of operator(default = 100m):"));
        tmp[3] = FromCheck(console.readLine("Enter First table engine of operator(Hive,Postgres,Spark):"));
        tmp[4] = ToCheck(console.readLine("Enter Second table of operator(Hive,Postgres,Spark):"));
        tmp[5] = MoreCheck(console.readLine("Do you want to add more (yes/no): "));
        tmp[6] = MoreCheck(console.readLine("Do you want to use delay time (yes/no): "));
	return tmp;
    }
    public static void testall() throws Exception{
        double TimeOfDay = 24.00*Math.random();
        Scanner in = new Scanner(System.in);
	Console console = System.console();
	String KindOfRunning = KindRunning(console.readLine("Kind of running (a-Training; b-Testing; c-Predict): ")); 
        System.out.printf("How many operators you want to process(default = 1):  ");
        int k = 1;
        try
        {
            k = in.nextInt();
        }
        catch (java.util.InputMismatchException e)
        {
            System.out.println("Invalid Input");
            return;
        }

        String[][] call = new String [k][7];
        String more = "yes";
        int count = 0;
        while ((more=="yes")&&(count<k))
        {
            call[count] = Ask();
            more = call[count][5];
            count++;
        }                
        for (int i = 0; i< count; i++)
        {
            System.out.println("\n Kind of operator "+i+": "+call[i][0]);
            System.out.println("\n Database of operator "+i+": "+call[i][1]);
            System.out.println("\n Size of database with operator "+i+": "+call[i][2]);
            System.out.println("\n First Table of operator "+i+": "+call[i][3]);
            System.out.println("\n Second Table of operator "+i+": "+call[i][4]);
            System.out.println("\n Using delay time of operator "+i+": "+call[i][6]);
	}              
        System.out.printf("Enter the number of loop:  ");
        int times = 1;
        try
        {
            times = in.nextInt();
        }
        catch (java.util.InputMismatchException e)
        {
            System.out.println("Invalid Input");
            return;
        }
	double TimeOfDay_tmp;
        for (int i = 0; i < times; i++)
        { 
            System.out.println("\n This is the "+i+"th round-------------------------------------------------------------------------------------------------------");
            TimeOfDay = 24.00*Math.random();
	    for (int j =0; j < count; j++)
            {
                //TPCHQuery.TPCH_Hive_Postgres(TimeOfDay,"", "");
		if (call[j][6].equals("no")) TimeOfDay_tmp = TimeOfDay/24;
		else TimeOfDay_tmp = TimeOfDay;
                TPCH(TimeOfDay_tmp,call[j], KindOfRunning);
            }
                
        }       
    }
    public static void TPCH(double TimeOfDay, String[] call, String KindOfRunning) throws Exception{
        if ((KindOfRunning=="training")||(KindOfRunning=="testing")){
            if (call[0]=="IRES")
                TPCHQuery.TPCH(TimeOfDay, call[1], call[2], call[3], call[4], KindOfRunning);
            else TPCHStandalone.TPCH_Standalone(TimeOfDay, call[1], call[2], call[3], call[4], KindOfRunning);
        }
        else {
            if (KindOfRunning=="predict"){
                LinearRegressionManual.TPCH(TimeOfDay, call[1], call[2], call[3], call[4], KindOfRunning);
            }
        }
        
    }
    public static String KindCheck(String Kind){
        if (Kind.toLowerCase().contains("a")) return "IRES";
        else if (Kind.toLowerCase().contains("b")) return "Standalone";
            else
                return "IRES";
    }
    public static String KindRunning(String KindOfRunning){
        if (KindOfRunning.toLowerCase().contains("a")) return "training";
        else if (KindOfRunning.toLowerCase().contains("b")) return "testing";
            else if (KindOfRunning.toLowerCase().contains("c")) return "predict";
                else return "training";
    }
    public static String DbCheck(String DB){
        if (DB.toLowerCase().contains("tpch")) return "tpch";
        else return "tpch";
    }
    public static String SizeCheck(String Size){
	if (Size.toLowerCase().contains("10000m")) return "10000m";
	else {
        	if (Size.toLowerCase().contains("1000m")) return "1000m";
        	else {
            		if (Size.toLowerCase().contains("10m")) return "10m";
            		else {
                        	if (Size.toLowerCase().contains("2000m")) return "2000m";
			}
			return "100m";
        	}     
	}   
    }
    public static String FromCheck(String From){
        if ((From.toLowerCase().contains("hive"))||(From.toLowerCase().equals("h"))) return "Hive";
        else {
            if ((From.toLowerCase().contains("postgres"))||(From.toLowerCase().equals("p"))) return "Postgres";
            else return "Hive";
        }
    }
    public static String ToCheck(String To){
        if ((To.toLowerCase().contains("hive"))||(To.toLowerCase().equals("h"))) return "Hive";
        else {
            if ((To.toLowerCase().contains("postgres"))||(To.toLowerCase().equals("p"))) return "Postgres";
            else return "Postgres";
        }
    }
    public static String MoreCheck(String Kind){
        if ((Kind.toLowerCase().contains("yes"))||(Kind.toLowerCase().equals("y"))) return "yes";
        else if ((Kind.toLowerCase().contains("no"))||(Kind.toLowerCase().equals("n"))) return "no";
            else
                return "yes";
    }
    
}
