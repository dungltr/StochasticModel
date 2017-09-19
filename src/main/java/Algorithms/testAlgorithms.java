/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

import static Algorithms.Algorithms.estimateCostValue;
import static Algorithms.Algorithms.estimateSizeOfMatrix;
import LibraryIres.Move_Data;
import java.io.IOException;
import java.util.Random;

/**
 *
 * @author letrung
 */
public class testAlgorithms {
    //    @Test
    public void testCost() throws IOException
    {   int M = 3;
        String realValue = System.getProperty("user.home")+"/realValue.csv";
        String Parameter = System.getProperty("user.home")+"/Parameter.csv";
//        Algorithms.estimateCost(M, realValue, Parameter);
    }
    //    @Test
    public void test() throws IOException{
        String delay_ys = "";
	double[] tmp = {1,0.512,2,0};
        int Max = 20;
        int numerOfVariable = 3;
        String directory ="/usr/local/Cellar/ires/asap-platform/asap-server/target/asapLibrary/operators/Move_HIVE_POSTGRES";
        double R_2_limit = 0.5;
        String realValue = directory + "/realValue.csv";
        String parameter = directory + "/Parameter.csv";
        String estimate = directory + "/Estimate.csv";
        int sizeOfValue = estimateSizeOfMatrix(Max, numerOfVariable, directory, R_2_limit,delay_ys,"");
        double Time = estimateCostValue(sizeOfValue, realValue, parameter, tmp, R_2_limit);
    }
    //    @Test 
    public void testAlgorithm() throws Exception {
        String Operator = "Move";
        String DataIn = "customer"; 
        String DatabaseIn = "mydb"; 
        String DataInSize = "100";
        String Schema = "(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        
        String DataOut = "customer_hive"; 
        String DatabaseOut = "mydb";
        
        String From = "HIVE";
        String To   = "POSTGRES";
        String SQL = "";
        
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
        Data.set_DataIn(DataIn);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(From);
        Data.set_To(To);
        Data.set_Schema(Schema);
        Random rand = new Random();
        double [] size = {rand.nextInt(5),rand.nextInt(5),rand.nextInt(5),rand.nextInt(5),rand.nextInt(5)};
        double [] ram = {512,1024,512,1024,2048};
        double [] core = {2,1,2,1,2};
        double [] time = {0,0,0,0,0};
        
        double Data_size;// = rand.nextInt(5);
        double Ram = (rand.nextInt(3)+1)*512;
        double Core = rand.nextInt(2)+1;
        double Time = rand.nextInt(3)+1;
        double[] tmp = new double[4];
        
        int i = rand.nextInt(5);

        Data.set_DataIn(DataIn+i);
        Data.set_DataOut(DataOut+i);    
        Data_size = size[i];
//        Ram = ram[i];
//        Core = core[i];
//        Time = time[i];
        tmp[0] = Data_size;
        tmp[1] = Ram;
        tmp[2] = Core;
        tmp[3] = Time;
//        tmp[] = {Data_size, Ram, Core, Time};
//        Algorithms.main(Data, SQL, tmp);
        Thread.sleep(500);
//        }
    }
    
}
