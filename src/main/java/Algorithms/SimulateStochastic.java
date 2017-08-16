/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

/**
 *
 * @author letrung
 */
public class SimulateStochastic {
    public static long TimeWaiting (double numberUser, double timeOfDay) throws InterruptedException {

    double NumberOfUser = numberUser*Math.random();    
    if ((0 <= timeOfDay)&& (timeOfDay<= 8)) return(1 + (int)NumberOfUser);
    else return(10000 + (int)NumberOfUser);   
    }
    public static double waiting (double numberUser, double timeOfDay) throws InterruptedException {
    //  0 < timeOfDay < 24  
    //  0 < numberofUser < 10000 
    long start = System.currentTimeMillis();
    double NumberOfUser = numberUser*Math.random();
    if ((0 <= timeOfDay)&& (timeOfDay<= 8)) Thread.sleep(10 + (int)NumberOfUser);
    else Thread.sleep(10000 + (int)NumberOfUser);   
    long stop = System.currentTimeMillis();
    return (double)(stop-start)/1000.0;// -12.0;
    }
    
}
