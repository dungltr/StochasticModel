package Algorithms;

import java.io.IOException;
import weka.core.converters.*;
import weka.core.Instances;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author letrung
 */
public class LinearRegression {
    public static void test () throws IOException{
    CSVLoader loader = new CSVLoader();
loader.setFile(new java.io.File("/Users/mhall/Documents/Pentaho/demo/iris.csv"));

Instances insts = loader.getDataSet();

System.out.println(insts.toString());
}
    
}
