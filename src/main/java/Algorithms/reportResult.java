/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

import static Algorithms.ReadMatrixCSV.readMatrix;
import WriteReadData.CsvFileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 *
 * @author letrung
 */
public class reportResult {
        private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
    public static void report(int M, String fileValue, String estimate, String fileError) throws IOException {
       int Max_fileValue = CsvFileReader.count(fileValue) - 1; // skip header file
       int Max_fileEstimate = CsvFileReader.count(estimate) - 1; // skip header file
       int Max_fileError = CsvFileReader.count(fileError) - 1; // skip header file
       double[][] realValue;
       double[][] estimateValue;
       double[][] errorValue;
       
       int numberOfRealValue = M;
       int numberOfEstimate = M;
       int numberOfError    = M;
       
       if (M < Max_fileValue) realValue = readMatrix(fileValue, M);
       else {
           realValue = readMatrix(fileValue, Max_fileValue);
           numberOfRealValue = Max_fileValue;
       }
       
       if (M < Max_fileEstimate) estimateValue = readMatrix(estimate, M);
       else {
           estimateValue = readMatrix(estimate, Max_fileEstimate);
           numberOfEstimate = Max_fileEstimate;
           
       }
       if (M < Max_fileError) errorValue = readMatrix(fileError, M);
       else {
           errorValue = readMatrix(fileError, Max_fileError);
           numberOfError = Max_fileError;
           
       }
       double averageTimeResponse = average(numberOfRealValue,realValue);
       System.out.println("\nAverage value of Response Time is: "+ averageTimeResponse + " in " + numberOfRealValue + " Sample");
       double averageTimeEstimate = average(numberOfEstimate,estimateValue);
       System.out.println("\nAverage value of Estimate Time is: "+averageTimeEstimate + " in " + numberOfEstimate + " Sample");
       double averageError = average(numberOfError,errorValue);
       System.out.println("\nAverage value of Error Time is: "+averageError + " in " + numberOfError + " Sample");
       
    }
    public static void reportError(String errorFile, double[] tmp, double costEstimateValue) throws IOException{
        String ErrorFile = errorFile;
        Path filePath = Paths.get(ErrorFile);
        double[][] error = new double[1][tmp.length+2];
        for (int i = 0; i < tmp.length; i++) error[0][i] = tmp[i];
        error[0][tmp.length] = costEstimateValue;
        error[0][tmp.length + 1] = tmp[tmp.length-1] - costEstimateValue;
//        if (!Files.exists(filePathError)) {
//            Files.createFile(filePathError);
//            }
//        Writematrix2CSV.addArray2Csv(ErrorFile, error);
        
        
        if (!Files.exists(filePath)) {
                Files.createFile(filePath);                
                String FILE_HEADER = "";
                for (int i= 0; i < tmp.length-1; i++)
                FILE_HEADER = FILE_HEADER + "Variable["+i+"]" + COMMA_DELIMITER;
                FILE_HEADER = FILE_HEADER + "CostValue"+ COMMA_DELIMITER + "EstimateValue"+ COMMA_DELIMITER +"ErrorValue";
                Writematrix2CSV.Writematrix2CSV(error, ErrorFile, FILE_HEADER);
                }
            else {
                String add = "";
                int i = 0;
            for (i = 0; error[0].length - 1 > i; i++)
            add = add + error[0][i] + COMMA_DELIMITER;
            if (error[0].length - 1 == i)
            add = add + error[0][i] + NEW_LINE_SEPARATOR;
            Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
            }
    }    
    public static double average(int numberOfValue, double[][] matrix){
        double tmp = 0;
        for (int i = 0; i < numberOfValue; i++) 
            tmp = tmp + matrix[i][matrix[0].length-1];
        tmp = tmp / numberOfValue;
        return tmp;           
    }
}
