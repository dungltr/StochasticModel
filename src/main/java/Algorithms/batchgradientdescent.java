/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

import static Algorithms.Algorithms.estimateCurrentCostValue;
import static Algorithms.Algorithms.estimateErrorSquare;
import static Algorithms.Algorithms.initParameterValue;
import static Algorithms.Algorithms.lookingOtherParameter;
import static Algorithms.Algorithms.roundMaxtrix;
import static Algorithms.Algorithms.setupMatrixA;
import static Algorithms.Algorithms.setupMatrixC;
import static Algorithms.Algorithms.setupMatrixX;
import static Algorithms.Algorithms.setupParameterB;
import static Algorithms.Algorithms.setupParameterBindex;
import static Algorithms.Algorithms.updateParameter;
import static Algorithms.ReadMatrixCSV.readMatrix;
import static Algorithms.testScilab.invert;
import static Algorithms.testScilab.multiply;
import static Algorithms.testScilab.transpose;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import static org.apache.commons.math.util.MathUtils.round;

/**
 *
 * @author letrung
 */
public class batchgradientdescent {
    private static final String COMMA_DELIMITER = ",";
    public static double estimateGradient(int sizeOfValue, String fileValue, String fileParameter, double[] X, double R_2_limit) throws IOException{
        int M = sizeOfValue;
        int N = 1;
        Path filePathParameter = Paths.get(fileParameter);
//        double[][] initParameter = {{1,1,1,1,1},{1,1,1,1,1}};
/*        double[][] initParameter = initParameterValue(X);
        if (!Files.exists(filePathParameter)) {
            Files.createFile(filePathParameter);
            System.out.println("\nNeed to update Parameter Value");
            int n = initParameter[0].length;
            int i = 0;
            String tmp = "";
            for (i = 0; n -1 >i; i++)
            tmp = tmp + "b[" + i + "]" + COMMA_DELIMITER;
            if (n - 1 == i) tmp = tmp + "b[" + i + "]";
            String FILE_HEADER = tmp;
            Writematrix2CSV.Writematrix2CSV(initParameter, fileParameter, FILE_HEADER);
            }
*/        double[][] realValue = readMatrix(fileValue, M);        
        double[][] Parameter = readMatrix(fileParameter, M);
        double[][] x = new double[realValue.length][realValue[0].length - 1];
        double[][] a = new double[realValue.length][realValue[0].length]; 
        double[] b = new double[Parameter[0].length];
        double[] c = new double[realValue.length];
        double[] d = new double[realValue.length];
        double[] err = new double[realValue.length];
       
        System.out.println("\nMatrix Real Value");
        x = setupMatrixX(realValue); 
        testScilab.printMatrix(x);
        System.out.println("\nMatrix Real Cost Fucntion");
        c = setupMatrixC(realValue);
        testScilab.printArray(c);
        System.out.println("\nMatrix A");
        a = setupMatrixA(x);
        testScilab.printMatrix(a);
        double[] B = new double[Parameter[0].length];       
        System.out.println("\nThe Old Paramater");
        B = setupParameterB(Parameter);
	testScilab.printArray(B);
////////////////////////////////        
//        double[] new_B = new double[Parameter[0].length];
        double[] loss = new double[Parameter[0].length];
        loss = resetArray(loss);
//        new_B = resetArray(new_B);
        double learningRate = 0.001;
        double eps = 0.01;
        int i = 0;
        int j = 0;
        double Error = 0;
        double J = 0;
        int epoch = M;
	double gradient = 0;
        while (i < epoch)
        {
                d = funcionValue(d,a,B);
		System.out.println("\n matrix of estimate value:");
		testScilab.printArray(d);
                Error = ErrorSquare(d,a,B,c);
		System.out.println("\n Value of Error:"+ Error);	
               // for (i = 0; i < d.length; i++) d[i] = d[i] - c[i];
		
		for (int k = 0; k < loss.length; k++)
                    for (j = 0; j < M; j++){
		//	System.out.println("\n B["+k+"]:=" + B[k] + "+" + learningRate + "*(" + d[j] + "-" + c[j] + ")*" + a[j][k] + "/" + M);	
                        B[k] = B[k] - learningRate*(d[j] - c[j])*a[j][k]/M;
		//	System.out.println("\n B["+k+"]:=" + B[k]);
                    }                
		System.out.println("\n New matrix of Parameter:");
                testScilab.printArray(B);
               	J = ErrorSquare(d,a,B,c);
		System.out.println("\n Value of New Error:"+ J);  
            if (Math.abs(J-Error) < eps) 
            {
                System.out.println("\n ABS:="+Math.abs(J-Error)+"< Eps:=" + eps); 
                System.out.println("\n Iteration:"+i);
                i = epoch;
                System.out.println("\n Iteration max:"+i);
            }
            else i = i+1;
            J = Error;
        }
        System.out.println("\nMatrix new B");
        testScilab.printArray(B);
/*           
        double average = 0;
            for (i = 0; i < c.length; i++)
                average = average + c[i];
            average = average/c.length;
//            System.out.println("\nAverage Value: " + average);

            double SSR = 0;
            double SST = 0;
            
            for (j = 0; j < d.length; j++)
                SSR = SSR + (d[j]-average)*(d[j]-average);            
//            System.out.println("\nSSR Value: " + SSR);            
            
            for (i = 0; i < c.length; i++)
                SST = SST + (c[i]-average)*(c[i]-average);                
            
//            System.out.println("\nSST Value: " + SST);                    
            double R_2_2 = SSR/SST; 
            
//            System.out.println("\nR^2 Value: " + R_2);
//            System.out.println("\nR^2_2 Value: " + R_2_2);
            int index = 0;
            double R_2_tmp;
            double[] b_tmp;
            while(index < M)
//            for (int index = 0; index < M; ++index)
            {   b_tmp = setupParameterBindex(Parameter[index]);  
                R_2_tmp = lookingOtherParameter(x,c,b_tmp);
//                System.out.println(" and the ErrorSquare is: " + estimateErrorSquare(x,c,b_tmp) + " and the R^2 is: " + R_2_tmp);
                if (( R_2_limit < R_2_tmp)&&(R_2_tmp < 1)&&(R_2_2<R_2_limit))
                {   System.out.println("\nMax of Line is: " + M);
                    System.out.println("\nR^2_2 Value: " + R_2_2);
                    System.out.println("\nError with R^2_2 Value: " + R_2_2+ " is " + estimateErrorSquare(x,c,B));
//                    R_2_2 = R_2_tmp;
                    System.out.println("\nR^2_2_tmp Repair: " + R_2_tmp);
                    System.out.println("\nR^2 Value " + R_2_tmp + " with Parameter ["+index+"]: " + Arrays.toString(b_tmp));//Double.toString(lookingOtherParameter(x,c,setupParameterBindex(Parameter[index]))));                    
//                    for (int j=0; j < b_tmp.length; j++)
//                        B[j] = b_tmp[j];
                    System.out.println("\nError with R^2_2 Repair: " + round(R_2_2,2)+ " is " + round(estimateErrorSquare(x,c,B),2));
                    index = M;
                }
                index++;
            }
        System.out.println("\nThe newest Parameter is:"+ Arrays.toString(roundMaxtrix(B))+" and Error with R^2_2: " + round(R_2_2,2)+ " is " + round(estimateErrorSquare(x,c,B),2));    
//        updateParameter(fileParameter,B);    
*/        return estimateCurrentCostValue(X, B);
    }
    public static double[] funcionValue (double [] d, double[][] a, double[] B){
        
        for (int i = 0; i < d.length; i++)
            {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j]; 
//                System.out.println("d["+i+"] in "+d.length+":"+d[i]);
            } 
        return d;
    }
    public static double ErrorSquare(double [] d, double[][] a, double[] B, double[] c){
        double ErrorSquare = 0;
        d = funcionValue(d, a, B);
        for (int i = 0; i < d.length; i++)
            ErrorSquare = (d[i]-c[i])*(d[i]-c[i]);
        return ErrorSquare;
    }
    public static double[] resetArray(double[] array){
        for (int i = 0; i < array.length; i++)
            array[i] = 0;
        return array;
    }

}

