/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;
import IRES.TPCHQuery;
import IRES.runWorkFlowIRES;
import IRES.testQueryPlan;
import LibraryIres.Move_Data;
import LibraryIres.YarnValue;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

import static Algorithms.ReadMatrixCSV.readMatrix;
//import static Algorithms.testScilab.*;
import static org.apache.commons.math.util.MathUtils.round;
/**
 *
 * @author letrungdung
 */
public class Algorithms {
    
    private static final String COMMA_DELIMITER = ",";
    public static double[][] setupMatrixX (double[][] tmp) {
        double[][] x = new double[tmp.length][tmp[0].length - 1];
        for (int i = 0; x.length > i; i++)
            for (int j = 0; x[0].length > j; j++)
                x[i][j] = tmp[i][j];
//        testScilab.printMatrix(x);
        return x;        
    } 
    public static double[] setupMatrixC (double[][] tmp) {
        double[] c = new double[tmp.length];
        for (int i = 0; c.length > i; i++)
                c[i] = tmp[i][tmp[0].length-1];
//        testScilab.printArray(c);
        return c;        
    }
    public static double[][] setupMatrixA (double[][] x) {
        int i=0;
        int j = 0;
        int k = 0;
        double[][] a = new double[x.length][x[0].length + 1];
        for (j=0; a.length > j; j++)
            for (i=0; a[0].length > i; i++)
                if (i==0) {
                    a[j][i]=1;
                }              
                else
                {
                    k = i - 1;
                    a[j][i]=x[j][k];
                }
//        testScilab.printMatrix(a);
        return a;
    }
    public static double[] estimateValue(double[][] x, double[] B) {
        int i = 0;
        int j = 0;
//        testScilab.printArray(a);
        double[] d = new double[x.length]; 
        for (i=0; x.length > i; i++)
        {
            d[i] = B[0];
            for (j = 0; x[0].length > j; j++)
                d[i] = d[i] + B[j+1]*x[i][j];
        }
        testScilab.printArray(d);
        return d;
    }
    public static double estimateCurrentCostValue(double[] x, double[] B) {
        int i,j = 0;
        double d = B[0];
        String output = "\nc=b[0]+";
        String output2 = "="+round(B[0],2);
        for (i = 0; B.length - 1 > i; i++)           
        {
            d = d + B[i+1]*x[i];
            j = i + 1;
            output = output + "b["+j+"]*x["+i+"]+";
            output2 = output2 +"+"+ round(B[i+1],2)+ "*" + round(x[i],2);
        }        
        System.out.println(output + "epsilon" + output2 + "=" + round(d,2));
        testScilab.printArray(x);
        testScilab.printArray(B);
        return d;
    }
    public static double[] estimateError(double[] c, double[] d) {
        int i = 0;
        int j = 0;
        double[] err = new double[c.length]; 
        for (i=0; c.length > i; i++)
        {
            err[i] = d[i] - c[i];
        }
        return err;
    }
    
    public static double[] setupParameterB(double[][] Parameter) {
        int i = 0;
        double[] B = new double[Parameter[0].length]; 
        for (i=0; B.length > i; i++)
        {
            B[i] = Parameter[Parameter.length-1][i];
        }
//        testScilab.printArray(B);
        return B;
    }
    public static double[] setupParameterBindex(double[] Parameter) {
        double[] BB = new double[Parameter.length]; 
        for (int i=0; Parameter.length > i; i++)
        {
            BB[i] = Parameter[i];
        }
//        System.out.println("\nParameter old is:"+ Arrays.toString(BB));
//        testScilab.printArray(BB);
        return BB;
    }
    public static void updateParameter(String filename, double[] b) throws IOException{
        Writematrix2CSV.addArray2Csv(filename, b);
        
    }
    public static double[] fillone(double[] X){
        double[] one = new double [X.length]; 
        for (int i = 0; i < X.length; i++)
            one[i] = 1;
        return one;    
    }
    public static String fileName (String directory, String delay, String name, String KindOfRunning){
	return directory +"/"+ delay +"_"+KindOfRunning+"_"+ name + ".csv";
    }
    public static String NameOfFileName(String delay, String name, String KindOfRunning){
	return delay+"_"+KindOfRunning+"_"+name;
    }
    public static int estimateSizeOfMatrix(int Max_Line, int numberOfVariable, String directory, double R_2_limit, String delay, String KindOfRunning) throws IOException {
        String fileRealValue = fileName(directory,delay,"realValue",KindOfRunning);
        String fileParameter = fileName(directory,delay,"parameter",KindOfRunning);
        String fileEstimate = fileName(directory,delay,"estimate",KindOfRunning);
        int Max_Estimate = CsvFileReader.count(fileEstimate)-1;
        int MaxOfLine;
        if (Max_Estimate < Max_Line)
            MaxOfLine = Max_Estimate;
        else MaxOfLine = Max_Line;
        
        System.out.println("\nMax Of Line is:"+MaxOfLine);
        Path filePathParameter = Paths.get(fileParameter);
        
        double[] X = new double [numberOfVariable+1];
        X = fillone(X);
        double[][] initParameter = new double [2][X.length];
        initParameter = initParameterValue(X);
        if (!Files.exists(filePathParameter)) {
            Files.createFile(filePathParameter);
            int n = initParameter[0].length;
            int i = 0;
            String tmp = "";
            for (i = 0; n -1 >i; i++)
            tmp = tmp + "b[" + i + "]" + COMMA_DELIMITER;
            if (n - 1 == i) tmp = tmp + "b[" + i + "]";
            String FILE_HEADER = tmp;
            
            System.out.println("\nNeed to update Parameter Value");
            Writematrix2CSV.Writematrix2CSV(initParameter, fileParameter, FILE_HEADER);
            }
        int M = numberOfVariable + 2;
        // = readMatrix(fileParameter, M);
        double R_2 = 0;
        double R_2_2 = 0;       
        int M_init = M;
        int N = 1;
        double[][] realValue;// = readMatrix(fileValue, M);
        double[][] estimateValue;
        double[][] Parameter;
        double[][] x;
        double[][] a;
        double[] B;
        double[] b;
        double[] c;// = new double[realValue.length];
        double[] d;// = new double[realValue.length];
        
        int sizeOfMatrix = Max_Line;
        
        while(((Math.abs(R_2_2)< R_2_limit)||(Math.abs(R_2_2)> 1))&&(M < MaxOfLine))
        {   //System.out.println("\nMatrix Real Value");           
            realValue = readMatrix(fileRealValue, M);
            //System.out.println("\nMatrix Estimate Value"); 
            estimateValue = readMatrix(fileEstimate, M);
            Parameter = readMatrix(fileParameter, M);
            
            x = new double[realValue.length][realValue[0].length - 1];
            a = new double[realValue.length][realValue[0].length];
            B = new double[realValue[0].length];
            if (realValue.length < estimateValue.length)
                {
                c = new double[realValue.length];
                d = new double[realValue.length];
                }
            else 
                {
                c = new double[estimateValue.length];
                d = new double[estimateValue.length];
                }
            
            //System.out.println("length of D: "+d.length);
//            System.out.println("\nMatrix Real Value");
            x = setupMatrixX(realValue);
//            System.out.println("\nMatrix A");
            a = setupMatrixA(x);
//            System.out.println("\n");            
            
//            System.out.println("\nMatrix c");
            c = setupMatrixC(realValue);
            d = setupMatrixC(estimateValue);
            //System.out.println("length of C: "+c.length);
            B = testScilab.multiply(testScilab.multiply(testScilab.invert(testScilab.multiply(testScilab.transpose(a),a)),testScilab.transpose(a)),c);
            for (int k = 0; k < B.length; k++){
                if (Double.isNaN(B[k]))
                {
//                System.out.println("\nNew Parameter is infinity, use old Parameter");
                B = setupParameterB(Parameter);
                k = B.length;
                }
            }
            
//            else 
//            System.out.println("\nParameter");
//            testScilab.printArray(B);
//            System.out.println("\nMatrix estimate Value");
    
            for (int i = 0; i < d.length; i++)
            {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j]; 
//                System.out.println("d["+i+"] in "+d.length+":"+d[i]);
            }

/*            System.out.println("\nMatrix d");
            d = setupMatrixC(estimateValue);
            double [] cc;
            double [] dd;
            if (c.length < d.length)
                {
                cc = new double [c.length];
                dd = new double [c.length];
                
                }
            else
                {
                cc = new double [d.length];
                dd = new double [d.length];                
                }
            for (int k = 0; k < cc.length; k++)
            {
                cc[k] = c[k];
                dd[k] = d[k];
            }
///////////////////////////////////////////////////////////            
/*            double[][] Trans_A = transpose(a);
            testScilab.printMatrix(Trans_A);
            System.out.println("\nMatrix Multiply of Transpose A and A");
            double[][] Mul_Trans_A_A = multiply(Trans_A,a);
            testScilab.printMatrix(Mul_Trans_A_A);       
            double determinant = testScilab.determinant2(Mul_Trans_A_A);
            System.out.println("\nDet of Multiply of Transpose A and A:" + determinant);
            System.out.println("\nInvert A");
            
            B = multiply(multiply(invert(multiply(transpose(a),a)),transpose(a)),c);
            if (B[0] == Double.NaN){
                System.out.println("New Parameter is infinity, use old Parameter");
                B = setupParameterB(Parameter);
                }
/*            if (determinant*determinant >= 0.0000001 )
            {
                double[][] invert = invert(Mul_Trans_A_A);
                testScilab.printMatrix(invert);
                System.out.println("\nMatrix multi_in");
                double[][] multi_in = multiply(invert,Trans_A);
                testScilab.printMatrix(multi_in);
                double[] multi_out = multiply(multi_in,c);
                testScilab.printArray(multi_out);
                B = multiply(multiply(invert(multiply(transpose(a),a)),transpose(a)),c);
            }
            else{
                System.out.println("\nCannot Update new Parameter, use old Parameter");
                B = setupParameterB(Parameter);
            }
*/
///////////////////////////////////////////////////////////////////////////////            
                        
/*            System.out.println("\nMatrix of Parameter");
//            B = multiply(multiply(invert(multiply(transpose(a),a)),transpose(a)),c);
            testScilab.printArray(B);
            
            System.out.println("\nMatrix of current value");  
            c = setupMatrixC(realValue);
            
            System.out.println("\nMatrix estimate Value Test first");           
            double[] d1 = estimateValue(x,B);           
            testScilab.printArray(d1);
            
            System.out.println("\nMatrix estimate Value");
            for (int i = 0; i< a.length;i++)
            {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j];
            } 
          System.out.println("\nObserver Value: ");  
            testScilab.printArray(c);           
            System.out.println("\n");
            
            System.out.println("\nEstimate Value: ");  
            testScilab.printArray(d);           
            System.out.println("\n");
*/            
            double average = 0;
            for (int i = 0; i < c.length; i++)
                average = average + c[i];
            average = average/c.length;
//            System.out.println("\nAverage Value: " + average);

            double SSR = 0;
            double SST = 0;
            double SSE = 0;
            double SSY = 0;
            
            for (int k = 0; k < c.length; k++)
            {
                SSE = SSE + (c[k]-d[k])*(c[k]-d[k]);                
            }
            
//            System.out.println("\na SSE Value: " + SSE);
            for (int k = 0; k < c.length; k++){
                SSY = SSY + (c[k]-average)*(c[k]-average);                
            }
//            System.out.println("\na SSY Value: " + SSY);
            R_2 = 1 - SSE/SSY;
            
            for (int j = 0; j < d.length; j++)
                SSR = SSR + (d[j]-average)*(d[j]-average);            
//            System.out.println("\nSSR Value: " + SSR);            
            
            for (int i = 0; i < c.length; i++)
                SST = SST + (c[i]-average)*(c[i]-average);                
            
//            System.out.println("\nSST Value: " + SST);                    
            R_2_2 = SSR/SST; 
            
//            System.out.println("\nR^2 Value: " + R_2);
//            System.out.println("\nR^2_2 Value: " + R_2_2);
            int index = 0;
            double R_2_tmp;
            while(index < M)
//            for (int index = 0; index < M; ++index)
            { 
                R_2_tmp = lookingOtherParameter(x,c,setupParameterBindex(Parameter[index]));
//                System.out.println(" and the ErrorSquare is: " + estimateErrorSquare(x,c,setupParameterBindex(Parameter[index])) + " and the R^2 is: " + R_2_tmp);
                if (( R_2_limit < R_2_tmp)&&(R_2_tmp < 1)&&(R_2_2<R_2_tmp))
                {   
                    System.out.println("\nR^2_2 Value: " + R_2);
                    System.out.println("\nR^2_2 Value: " + R_2_2);
//                    R_2_2 = R_2_tmp;
                    System.out.println("\nR^2_2 Repair: " + R_2_tmp);
                    System.out.println("\nR^2 Value with Parameter["+index+"]:" + R_2_tmp);//Double.toString(lookingOtherParameter(x,c,setupParameterBindex(Parameter[index]))));
                    index = M;
                }
                index++;
            }
            if (M < MaxOfLine) sizeOfMatrix = M;
            
            M = M+1;
        }
        System.out.println("\nR^2 Value: " + R_2);
        System.out.println("\nR^2_2 Value: " + R_2_2);
        System.out.println("\nR^2 Value Limit: " + R_2_limit);
        System.out.println("\nSize of real Value: " + sizeOfMatrix);
        System.out.println("\nEstimate the maximum of Matrix:------------------------------------------------------------------------ ");
        return sizeOfMatrix;
    }
    
    public static double estimateErrorSquare (double[][] x, double[] c, double[] B){
        double ErrorSquare = 0;
        double[][] a = setupMatrixA(x);
        double[] d = new double[c.length];
        for (int i = 0; i < d.length; i++)
            {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j]; 
                ErrorSquare = ErrorSquare + (c[i]-d[i])*(c[i]-d[i]);
            }        
        return ErrorSquare;       
    }
    public static double lookingOtherParameter(double[][] x, double[] c, double[] b){
        double average = 0;
            for (int i = 0; i < c.length; i++)
                average = average + c[i];
            average = average/c.length;
        double SSR = 0;
        double SST = 0;
        double[] d = new double[c.length];
        double[][] a = setupMatrixA(x);
        for (int i = 0; i < d.length; i++)
            {
                d[i] = 0;
                for (int j = 1; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*b[j];
//                System.out.println("d["+i+"] in "+d.length+":"+d[i]);
            }
        for (int j = 0; j < c.length; j++)
                SSR = SSR + (d[j]-average)*(d[j]-average);                                   
            for (int i = 0; i < c.length; i++)
                SST = SST + (c[i]-average)*(c[i]-average);                                  
        double R_2_2 = SSR/SST;
        return R_2_2;
    }
    public static double[][] initParameterValue(double[] X) {
        double[] X1 = new double [X.length];
        double[] X2 = new double [X.length];
        for (int i = 0; i < X.length; i++)
        { 
            X1[i] = 1;
            X2[i] = 1;
        }
        double [][] B = {X1,X2};
        return B;
}
        
    public static double estimateCostValue(int sizeOfValue, String fileValue, String fileParameter, double[] X, double R_2_limit) throws IOException{
        int M = sizeOfValue;
        int N = 1;
        Path filePathParameter = Paths.get(fileParameter);
//        double[][] initParameter = {{1,1,1,1,1},{1,1,1,1,1}};
        double[][] initParameter = initParameterValue(X);
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
        double[][] realValue = readMatrix(fileValue, M);        
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
        
        System.out.println("\nMatrix new B");
        B = testScilab.multiply(testScilab.multiply(testScilab.invert(testScilab.multiply(testScilab.transpose(a),a)),testScilab.transpose(a)),c);
            if (Double.isNaN(B[0])){
                System.out.println("New Parameter is infinity, use old Parameter");
                B = setupParameterB(Parameter);
                }        
        testScilab.printArray(B); 
        
///////////////////////////////////////////////////////////////////////////////////        
        for (int i = 0; i < d.length; i++)
            {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j]; 
//                System.out.println("d["+i+"] in "+d.length+":"+d[i]);
            }        
        double average = 0;
            for (int i = 0; i < c.length; i++)
                average = average + c[i];
            average = average/c.length;
//            System.out.println("\nAverage Value: " + average);

            double SSR = 0;
            double SST = 0;
            
            for (int j = 0; j < d.length; j++)
                SSR = SSR + (d[j]-average)*(d[j]-average);            
//            System.out.println("\nSSR Value: " + SSR);            
            
            for (int i = 0; i < c.length; i++)
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
        updateParameter(fileParameter,B);    
        return estimateCurrentCostValue(X, B);
    }
    public static String preapreFile(String directory) throws IOException{
        String directoryData = directory;
        File dir = new File(directoryData);
		if (!dir.exists()) {
			dir.mkdir();
                        System.out.println("\nCreate directory:-------------------------"+directoryData);
		}
                
        directoryData = directoryData+ "/data";        
        File dirData = new File(directoryData);
		if (!dirData.exists()) {
			dirData.mkdir();
                        System.out.println("\nCreate directory:-------------------------"+directoryData);
		}        
        Path filePathCostValue = Paths.get(directoryData+"/cost.csv"); 
        if (!Files.exists(filePathCostValue))
            Files.createFile(filePathCostValue);
        
        Path filePathExecTimeValue = Paths.get(directoryData+"/execTime.csv"); 
        if (!Files.exists(filePathExecTimeValue))
            Files.createFile(filePathExecTimeValue);
        return directoryData;
    }
    public static String DefaultDirectory(Move_Data Data){
        String IRES_HOME = new App().readhome("IRES_HOME");
        String IRES_library = IRES_HOME+"/asap-platform/asap-server";
        String NameOp = Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To();
        String directory_operator = IRES_library+"/defaultAsapLibrary/operators/"+ NameOp;
        return directory_operator;
    }
    public static String operatorFolder (){
        String IRES_HOME = new App().readhome("IRES_HOME");       
        String IRES_library = IRES_HOME+"/asap-platform/asap-server";    
        String OperatorFolder = IRES_library+"/target/asapLibrary/operators";
        return OperatorFolder;
    }
    public static String getNameQuery(String SQL){
        if (SQL.equals("")) return "";
        else return SQL.substring(SQL.lastIndexOf("DROP TABLE IF EXISTS "),SQL.indexOf("_"))
                .replaceAll("DROP TABLE IF EXISTS ","")
                .replaceAll(" ","");

    }
    public static void mainIRES(Move_Data Data, String SQL, YarnValue yarnValue, double TimeOfDay, double[] size, String KindOfRunning ) throws Exception{      
        String OperatorFolder = operatorFolder();
        String realValue, parameter, estimate, directory, Error;
        directory = testWriteMatrix2CSV.getDirectory(Data) ;
        directory = preapreFile(directory);
        
        String KindOfMoving = "";
        if (Data.get_Operator().toLowerCase().contains("move")) KindOfMoving = "move";
        if (Data.get_Operator().toLowerCase().contains("join")) KindOfMoving = "join";
        if (Data.get_Operator().toLowerCase().contains("sql")) KindOfMoving = "sql";
        if (Data.get_Operator().toLowerCase().equals("tpch")) KindOfMoving = "join";
        
        runWorkFlowIRES IRES = new runWorkFlowIRES();
        int numberParameter = size.length + 1;
        int numerOfVariable = numberParameter-1;
        if (Data.get_Operator().toLowerCase().contains("move")){
            IRES.createAbstractOperatorMove(Data, SQL);      
            IRES.createWorkflowMove(Data, SQL);
        }
        if (Data.get_Operator().toLowerCase().contains("join")){
            IRES.createAbstractOperatorJoin(Data, SQL);
            IRES.createWorkflowJoin(Data, SQL);
        }
        if (Data.get_Operator().toLowerCase().contains("sql")){
            IRES.createAbstractOperatorSQL(Data, SQL);
            IRES.createWorkflowSQL(Data, SQL);
        }    
       
        
        String delay_ys = "";
	if (TimeOfDay<1) delay_ys = "no_delay";

	realValue = fileName(directory,delay_ys,"realValue",KindOfRunning);
        System.out.println("\nReal Value File:-----------------------------------------------------"+realValue);
	String NameOfRealValue = NameOfFileName(delay_ys,"realValue",KindOfRunning);
        
        parameter = fileName(directory,delay_ys,"parameter",KindOfRunning);

        Error = fileName(directory,delay_ys,"error",KindOfRunning);
        
        estimate = fileName(directory,delay_ys,"estimate",KindOfRunning);
        
        String NameOfEstimateValue = NameOfFileName(delay_ys,"estimate",KindOfRunning);
        
	int Max = 0;
	Path filePathRealValue = Paths.get(realValue);
        if (Files.exists(filePathRealValue))
        Max = CsvFileReader.count(realValue)-1;
        
        double R_2_limit = 0.8;////////////////////////////////////////////////////////////////////
        int sizeOfValue;
        
        String policy ="metrics,cost,execTime\n"+
                "groupInputs,execTime,max\n"+
                "groupInputs,cost,sum\n"+
                "function,execTime,min";//\n"+
                //"function0,execTime,min\n"+
                //"function1,cost,min"; 
    
        String NameOp = runWorkFlowIRES.Nameop(Data);
        String NameOfWorkflow = NameOp+"_Workflow";     
        runWorkFlowIRES workflow = new runWorkFlowIRES();
///////////Set up for the first time only ////////////////////////         
        double costEstimateValue = 0;
        
        String DataIn = Data.get_DataIn();
        String DataOut = Data.get_DataOut();
        double Numberuser = 100;

        sizeOfValue = estimateSizeOfMatrix(Max, numerOfVariable, directory, R_2_limit, delay_ys, KindOfRunning);

        System.out.println("\nReal Running:--------------------------------------------------------"+
                "\n"+Data.get_DataIn()+"\n"+yarnValue.toString());              
        double[] StochasticValue = setupStochasticValue(size);
//	String test_file = "/home/ubuntu/test.csv";
//	LinearRegression.testLinearRegression(test_file);
//	double costEstimateValue2 = batchgradientdescent.estimateGradient(sizeOfValue, realValue, parameter, StochasticValue, R_2_limit);
//        System.out.println("\n Estimate Value of Batch Gradient Descent is: " + costEstimateValue2);
	costEstimateValue = estimateCostValue(sizeOfValue, realValue, parameter, StochasticValue, R_2_limit);
	double[] B = setupParameterB(readMatrix(parameter, 1));
        if (Data.get_Operator().toLowerCase().contains("move")){
            IRES.createDatasetMove_Hive_Postgres(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
            IRES.createOperatorMove(Data, SQL, costEstimateValue, B);
            IRES.createDataMove2(Data, SQL, yarnValue);
        }
        if (Data.get_Operator().toLowerCase().contains("join")) {
            IRES.createDatasetJoin(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
            IRES.createOperatorJoin(Data, SQL, costEstimateValue);
            IRES.createDataMove2(Data, SQL, yarnValue);
        }  
        if (Data.get_Operator().toLowerCase().contains("sql")) {
            IRES.createDatasetSQL(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
            IRES.createOperatorSQL(Data, SQL, costEstimateValue);
            IRES.createDataSQL(Data, SQL, yarnValue);
        }
        
	Path filePathEstimateValue = Paths.get(estimate); 
        if (!Files.exists(filePathEstimateValue))
            Files.createFile(filePathEstimateValue); 
        testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size, costEstimateValue)), NameOfEstimateValue); 

        double Time_Cost = IRES.runWorkflow(Data, size, NameOfWorkflow, policy);
//        double Time_Cost = 50*Math.random();
        long delay = SimulateStochastic.TimeWaiting(Numberuser,TimeOfDay)/1000;
        Time_Cost = Time_Cost + delay;        
        testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size, Time_Cost)), NameOfRealValue);
        testWriteMatrix2CSV.storeValueServer(Data, SQL, setupStochasticValue(setupValue(size, Time_Cost)), "execTime_realValue");
        String MaxTrain = new App().readhome("Max");
        int Max_train = Integer.parseInt(MaxTrain)*size.length;
        if (CsvFileReader.count(directory + "/execTime_realValue.csv")<Max_train)
        {
            //testWriteMatrix2CSV.storeValueServer(Data, SQL, setupStochasticValue(setupValue(size, Time_Cost)), "execTime_realValue");
            testWriteMatrix2CSV.storeValueServer(Data, SQL, setupStochasticValue(setupValue(size, Time_Cost)), "execTime");     
        }
        else fixExecTime(Data, Max_train);
        String weka = "weka"+ Max_train;
        System.out.println("----------"+weka+"-------------------");
	    storeCost(Data,Time_Cost,"execTime_estimate",weka);
        storeCost(Data,Time_Cost,NameOfEstimateValue,"dream");

        String wekaStore = weka+"_"+Data.get_DatabaseIn()+"_"+getNameQuery(SQL);
        String dreamStore = "dream"+"_"+Data.get_DatabaseIn()+"_"+getNameQuery(SQL);

        storeCost(Data,Time_Cost,"execTime_estimate",wekaStore);
        storeCost(Data,Time_Cost,NameOfEstimateValue,dreamStore);
	/*
	String modelDirPath = OperatorFolder+"/"+NameOp + "/models";
        File modelDir = new File(modelDirPath);
            if (modelDir.exists()) {
            //modelDir.delete();
                try {
                //Deleting the directory recursively using FileUtils.
                    FileUtils.deleteDirectory(modelDir);
                    System.out.println("Directory has been deleted recursively !");
                } catch (IOException ee) {
                    System.out.println("Problem occurs when deleting the directory : " + modelDirPath);
                    ee.printStackTrace();
                }
            }
	*/
	System.out.println("\n Estimate Value is: " + costEstimateValue);
        System.out.println("\n Real Value with Delay is: " + Double.toString(Time_Cost));
        System.out.println("\n Real Value without Delay is: " + Double.toString(Time_Cost-delay));
        System.out.println("\n Delay Value is: " + delay); 
        
        reportResult.reportError(Error, setupStochasticValue(setupValue(size, Time_Cost)), costEstimateValue, sizeOfValue);
//        reportResult.report(sizeOfValue, realValue, estimate, Error);
        
//        double costEstimateValue2 = batchgradientdescent.estimateGradient(sizeOfValue, realValue, parameter, StochasticValue, R_2_limit);
        //Thread.sleep(1000);
//        OptimizeWorkFlow Optimize = new OptimizeWorkFlow();
        //OptimizeWorkFlow.OptimizeWorkFlow(Data, policy);

//////////////////////////////////////////////////////////////////////////////////////          
    }   
    public static void storeCost(Move_Data Data, double realValue, String estimate, String result) throws IOException {
        String directory = testWriteMatrix2CSV.getDirectory(Data) ;
        double [][] estimateMatrix = readMatrix(directory + "/data/"+estimate+".csv",1);
        double [][] resultMatrix = new double [estimateMatrix.length][estimateMatrix[0].length+1];
        for (int i = 0; i < resultMatrix.length; i++){
            for (int j = 0; j < estimateMatrix[0].length; j++){
                resultMatrix[i][j] = estimateMatrix[i][j];
            }
            resultMatrix[i][estimateMatrix[0].length] = realValue;
        }           
        String resultFile = directory + "/data/"+result+".csv";
        File file = new File(result);
        if (file.exists()) 
            file.delete();
        Writematrix2CSV.addMatrix2Csv(resultFile, resultMatrix);
    }
	public static void fixExecTime(Move_Data Data, int Max) throws IOException{
        String directory = testWriteMatrix2CSV.getDirectory(Data) ;
        double [][] execTime = readMatrix(directory + "/data/execTime_realValue.csv", Max);
        String execTimeFile = directory + "/data/execTime.csv";
        File file = new File(execTimeFile);
        if (file.exists()) 
            file.delete();
        Writematrix2CSV.addMatrix2Csv(execTimeFile, execTime);
    }
    public static double[] setupValue(double[] size, double value){
        double[] Value = new double [size.length+1];
        for (int i = 0; i < size.length; i++)
            Value[i] = size[i];
        Value[size.length] = value;
        return Value;
    }

    public static double [] setupStochasticValue(double[] size){
        double[] StochasticValue = new double [size.length];
        for (int i = 0; i < size.length; i++)
            StochasticValue[i] = size[i];
        return StochasticValue;
    }
    public static double [] initParamter(int numberParameter){
        double [] Parameter = new double [numberParameter];
        for (int i = 0; i < numberParameter; i++)
            Parameter[i] = 1;
        return Parameter;
    }
            
    public static void setup(Move_Data Data, YarnValue yarnValue, double[] size, String Size_tpch, double TimeOfDay, String KindOfRunning) throws Exception {        
        runWorkFlowIRES IRES = new runWorkFlowIRES(); 
        int numberParameter = size.length + 1;
        String[] randomQuery = testQueryPlan.createRandomQuery("",Size_tpch);
        
        String KindOfMoving = "";
        if (Data.get_Operator().toLowerCase().contains("move")) KindOfMoving = "move";
        if (Data.get_Operator().toLowerCase().contains("join")) KindOfMoving = "join";
        if (Data.get_Operator().toLowerCase().contains("sql")) KindOfMoving = "sql";
        if (Data.get_Operator().toLowerCase().equals("tpch")) KindOfMoving = "join";
        double[] size_random = TPCHQuery.calculateSize(randomQuery, Data.get_From(), Data.get_To(), Size_tpch, KindOfMoving);
        double[] yarn_random = testQueryPlan.createRandomYarn();
        String directory = testWriteMatrix2CSV.getDirectory(Data) ;
        directory = preapreFile(directory);
        double Data_size;
        double Ram;
        double Core;
        double TimeRepsonse = 0;
        String SQL = "";
        int i = 0;        
        double Numberuser = 100;
            
        String delay_ys = "";
	if (TimeOfDay<1) delay_ys = "no_delay";
        String NameOfRealValue = NameOfFileName(delay_ys,"realValue",KindOfRunning);
	    String NameOfParameter = NameOfFileName(delay_ys,"parameter",KindOfRunning);
        String NameOfEstimateValue = NameOfFileName(delay_ys,"estimate",KindOfRunning);
        
        String execTime = directory + "/data/execTime.csv";
        String execTime_estimate = directory + "/data/execTime_estimate.csv";
        String RealValue = directory + "/data/"+NameOfRealValue;
        String EstimateValue = directory + "/data/"+NameOfEstimateValue;
        String ParameterValue = directory + "/data/"+NameOfParameter;
        
        Path filePathExecTime = Paths.get(execTime); 
        Path filePathExecTime_Estimate = Paths.get(execTime_estimate);
        Path filePathRealValue = Paths.get(RealValue); 
        Path filePathEstimateValue = Paths.get(EstimateValue); 
        Path filePathParameterValue = Paths.get(ParameterValue); 

        
            

        double[] Parameter = initParamter(numberParameter);
        Random rn = new Random();
        if (!Files.exists(filePathRealValue)){
        
            for (i = 0; i < size.length+2; i++){   
                System.out.println("\nTest Time:"+i+"--------------------------------------------------------");
                TimeOfDay = Math.random();
                randomQuery = testQueryPlan.createRandomQuery("",Size_tpch);
                size_random = TPCHQuery.calculateSize(randomQuery,Data.get_From(), Data.get_To(),Size_tpch,KindOfMoving);
                TimeRepsonse =  Math.random()*50;//IRES.runWorkflow(NameOfWorkflow, policy);
                double delay = SimulateStochastic.waiting(Numberuser,TimeOfDay);
                TimeRepsonse = TimeRepsonse + delay;                   
                //size_random[size_random.length-1] = TimeOfDay;  
                int Max = 0;
                int random = 0;

                String fileLink = DefaultDirectory(Data)+"/data/"+NameOfRealValue+".csv";
                Path filePath = Paths.get(fileLink);

                if (Files.exists(filePath)){
                    Max = CsvFileReader.count(fileLink)-1;
                    double[][] matrix = readMatrix(fileLink, Max);          
                    random = rn.nextInt(matrix.length);
                    double[] Array = resetValue(Data, NameOfRealValue, size_random, random); 
                    TimeRepsonse = Array[Array.length-1];
                    double[] array = cutOff(Array);
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(array,TimeRepsonse)), NameOfRealValue);

                    Array = resetValue(Data, "execTime", size_random, random);
                    TimeRepsonse = Array[Array.length-1];
                    array = cutOff(Array);
                    testWriteMatrix2CSV.storeValueServer(Data, SQL, setupStochasticValue(setupValue(array, TimeRepsonse)), "execTime");

                    Array = resetValue(Data, NameOfEstimateValue, size_random, random);
                    TimeRepsonse = Array[Array.length-1];
                    array = cutOff(Array);
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(array, TimeRepsonse)), NameOfEstimateValue);
                    //
                    Array = resetValue(Data, "execTime_estimate", size_random, random); 
                    TimeRepsonse = Array[Array.length-1];
                    array = cutOff(Array);
                    testWriteMatrix2CSV.storeValueServer(Data, SQL, setupStochasticValue(setupValue(array,TimeRepsonse)), "execTime_estimate");

                    fileLink = DefaultDirectory(Data)+"/data/"+NameOfParameter+".csv";
                    filePath = Paths.get(fileLink);
                    double[] Array2 = resetValue(Data, NameOfParameter, size_random, random);
                    testWriteMatrix2CSV.storeParameter(Data, Array2, NameOfParameter);
                }
                
                else {
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size_random,TimeRepsonse)), NameOfRealValue);
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size_random,TimeRepsonse)), "execTime");
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size_random,TimeRepsonse)), NameOfEstimateValue);
                    testWriteMatrix2CSV.storeValue(Data, SQL, setupStochasticValue(setupValue(size_random,TimeRepsonse)), "execTime_estimate");
                    testWriteMatrix2CSV.storeParameter(Data, Parameter, NameOfParameter);
                }
            
            }
        }
        //FileUtils.copyDirectory(FileUtils.getFile(DefaultDirectory(Data)+"/data"), 
        //                    FileUtils.getFile(directory));
        if(Data.get_Operator().toLowerCase().contains("move")){
            IRES.createDatasetMove_Hive_Postgres(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
            IRES.createDataMove2(Data, SQL, yarnValue);   
            }
        if(Data.get_Operator().toLowerCase().contains("join")){
            IRES.createDatasetJoin(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
            IRES.createDataMove2(Data, SQL, yarnValue); 
        }
        if(Data.get_Operator().toLowerCase().contains("sql")){
            IRES.createDatasetSQL(Data, size, SQL, TimeOfDay);//createDatasetMove(Data, SQL);
            IRES.createDataSQL(Data, SQL, yarnValue); 
        }
        
//        }
        
    }
    public static double[] cutOff (double[] Array){
        double [] array = new double[Array.length-1];
        for (int i = 0; i< array.length; i++)
            array[i] = Array[i];
        return array;
    }
    public static double[] resetValue(Move_Data Data, String NameOfRealValue, double[] array, int random) throws IOException{
        String fileLink = DefaultDirectory(Data)+"/data/"+NameOfRealValue+".csv";
        Path filePath = Paths.get(fileLink);
        if (Files.exists(filePath)){
        int Max = CsvFileReader.count(fileLink)-1;
        double[][] matrix = readMatrix(fileLink, Max);       
        return matrix[random];
        }
        else return array;
    }
    public static double[] roundMaxtrix(double [] tmp){
        double [] matrix = new double[tmp.length];
        for (int i = 0; i < tmp.length; i ++)
            matrix[i] = round(tmp[i], 2);
        return matrix;
    }
    public static void main (String [] args){
        String test = "DROP TABLE IF EXISTS randomQuery[2]_From_To;";
        String Query = getNameQuery(test);
        System.out.println(Query);
    }
}
