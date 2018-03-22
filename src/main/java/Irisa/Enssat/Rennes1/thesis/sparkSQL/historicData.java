package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import Algorithms.Writematrix2CSV;
import Algorithms.testScilab;
import Scala.Cost;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;
import scala.Int;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static Algorithms.Algorithms.*;
import static Algorithms.ReadMatrixCSV.readMatrix;
import static Algorithms.testScilab.invert;
import static Algorithms.testScilab.multiply;
import static Algorithms.testScilab.transpose;
import static org.apache.commons.math.util.MathUtils.round;

/**
 * Created by letrungdung on 20/03/2018.
 */
public class historicData {
    private static final String COMMA_DELIMITER = ",";
    static String sparkHome = new App().readhome("SPARK_HOME");
    static String idQuery = "";
    public static void storeIdQuery(String IdQuery){
        idQuery = IdQuery;
        System.out.println(idQuery);
    }

    public static java.util.List<Cost> dreamValue(java.util.List<Cost> tempList,
                                                  java.util.List<scala.collection.immutable.List<Int>> finalSetPlansList){
        java.util.List<Cost> List = tempList;
        for (int i = 0; i < finalSetPlansList.size(); i++){
            Scala.Cost temp = List.get(i);
            double executeTime = dreamValue(idQuery,
                    finalSetPlansList.get(i).toString(), "executeTime",
                    tempList.get(i).card().toDouble(),
                    tempList.get(i).size().toDouble());
            temp = new Cost(temp.card(),temp.size(),executeTime,temp.moneytary());
            System.out.println(temp);
            List.set(i,temp);
        }
        /*
        for (int i = 0; i < tempList.size(); i++){
            System.out.println(tempList.get(i).card());
            System.out.println(tempList.get(i).size());
            System.out.println(tempList.get(i).executeTime());
            System.out.println("///////////////////////");
        }
        */
        return List;
    }
    public static double dreamValue(String homeSetTable, String logicalId, String nameValue, double cardinality, double size){
        //String folder = "data/dream/" +homeSetTable + "/" + logicalId;
        int numberVariables = 2;
        String folder = "data/dream/" +idQuery + "/" + logicalId;
        String file = folder + "/" + nameValue + ".csv";
        String fileEstimate = fileEstimateValue(file);
        File Dir = new File(folder);
        if (!Dir.exists()) {
            Dir.mkdir();
        }
        Path filePath = Paths.get(file);
        if (!Files.exists(filePath)) {
            try {
                Files.createFile(filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            if (CsvFileReader.count(file)==0){
                setupFile(file, numberVariables);
                return 0;
            }
            else {
                List<Double> variables = new ArrayList<>();
                variables.add((double)(cardinality));
                variables.add((double)(size));
                double estimateValue = estimate(file, variables);
                double[] valueArray = setupValue(variables, estimateValue);
                //Writematrix2CSV.addArray2Csv(fileEstimate, valueArray);
                return estimateValue;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }
    public static void estimateAndStore(String homeSetTable, String logicalId, String nameValue, double cardinality, double size) throws IOException {
        String folder = "data/dream/" +idQuery + "/" + logicalId;
        String file = folder + "/" + nameValue + ".csv";
        String fileEstimate = fileEstimateValue(file);
        List<Double> variables = new ArrayList<>();
        variables.add((double)(cardinality));
        variables.add((double)(size));
        double estimateValue = estimate(file, variables);
        double[] valueArray = setupValue(variables, estimateValue);
        Writematrix2CSV.addArray2Csv(fileEstimate, valueArray);
    }
    public static double[] convertListToArray(List<Double> variables){
        double[] doubleArray = new double[variables.size()];
        for (int i = 0; i < variables.size(); i++){
            doubleArray[i] = variables.get(i);
        }
        return doubleArray;
    }
    public static double estimate(String file, List<Double> variables){
        try {
            double R_2_limit = 0.8;
            int Max_line_estimate = estimateSizeOfMatrix(CsvFileReader.count(file),variables.size(),file,R_2_limit);
            double value = estimateCostValue(Max_line_estimate,file,convertListToArray(variables),R_2_limit);
            return value;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static String fileParameter (String file){
        return file.replace(".csv","_") + "Parameter" + ".csv";
    }
    public static String fileRealValue (String file){
        return file.replace(".csv","_") + "RealValue" + ".csv";
    }
    public static String fileEstimateValue (String file){
        return file.replace(".csv","_") + "EstimateValue" + ".csv";
    }
    public static int estimateSizeOfMatrix(int Max_Line, int numberOfVariable, String file, double R_2_limit) throws IOException {
        String fileParameter = fileParameter(file);
        String fileRealValue = fileRealValue(file);
        String fileEstimate = fileEstimateValue(file);
        int Max_Estimate = CsvFileReader.count(file);
        int MaxOfLine;
        if (Max_Estimate < Max_Line)
            MaxOfLine = Max_Estimate;
        else MaxOfLine = Max_Line;
        int M = numberOfVariable + 2;
        double R_2 = 0;
        double R_2_2 = 0;
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
            if (realValue.length < estimateValue.length) {
                c = new double[realValue.length];
                d = new double[realValue.length];
            }
            else {
                c = new double[estimateValue.length];
                d = new double[estimateValue.length];
            }
            //System.out.println("length of D: "+d.length);
            //System.out.println("\nMatrix Real Value");
            x = setupMatrixX(realValue);
            //System.out.println("\nMatrix A");
            a = setupMatrixA(x);
            //System.out.println("\n");
            //System.out.println("\nMatrix c");
            c = setupMatrixC(realValue);
            d = setupMatrixC(estimateValue);
            //System.out.println("length of C: "+c.length);
            B = multiply(multiply(invert(multiply(transpose(a),a)),transpose(a)),c);
            for (int k = 0; k < B.length; k++){
                if (Double.isNaN(B[k])) {
                //System.out.println("\nNew Parameter is infinity, use old Parameter");
                    B = setupParameterB(Parameter);
                    k = B.length;
                }
            }
            for (int i = 0; i < d.length; i++) {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j];
                //System.out.println("d["+i+"] in "+d.length+":"+d[i]);
            }
            double average = 0;
            for (int i = 0; i < c.length; i++)
                average = average + c[i];
            average = average/c.length;
            //System.out.println("\nAverage Value: " + average);
            double SSR = 0;
            double SST = 0;
            double SSE = 0;
            double SSY = 0;

            for (int k = 0; k < c.length; k++) {
                SSE = SSE + (c[k]-d[k])*(c[k]-d[k]);
            }
            //System.out.println("\na SSE Value: " + SSE);
            for (int k = 0; k < c.length; k++){
                SSY = SSY + (c[k]-average)*(c[k]-average);
            }
            //System.out.println("\na SSY Value: " + SSY);
            R_2 = 1 - SSE/SSY;

            for (int j = 0; j < d.length; j++)
                SSR = SSR + (d[j]-average)*(d[j]-average);
            //System.out.println("\nSSR Value: " + SSR);

            for (int i = 0; i < c.length; i++)
                SST = SST + (c[i]-average)*(c[i]-average);

            //System.out.println("\nSST Value: " + SST);
            R_2_2 = SSR/SST;

            //System.out.println("\nR^2 Value: " + R_2);
            //System.out.println("\nR^2_2 Value: " + R_2_2);
            int index = 0;
            double R_2_tmp;
            while(index < M)
//            for (int index = 0; index < M; ++index)
            {
                R_2_tmp = lookingOtherParameter(x,c,setupParameterBindex(Parameter[index]));
//                System.out.println(" and the ErrorSquare is: " + estimateErrorSquare(x,c,setupParameterBindex(Parameter[index])) + " and the R^2 is: " + R_2_tmp);
                if (( R_2_limit < R_2_tmp)&&(R_2_tmp < 1)&&(R_2_2<R_2_tmp))
                {
                    //System.out.println("\nR^2_2 Value: " + R_2);
                    //System.out.println("\nR^2_2 Value: " + R_2_2);
//                    R_2_2 = R_2_tmp;
                    //System.out.println("\nR^2_2 Repair: " + R_2_tmp);
                    //System.out.println("\nR^2 Value with Parameter["+index+"]:" + R_2_tmp);//Double.toString(lookingOtherParameter(x,c,setupParameterBindex(Parameter[index]))));
                    index = M;
                }
                index++;
            }
            if (M < MaxOfLine) sizeOfMatrix = M;
            M = M+1;
        }
        //System.out.println("\nR^2 Value: " + R_2);
        //System.out.println("\nR^2_2 Value: " + R_2_2);
        //System.out.println("\nR^2 Value Limit: " + R_2_limit);
        //System.out.println("\nSize of real Value: " + sizeOfMatrix);
        //System.out.println("\nEstimate the maximum of Matrix:------------------------------------------------------------------------ ");
        return sizeOfMatrix;
    }
    public static double estimateCostValue(int sizeOfValue, String file, double[] X, double R_2_limit) throws IOException{
        int M = sizeOfValue;
        String fileParameter = fileParameter(file);
        String fileValue = fileRealValue(file);
        int N = 1;
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
        B = multiply(multiply(invert(multiply(transpose(a),a)),transpose(a)),c);
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
    public static void setupFile(String file, int numberVariables){
        String fileRealValue = fileRealValue(file);
        String fileParameter = fileParameter(file);
        String fileEstimate = fileEstimateValue(file);
        List<Double> variables = new ArrayList<>();
        int numberParameter = numberVariables + 1;
        double[] parameters = new double[numberParameter];
        double value = 0;
        for (int j = 0; j < 3 * numberVariables; j++){
            variables.clear();
            value = 0;
            for (int i =0 ; i < numberVariables; i ++){
                double temp = 1000*1000*Math.random();
                variables.add(temp);
                parameters[i] = 1.0;
                value = value + temp;
            }
            double[] valueArray = setupValue(variables, value);
            parameters[parameters.length-1] = 1.0;
            try {
                Writematrix2CSV.addArray2Csv(file, valueArray);
                Writematrix2CSV.addArray2Csv(fileParameter, parameters);
                Writematrix2CSV.addArray2Csv(fileRealValue, valueArray);
                Writematrix2CSV.addArray2Csv(fileEstimate, valueArray);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static double[] setupValue(List<Double> size, double value){
        double[] Value = new double [size.size()+1];
        for (int i = 0; i < size.size(); i++)
            Value[i] = size.get(i);
        Value[size.size()] = value;
        return Value;
    }
    public static double[] setupValue(double[] size, double value){
        double[] Value = new double [size.length+1];
        for (int i = 0; i < size.length; i++)
            Value[i] = size[i];
        Value[size.length] = value;
        return Value;
    }
    public static void updateValue(String homeFolder, String logicalId, Scala.Cost cost, double durationInMs, String name) throws IOException {
        String file = "data/dream/" + homeFolder + "/" + logicalId + "/" + name + ".csv";
        String fileRealValue = fileRealValue(file);
        List<Double> variables = new ArrayList<>();
        variables.add(cost.card().toDouble());
        variables.add(cost.size().toDouble());
        double value = durationInMs;
        double[] valueArray = setupValue(variables, value);
        Writematrix2CSV.addArray2Csv(file, valueArray);
        Writematrix2CSV.addArray2Csv(fileRealValue, valueArray);
        System.out.println("Here is the execute time of: "+
                logicalId + " in "+ homeFolder + " := " +
                durationInMs + " cost.card:= " + cost.card().toDouble()+
                "cost.size:=" + cost.size().toDouble());
    }
    public static void setupFolder(String homeSetTable, String logicalId){
        String folder = "data/dream/" +homeSetTable + "/" + logicalId;
        File Dir = new File(folder);
        if (!Dir.exists()) {
            Dir.mkdirs();
        }
    }
}
