package Algorithms;

import static Algorithms.Algorithms.estimateSizeOfMatrix;
import static Algorithms.Algorithms.fileName;
import static Algorithms.Algorithms.fillone;
import static Algorithms.Algorithms.initParameterValue;
import static Algorithms.Algorithms.lookingOtherParameter;
import static Algorithms.Algorithms.setupMatrixA;
import static Algorithms.Algorithms.setupMatrixC;
import static Algorithms.Algorithms.setupMatrixX;
import static Algorithms.Algorithms.setupParameterB;
import static Algorithms.Algorithms.setupParameterBindex;
import static Algorithms.ReadMatrixCSV.readMatrix;
import static Algorithms.testScilab.invert;
import static Algorithms.testScilab.multiply;
import static Algorithms.testScilab.transpose;
import static IRES.TPCHQuery.Schema;
import static IRES.testQueryPlan.createRandomQuery;
import LibraryIres.Move_Data;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import weka.core.converters.*;
//import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.core.converters.ArffSaver;

import java.util.ArrayList;

import weka.classifiers.functions.LinearRegression;
import weka.core.Attribute;
//import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author letrung
 */
public class LinearRegressionManual {

    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";
    int int_localhost = 1323;
    String name_host = "localhost";
    String SPARK_HOME = new App().readhome("SPARK_HOME");
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String IRES_HOME = new App().readhome("IRES_HOME");
    String HDFS = new App().readhome("HDFS");
    String ASAP_HOME = IRES_HOME;
    String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    String directory_library = IRES_library+"/target/asapLibrary/";
    String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    String directory_datasets = IRES_library+"/target/asapLibrary/datasets/";
    String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    
    String[] start = new String[]{"/bin/sh", ASAP_HOME+"/start-ires.sh"};
    String[] stop = new String[]{"/bin/sh", ASAP_HOME+"/stop-ires.sh"};
    public static void testLinearRegression (String file_name) throws IOException{
        CSVLoader loader = new CSVLoader();
//        loader.setFile(new java.io.File(file_name));
//        Instances insts = loader.getDataSet();
//        System.out.println(insts.toString());
        loader.setSource(new File(file_name));
        Instances data = loader.getDataSet();
        System.out.println(data.toString());
    }
    public static void small_test(String training, String testing) throws IOException, Exception {
        DataSource source = new DataSource(training);              
        Instances instances = source.getDataSet();		
        instances.setClassIndex(instances.numAttributes() - 1);
        
        System.out.println("\nDataset:\n");
        System.out.println(instances);
                
        LinearRegression train = new LinearRegression();
        train.buildClassifier(instances);
        
        int numFeatures = instances.numAttributes() - 1;
        int numParameters = train.numParameters();
        
        System.out.println("\n Number of Parameters: " + numParameters);
        double[] coefficient = train.coefficients();
              
        int vectors = instances.numInstances();
        System.out.println("\n Number of Instances: " + vectors);
        
        double mse = 0;
	for (int j = 0; j < vectors; j++){
            double prediction = train.classifyInstance(instances.instance(j));
            System.out.println("Prediction of ["+j+"]:= "+prediction);
            mse += Math.pow(prediction - instances.instance(j).value(numFeatures),2);
	}	
        ////////////////////////
        System.out.println("\n Test coefficient at the first set: ");
        int k = 0;
        double predict2 = coefficient[coefficient.length-1];
        System.out.println("\n Predict of value [0]:= predict = " + predict2 + "+");
        for (int j = 0; j < coefficient.length-1; j++)
        {
            System.out.println(coefficient[j] + "*" + instances.instance(k).value(j)+ "+");  
            predict2 = predict2 + coefficient[j] * instances.instance(k).value(j);
        }           
        System.out.println("= " + predict2);
        /////////////////
        System.out.println("\n Array of coefficient: ");
        testScilab.printArray(coefficient);
	// Report recognition rate
	System.out.println("Mean square error (Should be about 0.5): " + (double)Math.sqrt(mse/vectors));       
    }
    public static void main_test(String training, String testing) throws IOException, Exception {
                CSVLoader loader = new CSVLoader();
//                loader.setFile(new java.io.File(file_name));
                loader.setSource(new File(training));
                Instances data = loader.getDataSet();
                System.out.println(data.toString());
		// a set of feature vectors is stored in an Instances object in weka.
		// in oder to create such an object, we first have to create a list of features called attributes in weks.
		// in this example we generate 10 random real valued features plus a real valued class attribute. 
		int numFeatures = 3;
		ArrayList<Attribute> attribs = new ArrayList<Attribute>(numFeatures+1);
                // generate 10 features and add them to the list of features.
		for (int i =0;i<numFeatures;i++){
			String nameString = "Feature " + i;
			attribs.add(new weka.core.Attribute(nameString));
		}
		// generate a real valued class attribute.
		Attribute classAttribute = new Attribute("Class");
		// add to the list of features.
		attribs.add(classAttribute);
		

		// create 10000 random training vectors
		int vectors = 100;
//		Instances trainingSet = Instances("Training Set", attribs, vectors);
                Instances trainingSet = new Instances(data);
/*		for (int j = 0; j < vectors; j++){
			double [] vector = new double [numFeatures+1];
			for (int i =0;i<numFeatures;i++){
				vector[i]=Math.random();
			}
			vector [numFeatures] = (int) (Math.random() *1.99);
			trainingSet.add(new Instance(1.0, vector));
		}
	
*/
                trainingSet.setClass(classAttribute);
                ///////////////////////////////////////////// 
                DataSource source = new DataSource(training);              
                Instances instances = source.getDataSet();		
                instances.setClassIndex(instances.numAttributes() - 1);
//                trainingSet.setClassIndex(1);
                LinearRegression train = new LinearRegression();
                train.buildClassifier(instances);
                System.out.println("\nDataset:\n");
                System.out.println(instances);
                int numParameters = train.numParameters();
                ///////////////////////////////////////////////        
                System.out.println("\n Number of Parameters: " + numParameters);
                
                double[] coefficient = train.coefficients();
                System.out.println("\n Array of coefficient: ");
                testScilab.printArray(coefficient);
                
                
                CSVLoader loaderTesting = new CSVLoader();
//                testing = training;
                loaderTesting.setSource(new File(testing));
                System.out.println("Testing OK at here");
                Instances dataTesting = loaderTesting.getDataSet();
                
                System.out.println(dataTesting.toString());
		// create 10000 random test vectors
		Instances testSet = new Instances(dataTesting);//"Test Set", attribs, vectors);
/*		for (int j = 0; j < vectors; j++){
			double [] vector = new double [numFeatures+1];
			for (int i =0;i<numFeatures;i++){
				vector[i]=Math.random();
			}
			vector [numFeatures] = (int) (Math.random() *1.99);
			testSet.add(new Instance(1.0, vector));
		}
*/
                testSet.setClass(classAttribute);
		try {
			// Train Classifier
			LinearRegression frf = new LinearRegression();
                        testSet.setClassIndex(testSet.numAttributes() - 1);
			frf.buildClassifier(testSet);
			
			// Evaluate Classifier:
			double mse = 0;
			for (int j = 0; j < vectors; j++){
				double prediction = frf.classifyInstance(testSet.instance(j));
                                System.out.println("Prediction of ["+j+"]:= "+prediction);
				mse += Math.pow(prediction - testSet.instance(j).value(numFeatures),2);
			}
			coefficient = frf.coefficients();
                        System.out.println("\n Array of coefficient: ");
                        testScilab.printArray(coefficient);
			// Report recognition rate
			System.out.println("Mean square error (Should be about 0.5): " + (double)Math.sqrt(mse/vectors));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
    public static void test_minDataset(String training, String testing, double R_2) throws IOException, Exception {
        int set_training = CsvFileReader.count(training)-1;
        int set_testing = CsvFileReader.count(testing)-1;       
        double [][] matrix_training = readMatrix(training, set_training);
        double [][] matrix_originTesting = readMatrix(testing, set_testing);     
        double [][] matrix_minsetTesting = readMatrix(testing, set_testing);
           
        CSVLoader loader = new CSVLoader();               
        loader.setSource(new File(training));     
        Instances data = loader.getDataSet();
        System.out.println(data.toString());
        Instances trainingSet = new Instances(data);
	/*Prepare for the temp file of training*/	
	int numFeatures = matrix_training[0].length - 1;
        double R_2_limit = R_2;
        int sizeOfValue = estimateSizeOfMatrix(set_training, numFeatures, training, R_2_limit);
        double[][] real_matrix_training = readMatrix(training, sizeOfValue);      
        storeMatrix(real_matrix_training,training.replace(".csv", "_temp.csv"));
        System.out.println("\nMatrix of training temp: \n");
        CSVLoader loadertrainingtemp = new CSVLoader();             
        loadertrainingtemp.setSource(new File(training.replace(".csv", "_temp.csv"))); 
        
        Instances datatemp = loadertrainingtemp.getDataSet();
        System.out.println(datatemp.toString());
        Instances trainingSetTemp = new Instances(datatemp);
        
	ArrayList<Attribute> attribs = new ArrayList<Attribute>(numFeatures+1);
        // generate 10 features and add them to the list of features.
        for (int i =0;i<numFeatures;i++){
                String nameString = "Feature " + i;
                attribs.add(new weka.core.Attribute(nameString));
        }
        // generate a real valued class attribute.
        Attribute classAttribute = new Attribute("Class");
        // add to the list of features.
        attribs.add(classAttribute);

//              Training for the origin traning set
        LinearRegression train = new LinearRegression();
        trainingSet.setClassIndex(trainingSet.numAttributes()-1);
        train.buildClassifier(trainingSet);
        double[] coefficient = train.coefficients();
        System.out.println("\n Array of coefficient in origin training: ");
        testScilab.printArray(coefficient);
//              Training for the temp of traning set
        LinearRegression trainTemp = new LinearRegression();
        trainingSetTemp.setClassIndex(trainingSetTemp.numAttributes()-1);
        trainTemp.buildClassifier(trainingSetTemp);
        double[] coefficient_temp = trainTemp.coefficients();
        System.out.println("\n Array of coefficient in training temp: ");
        testScilab.printArray(coefficient_temp);
//              Prepare for the testing                
        System.out.println("\nMatrix of testing: \n");
        CSVLoader loaderTesting = new CSVLoader();
        loaderTesting.setSource(new File(testing));
        Instances dataTesting = loaderTesting.getDataSet();               
        System.out.println(dataTesting.toString());
        // create 10000 random test vectors
        Instances testSet = new Instances(dataTesting);//"Test Set", attribs, vectors);
//      Prepare for the testing value and coefficient of traning temp
        int vectors = testSet.numInstances();
        testSet.setClass(classAttribute);
        try {
            // Train Classifier
            LinearRegression frf = new LinearRegression();
            trainingSet.setClassIndex(trainingSet.numAttributes()-1);
            frf.buildClassifier(trainingSet);

            // Evaluate Classifier:
            double mse = 0;
            for (int j = 0; j < vectors; j++){
                    double prediction = frf.classifyInstance(testSet.instance(j));
                    System.out.println("Prediction of ["+j+"]:= "+prediction);
                    matrix_originTesting[j][matrix_originTesting[j].length-1] = prediction;
                    mse += Math.pow(prediction - testSet.instance(j).value(numFeatures),2);
            }
            System.out.println("Mean square error (Should be about 0.5): " + (double)Math.sqrt(mse/vectors));
            storeMatrix(matrix_originTesting, testing.replace(".csv", "_origin.csv"));
            // For the train temp dataset
            trainingSetTemp.setClassIndex(trainingSetTemp.numAttributes()-1);
            frf.buildClassifier(trainingSetTemp);

            // Evaluate Classifier:
            mse = 0;
            for (int j = 0; j < vectors; j++){
                    double prediction = frf.classifyInstance(testSet.instance(j));
                    System.out.println("Prediction of ["+j+"]:= "+prediction);
                    matrix_minsetTesting[j][matrix_minsetTesting[j].length-1] = prediction;
                    mse += Math.pow(prediction - testSet.instance(j).value(numFeatures),2);
            }
            // Report recognition rate
            System.out.println("Mean square error (Should be about 0.5): " + (double)Math.sqrt(mse/vectors));
            storeMatrix(matrix_minsetTesting, testing.replace(".csv", "_minset.csv"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		
	}
    public static void TPCH(double TimeOfDay, String DB, String Size, String from, String to, String KindOfRunning) throws IOException, Exception {
        String delay_ys = "";
        double R2 = 0.8;
        ////////////////////////////////////////////
        String Operator = "TPCH_"+ Size;   
        String DataIn = "";
        String DataInSize = "";       
        String DatabaseIn = "";
        String Schema = "";
        String DataOut = "";
        String DatabaseOut = "";  
        Move_Data Data = new Move_Data(Operator, DataIn, DataInSize, DatabaseIn, Schema, from, to, DataOut, DatabaseOut);
        Data.set_Operator(Operator);
        Data.set_DataIn(DataIn);
        Data.set_DataInSize(DataInSize);
        Data.set_DataOut(DataOut);
        Data.set_DatabaseIn(DatabaseIn);
        Data.set_DatabaseOut(DatabaseOut);
        Data.set_From(from);
        Data.set_To(to);
        Data.set_Schema(Schema);
        String directory = testWriteMatrix2CSV.getDirectory(Data);
        //////////////////////////////////////////
        if (TimeOfDay<1) delay_ys = "no_delay";
        String training = fileName(directory,delay_ys,"realValue","training");
        String testing = fileName(directory,delay_ys,"realValue","testing");
        
        test_minDataset(training, testing, R2);
		
    }
    public static int estimateSizeOfMatrix(int Max_Line, int numberOfVariable, String fileLink, double R_2_limit) throws IOException {
        String fileRealValue = fileLink;
        int MaxOfLine = Max_Line;       
        System.out.println("\nMax Of Line is:"+MaxOfLine);               
        int M = numberOfVariable + 2; // At least the number of equation = number of Variable + 2
        double R_2 = 0;
        double R_2_2 = 0;       
        double[][] realValue;

        double[][] x;
        double[][] a;
        double[] B;
        double[] b;
        double[] c;// = new double[realValue.length];
        double[] d;// = new double[realValue.length];
        
        int sizeOfMatrix = Max_Line;
        boolean check = false;
        while(((Math.abs(R_2_2)< R_2_limit)||(Math.abs(R_2_2)> 1)||check)&&(M < MaxOfLine))
        {            
            realValue = readMatrix(fileRealValue, M);
            x = new double[realValue.length][realValue[0].length - 1];
            a = new double[realValue.length][realValue[0].length];
            B = new double[realValue[0].length];

            c = new double[realValue.length];
            d = new double[realValue.length];

            x = setupMatrixX(realValue);
            a = setupMatrixA(x);

            c = setupMatrixC(realValue);
            d = setupMatrixC(realValue);
            //System.out.println("length of C: "+c.length);
            B = multiply(multiply(invert(multiply(transpose(a),a)),transpose(a)),c);
    
            for (int i = 0; i < d.length; i++)
            {
                d[i] = 0;
                for (int j = 0; j < a[0].length; j++)
                    d[i] = d[i]+a[i][j]*B[j]; 
            }

          
            double average = 0;
            for (int i = 0; i < c.length; i++)
                average = average + c[i];
            average = average/c.length;

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
            if (M < MaxOfLine) sizeOfMatrix = M; 
//            check = checkParameter(B);
            if ((R_2_2 > R_2_limit)&&(R_2_2 < 1)&&check) sizeOfMatrix = M; 
            M = M+1;
        }
        System.out.println("\nR^2 Value: " + R_2);
        System.out.println("\nR^2_2 Value: " + R_2_2);
        System.out.println("\nR^2 Value Limit: " + R_2_limit);
        System.out.println("\nSize of real Value: " + sizeOfMatrix);
        System.out.println("\nEstimate the maximum of Matrix:------------------------------------------------------------------------ ");
        return sizeOfMatrix;
    }
    public static boolean checkParameter (double [] Parameter){
        for (int i=0; i < Parameter.length; i++)
            if (Parameter[i] < 0) return true;
        return false;
    }
    public static void storeMatrix(double [][] Matrix, String fileLink) throws IOException{
        String fileName = fileLink;
        Path filePath = Paths.get(fileName);
        Files.deleteIfExists(filePath);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);                
            String FILE_HEADER = "";
            for (int i= 0; i < Matrix[0].length-1; i++)
            FILE_HEADER = FILE_HEADER + "Variable["+i+"]" + COMMA_DELIMITER;
            int last = Matrix[0].length-1;
            FILE_HEADER = FILE_HEADER + "CostValue["+ last +"]";
            Writematrix2CSV.Writematrix2CSV(Matrix, fileName, FILE_HEADER);
            }
        else {
            String add = "";
        int i = 0;            
        for (i = 0; Matrix[0].length - 1 > i; i++)
        add = add + Matrix[0][i] + COMMA_DELIMITER;
        if (Matrix[0].length - 1 == i)
        add = add + Matrix[0][i] + NEW_LINE_SEPARATOR;
        Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
        }
    }    
}    

