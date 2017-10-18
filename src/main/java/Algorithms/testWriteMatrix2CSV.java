/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

import static Algorithms.Algorithms.initParamter;
import LibraryIres.Move_Data;
import com.sparkexample.App;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 *
 * @author letrung
 */
public class testWriteMatrix2CSV {
    	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
    public static void main() throws IOException {
                String fileName = System.getProperty("user.home")+"/matrix.csv";
                
		double[][] a = { { 1, 2, 3 }, { 4, 5, 6 }, { 9, 1, 3} };
                double[] tmp_Array = { 7, 8, 9 };
                double[][] b = { { 11, 12, 13 }, { 14, 15, 16 }, { 19, 11, 13} };
                System.out.println("Write CSV file:");
                int n = a[0].length;
                int i = 0;
                String tmp = "";
                for (i = 0; n -1 >i; i++)
                tmp = tmp + "b[" + i + "]" + COMMA_DELIMITER;
                if (n - 1 == i) tmp = tmp + "b[" + i + "]";
                String FILE_HEADER = tmp;
		Writematrix2CSV.Writematrix2CSV(a, fileName, FILE_HEADER);
                ReadMatrixCSV.readMatrixCsvFile(fileName);
                System.out.println("Add Array to CSV file:");
                Writematrix2CSV.addArray2Csv(fileName, tmp_Array);
                ReadMatrixCSV.readMatrixCsvFile(fileName);
                System.out.println("Add Matrix to CSV file:");
                Writematrix2CSV.addMatrix2Csv(fileName, b);
                ReadMatrixCSV.readMatrixCsvFile(fileName);
                System.out.println("Read last line in CSV file:");
    }
    public static void store(Move_Data Data, String SQL, double Data_size, double Ram, double Core, double TimeCost, String realValue) throws IOException {
        String fileName = realValue;
        double[][] a = new double[1][4];
        a[0][0] = Data_size;
        a[0][1] = Ram;
        a[0][2] = Core;
        a[0][3] = TimeCost;
        Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
                String FILE_HEADER = "";
                FILE_HEADER = FILE_HEADER + "Line" + COMMA_DELIMITER + 
                        "RamGB" + COMMA_DELIMITER + 
                        "Core" + COMMA_DELIMITER + 
                        "TimeInDay" + COMMA_DELIMITER +
                        "Value";
                Writematrix2CSV.Writematrix2CSV(a, fileName, FILE_HEADER);
                }
            else {
                String add = "";
            int i = 0;
            for (i = 0; a[0].length - 1 > i; i++)
            add = add + a[0][i] + COMMA_DELIMITER;
            if (a[0].length - 1 == i)
            add = add + a[0][i] + NEW_LINE_SEPARATOR;
            Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
            }
    }
    public static void storeValue(Move_Data Data, String SQL, double[] Value, String NameValue) throws IOException {
        String directory = testWriteMatrix2CSV.getDirectory(Data);
        directory = Algorithms.preapreFile(directory);
        String fileName = directory + "/" + NameValue + ".csv";
        double[][] a = new double[1][Value.length];
        for (int i = 0; i < Value.length; i++)
            a[0][i] = Value[i];
        Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);                
		String FILE_HEADER = "";
                for (int i= 0; i < Value.length-1; i++)
                FILE_HEADER = FILE_HEADER + "Variable["+i+"]" + COMMA_DELIMITER;
                int last = Value.length-1;
                FILE_HEADER = FILE_HEADER + NameValue;
                Writematrix2CSV.Writematrix2CSV(a, fileName, FILE_HEADER);
                }
            else {
                String add = "";
            int i = 0;
            for (i = 0; a[0].length - 1 > i; i++)
            add = add + a[0][i] + COMMA_DELIMITER;
            if (a[0].length - 1 == i)
            add = add + a[0][i] + NEW_LINE_SEPARATOR;
            Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
            }
    }
    public static void storeParameter(Move_Data Data, double[] Parameter, String fileParameter) throws IOException {
        String directory = testWriteMatrix2CSV.getDirectory(Data);
        directory = Algorithms.preapreFile(directory);
        String fileName = directory + "/" + fileParameter + ".csv";
        double[][] a = new double[1][Parameter.length];
        for (int i = 0; i < Parameter.length; i++)
            a[0][i] = Parameter[i];
        Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);                
                String FILE_HEADER = "";
                for (int i= 0; i < Parameter.length-1; i++)
                FILE_HEADER = FILE_HEADER + "Beta["+i+"]" + COMMA_DELIMITER;
                int last = Parameter.length-1;
                FILE_HEADER = FILE_HEADER + "Beta["+ last +"]";
                Writematrix2CSV.Writematrix2CSV(a, fileName, FILE_HEADER);
                }
            else {
                String add = "";
            int i = 0;
            for (i = 0; a[0].length - 1 > i; i++)
            add = add + a[0][i] + COMMA_DELIMITER;
            if (a[0].length - 1 == i)
            add = add + a[0][i] + NEW_LINE_SEPARATOR;
            Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
            }
    }
/*    public static void storeEstimateData(Move_Data Data, String SQL, double Data_size, double Ram, double Core, double TimeCost, String estimateNameValue) throws IOException {
        String directory = testWriteMatrix2CSV.getDirectory(Data);
        String fileName = directory + "/" + estimateNameValue + ".csv";
        double[][] a = new double[1][4];
        a[0][0] = Data_size;
        a[0][1] = Ram;
        a[0][2] = Core;
        a[0][3] = TimeCost;
        Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
                String FILE_HEADER = "";
                FILE_HEADER = FILE_HEADER + "Line" + COMMA_DELIMITER + 
                        "RamGB" + COMMA_DELIMITER + 
                        "Core" + COMMA_DELIMITER + 
                        "TimeInDay" + COMMA_DELIMITER +
                        estimateNameValue;
                Writematrix2CSV.Writematrix2CSV(a, fileName, FILE_HEADER);
                }
            else {
                String add = "";
            int i = 0;
            for (i = 0; a[0].length - 1 > i; i++)
            add = add + a[0][i] + COMMA_DELIMITER;
            if (a[0].length - 1 == i)
            add = add + a[0][i] + NEW_LINE_SEPARATOR;
            Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
            }
    }
*/    public static String getDirectory(Move_Data Data){
        String IRES_HOME = new App().readhome("IRES_HOME");
        String IRES_library = IRES_HOME+"/asap-platform/asap-server";
        String NameOp = Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To();
        String directory_operator = IRES_library+"/target/asapLibrary/operators/"+ NameOp + "/" ;
        return directory_operator;
    }

    public static void storeValueServer(Move_Data Data, String SQL, double[] Value, String NameValue) throws IOException {
        String directory = testWriteMatrix2CSV.getDirectory(Data);
        directory = Algorithms.preapreFile(directory);
        String fileName = directory + "/" + NameValue +".csv";
        double[][] a = new double[1][Value.length];
        for (int i = 0; i < Value.length; i++)
            a[0][i] = Value[i];
        Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
            }
            if (Files.size(filePath) == 0){
                System.out.println("\n File size = 0:-------------------------");
		String FILE_HEADER = "";
                for (int i= 0; i < Value.length-1; i++)
                FILE_HEADER = FILE_HEADER + "Variable["+i+"]" + COMMA_DELIMITER;
                int last = Value.length-1;
                FILE_HEADER = FILE_HEADER + NameValue;
                Writematrix2CSV.Writematrix2CSV(a, fileName, FILE_HEADER);
                }
            else {
                String add = "";
            int i = 0;
            for (i = 0; a[0].length - 1 > i; i++)
            add = add + a[0][i] + COMMA_DELIMITER;
            if (a[0].length - 1 == i)
            add = add + a[0][i] + NEW_LINE_SEPARATOR;
            Files.write(filePath, add.getBytes(), StandardOpenOption.APPEND);
            }
    }
}
    
