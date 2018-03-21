package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import Algorithms.Writematrix2CSV;
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
import java.util.List;

/**
 * Created by letrungdung on 20/03/2018.
 */
public class historicData {
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
                    tempList.get(i).card().toLong(),
                    tempList.get(i).size().toLong());
            temp = new Cost(temp.card(),temp.size(),executeTime,temp.moneytary());
            System.out.println(temp);
            List.set(i,temp);
        }
        for (int i = 0; i < tempList.size(); i++){
            System.out.println(tempList.get(i).card());
            System.out.println(tempList.get(i).size());
            System.out.println("///////////////////////");
        }
        return List;
    }
    public static double dreamValue(String homeSetTable, String logicalId, String nameValue, long cardinality, long size){
        //String folder = "data/dream/" +homeSetTable + "/" + logicalId;
        int numberVariables = 2;
        String folder = "data/dream/" +idQuery + "/" + logicalId;
        String file = folder + "/" + nameValue + ".csv";
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
            else return 1;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }
    public static void setupFile(String file, int numberVariables){
        List<Double> variables = new ArrayList<>();
        int numberParameter = numberVariables + 1;
        double value = 0;
        for (int j = 0; j < 2 * numberVariables; j++){
            variables.clear();
            for (int i =0 ; i < numberParameter; i ++){
                double temp = 1000*1000*Math.random();
                variables.add(temp);
            }
            value = 1000*Math.random();
            double[] array = setupValue(variables, value);
            try {
                Writematrix2CSV.addArray2Csv(file, array);
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
    public static void setupFolder(String homeSetTable, String logicalId){
        String folder = "data/dream/" +homeSetTable + "/" + logicalId;
        File Dir = new File(folder);
        if (!Dir.exists()) {
            Dir.mkdirs();
        }
    }
}
