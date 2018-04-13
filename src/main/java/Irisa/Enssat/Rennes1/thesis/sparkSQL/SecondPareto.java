package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import Irisa.Enssat.Rennes1.TestMO;
import Scala.SecondCost;
import Scala.TestOriginalCostBasedJoinReorder$;
import com.sparkexample.App;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Int;
import scala.collection.Set;
import scala.collection.immutable.List;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Created by letrungdung on 04/04/2018.
 */
public class SecondPareto {
    static  java.util.List<LogicalPlan> logicalPlansList = new ArrayList<>();
    static  java.util.List<SecondCost> costPlansList = new ArrayList<>();
    static  java.util.List<List<Int>> setPlansList = new ArrayList<>();

    static  java.util.List<LogicalPlan> tempLogicalPlansList = new ArrayList<>();
    static  java.util.List<SecondCost> tempCostPlansList = new ArrayList<>();
    static  java.util.List<List<Int>> tempSetPlansList = new ArrayList<>();

    static  java.util.List<LogicalPlan> ParetoLogicalPlansList = new ArrayList<>();
    static  java.util.List<SecondCost> ParetoCostPlansList = new ArrayList<>();
    static  java.util.List<List<Int>> ParetoSetPlansList = new ArrayList<>();

    public static void addLogicalPlan (LogicalPlan LogicalPlan) {
        logicalPlansList.add(LogicalPlan);
    }
    public static void addCostPlan (SecondCost cost) {
        costPlansList.add(cost);
    }
    public static void addSetPlan (List<Int> list) {
        setPlansList.add(list);
    }

    public static void addParetoLogicalPlan (LogicalPlan LogicalPlan) {
        ParetoLogicalPlansList.add(LogicalPlan);
    }
    public static void addParetoCostPlan (SecondCost cost) {
        ParetoCostPlansList.add(cost);
    }
    public static void addParetoSetPlan (List<Int> list) {
        ParetoSetPlansList.add(list);
    }

    public static void filterPlans (){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        java.util.List<SecondCost> finalCostPlansList = new ArrayList<>();
        java.util.List<List<Int>> finalSetPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }

        for (int i = 0; i < logicalPlansList.size(); i++){
            if (setPlansList.get(i).size()==max){
                finaLogicalPlansList.add(logicalPlansList.get(i));
                finalCostPlansList.add(costPlansList.get(i));
                finalSetPlansList.add(setPlansList.get(i));
            }
        }

        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finaLogicalPlansList.size(); i++){
            System.out.println(finaLogicalPlansList.get(i));
            System.out.println(finalCostPlansList.get(i));
            System.out.println(finalSetPlansList.get(i));
        }
        System.out.println("End of showing logical plans in Pareto set");

    }
    public static void filterPlans (int sizeItems){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        java.util.List<SecondCost> finalCostPlansList = new ArrayList<>();
        java.util.List<List<Int>> finalSetPlansList = new ArrayList<>();
        int max = sizeItems;

        for (int i = 0; i < logicalPlansList.size(); i++){
            java.util.List<List<Int>> List = setPlansList;
            scala.collection.immutable.List<Int> tempList = List.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finaLogicalPlansList.add(logicalPlansList.get(i));
                    finalCostPlansList.add(costPlansList.get(i));
                    finalSetPlansList.add(setPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finaLogicalPlansList.add(logicalPlansList.get(i));
                    finalCostPlansList.add(costPlansList.get(i));
                    finalSetPlansList.add(setPlansList.get(i));
                }

            }
        }
        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finaLogicalPlansList.size(); i++){
            System.out.println(finaLogicalPlansList.get(i));
            System.out.println(finalCostPlansList.get(i));
            System.out.println(finalSetPlansList.get(i));
        }
        System.out.println("End of showing logical plans in Pareto set");

    }
    public static void main(String[] args){
        test();
    }
    public static java.util.List<LogicalPlan> setLogicalPlans(int sizeItems) {
        return filterPlans(logicalPlansList, sizeItems);
    }
    public static  java.util.List<SecondCost> setCosts(int sizeItems) {
        return filterCosts(costPlansList,sizeItems);
    }
    public static  java.util.List<List<Int>> setList(int sizeItems) {
        return filterSets(setPlansList,sizeItems);
    }

    public static java.util.List<LogicalPlan> setParetoLogicalPlans(int sizeItems) {
        return filterParetoPlans(ParetoLogicalPlansList, sizeItems);
    }
    public static  java.util.List<SecondCost> setParetoCosts(int sizeItems) {
        return filterParetoCosts(ParetoCostPlansList,sizeItems);
    }
    public static  java.util.List<List<Int>> setParetoList(int sizeItems) {
        return filterParetoSets(ParetoSetPlansList,sizeItems);
    }

    public static java.util.List<LogicalPlan> currentSetLogicalPlans() {
        return tempLogicalPlansList;
    }
    public static  java.util.List<SecondCost> currentSetCosts() {
        return tempCostPlansList;
    }
    public static  java.util.List<List<Int>> currentSetList() {
        return tempSetPlansList;
    }

    public static void test(){
        System.out.println("These are logical plans in Spark Optimize processing set --------------");
        printAllLogicalPlans(logicalPlansList,costPlansList,setPlansList);
        TestOriginalCostBasedJoinReorder$.MODULE$.testBigTable();
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<SecondCost> finalCostPlansList = filterCosts(costPlansList);
        java.util.List<List<Int>> finalSetPlansList = filterSets(setPlansList);

        //printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);

        System.out.println("Before update cost Value");
        printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);
        //finalCostPlansList = updateCostValue(finalCostPlansList,finalSetPlansList);

        System.out.println("After update cost Value");
        printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);

        System.out.println("End of showing logical plans in Spark Optimize processing**************");
        /*
        java.util.List<Integer> currentFront = Front(finalCostPlansList);
        //printFront(currentFront);
        System.out.println("These are logical plans in Pareto Optimize processing set ----------");
        java.util.List<LogicalPlan> finaParetoPlans = paretoPlans(finaLogicalPlansList,currentFront);
        java.util.List<MultipleCost> finalParetoCost = paretoCost(finalCostPlansList,currentFront);
        java.util.List<List<Int>> finalParetoSet = paretoSet(finalSetPlansList,currentFront);
        //printAllLogicalPlans(finaParetoPlans,finalParetoCost,finalParetoSet);
        System.out.println("End of showing logical plans in Pareto set ****************************");
        */
    }
    public static void printAllLogicalPlans(java.util.List<LogicalPlan> finaLogicalPlansList,
                                            java.util.List<SecondCost> finalCostPlansList ,
                                            java.util.List<List<Int>> finalSetPlansList ){

        for (int i = 0; i < finaLogicalPlansList.size(); i++){
            System.out.println("----");
            System.out.println("LogicalPlans ----  "+ i );
            System.out.println(finaLogicalPlansList.get(i));
            System.out.println(finalCostPlansList.get(i));
            System.out.println(finalSetPlansList.get(i));
        }
        //String logical = finaLogicalPlansList.get(0).toString();

        //System.out.println(logical);
    }
    public static java.util.List<LogicalPlan> filterParetoPlans (java.util.List<LogicalPlan> logicalPlansList, int sizeItems){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,ParetoSetPlansList.get(i).size());
        }
        max = sizeItems;
        for (int i = 0; i < logicalPlansList.size(); i++){
            List<Int> tempList = ParetoSetPlansList.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finaLogicalPlansList.add(logicalPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finaLogicalPlansList.add(logicalPlansList.get(i));
                }
            }

        }
        return finaLogicalPlansList;
    }
    public static java.util.List<LogicalPlan> filterPlans (java.util.List<LogicalPlan> logicalPlansList, int sizeItems){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }
        max = sizeItems;
        for (int i = 0; i < logicalPlansList.size(); i++){
            List<Int> tempList = setPlansList.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finaLogicalPlansList.add(logicalPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finaLogicalPlansList.add(logicalPlansList.get(i));
                }
            }

        }
        return finaLogicalPlansList;
    }
    public static java.util.List<LogicalPlan> filterPlans (java.util.List<LogicalPlan> logicalPlansList){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }

        for (int i = 0; i < logicalPlansList.size(); i++){
            if (setPlansList.get(i).size()==max){
                finaLogicalPlansList.add(logicalPlansList.get(i));
            }
        }
        return finaLogicalPlansList;
    }
    public static java.util.List<SecondCost> filterParetoCosts (java.util.List<SecondCost> costPlansList, int sizeItems){
        java.util.List<SecondCost> finalCostPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < costPlansList.size(); i++){
            max = Math.max(max,ParetoSetPlansList.get(i).size());
        }
        max = sizeItems;
        for (int i = 0; i < costPlansList.size(); i++){
            List<Int> tempList = ParetoSetPlansList.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finalCostPlansList.add(costPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finalCostPlansList.add(costPlansList.get(i));
                }
            }

        }
        return finalCostPlansList;
    }
    public static java.util.List<SecondCost> filterCosts (java.util.List<SecondCost> costPlansList, int sizeItems){
        java.util.List<SecondCost> finalCostPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }
        max = sizeItems;
        for (int i = 0; i < logicalPlansList.size(); i++){
            List<Int> tempList = setPlansList.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finalCostPlansList.add(costPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finalCostPlansList.add(costPlansList.get(i));
                }
            }

        }
        return finalCostPlansList;
    }
    public static java.util.List<SecondCost> filterCosts (java.util.List<SecondCost> costPlansList){
        java.util.List<SecondCost> finalCostPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }
        for (int i = 0; i < logicalPlansList.size(); i++){
            if (setPlansList.get(i).size()==max){
                finalCostPlansList.add(costPlansList.get(i));
            }
        }
        return finalCostPlansList;
    }
    public static java.util.List<List<Int>> filterParetoSets (java.util.List<List<Int>> setPlansList, int sizeItems){
        java.util.List<List<Int>> finalSetPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < setPlansList.size(); i++){
            max = Math.max(max,ParetoSetPlansList.get(i).size());
        }
        max = sizeItems;
        for (int i = 0; i < setPlansList.size(); i++){
            List<Int> tempList = ParetoSetPlansList.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finalSetPlansList.add(setPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finalSetPlansList.add(setPlansList.get(i));
                }
            }
        }
        return finalSetPlansList;
    }
    public static java.util.List<List<Int>> filterSets (java.util.List<List<Int>> setPlansList, int sizeItems){
        java.util.List<List<Int>> finalSetPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }
        max = sizeItems;
        for (int i = 0; i < logicalPlansList.size(); i++){
            List<Int> tempList = setPlansList.get(i);
            tempList.distinct();
            if (tempList.contains(-1)){
                if (tempList.size() == max + 1){
                    finalSetPlansList.add(setPlansList.get(i));
                }
            }
            else{
                if (tempList.size() == max){
                    finalSetPlansList.add(setPlansList.get(i));
                }
            }
        }
        return finalSetPlansList;
    }
    public static java.util.List<List<Int>> filterSets (java.util.List<List<Int>> setPlansList){
        java.util.List<List<Int>> finalSetPlansList = new ArrayList<>();
        int max = 0;
        for (int i = 0; i < logicalPlansList.size(); i++){
            max = Math.max(max,setPlansList.get(i).size());
        }
        for (int i = 0; i < logicalPlansList.size(); i++){
            if (setPlansList.get(i).size()==max){
                finalSetPlansList.add(setPlansList.get(i));
            }
        }
        return finalSetPlansList;
    }
    public static void preparingForMO(int sizeItems) throws IOException {
        java.util.List<List<Int>> filterSets = filterSets(setPlansList,sizeItems);
        java.util.List<SecondCost> filterCosts = filterCosts(costPlansList,sizeItems);
        java.util.List<LogicalPlan> filterPlans = filterPlans(logicalPlansList,sizeItems);

        preparingForMO(filterPlans, filterCosts, filterSets);

        String fileOldResult = ReadFile.readhome("MOEA_HOME") +"/Matrix.csv";
        String fileNewResult = ReadFile.readhome("MOEA_HOME") +"/Matrix_result.csv";
        String fileTotalResult = ReadFile.readhome("MOEA_HOME") +"/Matrix_total.csv";

        String folder = ReadFile.readhome("MOEA_HOME")+"/Matrix";

        File folderResult = new File(folder);
        File[] listOfMatrix = folderResult.listFiles();
        utilities.renewFile(fileTotalResult);
        for (int i=0; i < listOfMatrix.length; i++){
            double[][] oldMatrix = ReadMatrixCSV.readMatrix(listOfMatrix[i].toString(),CsvFileReader.count(listOfMatrix[i].toString()));
            utilities.renewFile(fileOldResult);
            Writematrix2CSV.addMatrix2Csv(fileOldResult,oldMatrix);
            TestMO.main(new String[] {"arg"});
            double[][] newMatrix = ReadMatrixCSV.readMatrix(fileNewResult,CsvFileReader.count(fileNewResult));
            Writematrix2CSV.addMatrix2CsvNoRenew(fileTotalResult,newMatrix);
            System.out.println(fileOldResult);
        }

        java.util.List<List<Int>> Sets = new ArrayList<>();
        java.util.List<SecondCost> Costs = new ArrayList<>();
        java.util.List<LogicalPlan> Plans = new ArrayList<>();
        double[][] matrixTotal = ReadMatrixCSV.readMatrix(fileTotalResult, CsvFileReader.count(fileTotalResult));
        for (int i = 0; i < filterPlans.size(); i ++){
            for (int j=0; j < matrixTotal.length; j++){
                if (i == matrixTotal[j][0]){
                    Plans.add(filterPlans.get(i));
                    Costs.add(filterCosts.get(i));
                    Sets.add(filterSets.get(i));
                }
            }
        }
        printAllLogicalPlans(filterPlans, filterCosts, filterSets);
        tempLogicalPlansList = Plans;
        tempCostPlansList = Costs;
        tempSetPlansList = Sets;
    }
    public static void preparingForMO(java.util.List<LogicalPlan> logicalPlan,
                                      java.util.List<SecondCost> costPlans,
                                      java.util.List<List<Int>> setPlans) throws IOException {
        String folderMatrix = ReadFile.readhome("MOEA_HOME") + "/Matrix";
        Path filePath = Paths.get(folderMatrix);
        if (Files.exists(filePath)) {
            //Files.delete(filePath);
            FileUtils.deleteDirectory(new File(folderMatrix));
        }
        Files.createDirectory(filePath);
        double[] Cost = new double[3];
        int i = 0;
        java.util.List<Set<Int>> set = new ArrayList<>();
        for (int j = 0; j < logicalPlan.size(); j++){
            List<Int> tempSet = setPlans.get(i);
            if (checkExit(set,tempSet.toSet())) tempSet = set.get(set.indexOf(tempSet.toSet())).toList();
            else
                set.add(tempSet.toSet());
            Cost[0] = Double.valueOf(i);
            Cost[1] = costPlans.get(j).size().toDouble();
            Cost[2] = costPlans.get(j).executeTime();
            Writematrix2CSV.addArray2Csv(ReadFile.readhome("MOEA_HOME") + "/Matrix/" + tempSet.toSet() + ".csv",Cost);
            i++;
        }
    }
    public static boolean checkExit(java.util.List<Set<Int>> listSet, Set<Int> set){
        return listSet.contains(set);
    }
}
