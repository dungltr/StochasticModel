package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import Scala.Cost;
import Scala.TestCostBasedJoinReorder$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Int;
import scala.collection.immutable.List;

import java.util.ArrayList;

/**
 * Created by letrungdung on 08/03/2018.
 */
public class Pareto {

    static  java.util.List<LogicalPlan> logicalPlansList = new ArrayList<>();
    static  java.util.List<Cost> costPlansList = new ArrayList<>();
    static  java.util.List<List<Int>> setPlansList = new ArrayList<>();

    public static void addLogicalPlan (LogicalPlan LogicalPlan) {
        logicalPlansList.add(LogicalPlan);
    }
    public static void addCostPlan (Cost cost) {
        costPlansList.add(cost);
    }
    public static void addSetPlan (List<Int> list) {
        setPlansList.add(list);
    }
    public static void filterPlans (){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        java.util.List<Cost> finalCostPlansList = new ArrayList<>();
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
        /*
        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finaLogicalPlansList.size(); i++){
            System.out.println(finaLogicalPlansList.get(i));
            System.out.println(finalCostPlansList.get(i));
            System.out.println(finalSetPlansList.get(i));
        }
        System.out.println("End of showing logical plans in Pareto set");
        */
    }
    public static void printAllLogicalPlans(java.util.List<LogicalPlan> finaLogicalPlansList,
                                            java.util.List<Cost> finalCostPlansList ,
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
    public static void printLogicalPlans(java.util.List<LogicalPlan> finaLogicalPlansList){
        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finaLogicalPlansList.size(); i++){
            System.out.println(finaLogicalPlansList.get(i));
        }
    }
    public static void printCostPlans(java.util.List<Cost> finalCostPlansList){
        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finalCostPlansList.size(); i++){
            System.out.println(finalCostPlansList.get(i));
        }
    }
    public static void printSetPlans(java.util.List<List<Int>> finalSetPlansList){
        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finalSetPlansList.size(); i++){
            System.out.println(finalSetPlansList.get(i));
        }
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
    public static java.util.List<Cost> filterCosts (java.util.List<Cost> costPlansList){
        java.util.List<Cost> finalCostPlansList = new ArrayList<>();
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
    public static void printFront (java.util.List<Integer> currentFront){
        for (int i = 0; i< currentFront.size(); i++)
            System.out.println(currentFront.get(i));

    }

    public static java.util.List<LogicalPlan> paretoPlans (java.util.List<LogicalPlan> finaLogicalPlansList,
                                                           java.util.List<Integer> currentFront){
        java.util.List<LogicalPlan> Plans = new ArrayList<>();
        for (int i = 0; i < currentFront.size(); i++){
            Plans.add(finaLogicalPlansList.get(currentFront.get(i)));
        }
        return Plans;
    }
    public static java.util.List<Cost> paretoCost (java.util.List<Cost> finalCostPlansList,
                                                           java.util.List<Integer> currentFront){
        java.util.List<Cost> Plans = new ArrayList<>();
        for (int i = 0; i < currentFront.size(); i++){
            Plans.add(finalCostPlansList.get(currentFront.get(i)));
        }
        return Plans;
    }
    public static java.util.List<List<Int>> paretoSet (java.util.List<List<Int>> finalSetPlansList,
                                                   java.util.List<Integer> currentFront){
        java.util.List<List<Int>> Plans = new ArrayList<>();
        for (int i = 0; i < currentFront.size(); i++){
            Plans.add(finalSetPlansList.get(currentFront.get(i)));
        }
        return Plans;
    }

    public static int compare(Cost costA, Cost costB){
        boolean dominate1 = false;
        boolean dominate2 = false;
        if (costA.size().doubleValue() < costB.size().doubleValue()) {
            dominate1 = true;
            if (dominate2) {
                return 0;
            }
        } else{
            if (costB.size().doubleValue() > costA.size().doubleValue()) {
                dominate2 = true;
                if (dominate1) {
                    return 0;
                }
            }
        }
        if (costA.card().doubleValue() < costB.card().doubleValue()) {
            dominate1 = true;
            if (dominate2) {
                return 0;
            }
        } else{
            if (costB.card().doubleValue() > costA.card().doubleValue()) {
                dominate2 = true;
                if (dominate1) {
                    return 0;
                }
            }
        }
        if (dominate1 == dominate2) {
            return 0;
        } else if (dominate1) {
            return -1;
        } else {
            return 1;
        }
    }
    public static int betterThan(Cost costA, Cost costB){
        //double cardA = (double) costA.card().doubleValue();
        /*
        if ((costA.card().doubleValue() < costB.card().doubleValue())
                &&(costA.size().doubleValue() < costB.size().doubleValue())){
            return -1;
        }
        else{
            return false;
        }
        */
        if (costA.card().doubleValue() < costB.card().doubleValue()) {
            return -1;
        } else {
            if (costA.card().doubleValue() > costB.card().doubleValue()) {
                return 1;
            } else {
                return 0;
            }
        }

    }
    public static java.util.List<Integer> Front (java.util.List<Cost> finalCostPlansList){
        int N = finalCostPlansList.size();
        int[][] dominanceChecks = new int[N][N];
        for (int i = 0; i < N; i++) {
            Cost si = finalCostPlansList.get(i);
            for (int j = i+1; j < N; j++) {
                if (i != j) {
                    Cost sj = finalCostPlansList.get(j);
                    dominanceChecks[i][j] = compare(si, sj);
                    dominanceChecks[j][i] = -dominanceChecks[i][j];
                }
            }
        }

        int[] dominatedCounts = new int[N];
        java.util.List<java.util.List<Integer>> dominatesList = new ArrayList<java.util.List<Integer>>();
        java.util.List<Integer> currentFront = new ArrayList<Integer>();


        for (int i = 0; i < N; i++) {
            java.util.List<Integer> dominates = new ArrayList<Integer>();
            int dominatedCount = 0;

            for (int j = 0; j < N; j++) {
                if (i != j) {
                    if (dominanceChecks[i][j] < 0) {
                        dominates.add(j);
                    } else if (dominanceChecks[j][i] < 0) {
                        dominatedCount += 1;
                    }
                }
            }

            if (dominatedCount == 0) {
                currentFront.add(i);
            }

            dominatesList.add(dominates);
            dominatedCounts[i] = dominatedCount;
        }

        return currentFront;
    }
    public static void test(){
        System.out.println("These are logical plans in Spark Optimize processing set --------------");
        TestCostBasedJoinReorder$.MODULE$.testBigTable();
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<Cost> finalCostPlansList = filterCosts(costPlansList);
        java.util.List<List<Int>> finalSetPlansList = filterSets(setPlansList);

        System.out.println("Before update cost Value");
        printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);
        finalCostPlansList = updateCostValue(finalCostPlansList,finalSetPlansList);

        System.out.println("After update cost Value");
        printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);

        System.out.println("End of showing logical plans in Spark Optimize processing**************");
        java.util.List<Integer> currentFront = Front(finalCostPlansList);
        //printFront(currentFront);
        System.out.println("These are logical plans in Pareto Optimize processing set ----------");
        java.util.List<LogicalPlan> finaParetoPlans = paretoPlans(finaLogicalPlansList,currentFront);
        java.util.List<Cost> finalParetoCost = paretoCost(finalCostPlansList,currentFront);
        java.util.List<List<Int>> finalParetoSet = paretoSet(finalSetPlansList,currentFront);
        //printAllLogicalPlans(finaParetoPlans,finalParetoCost,finalParetoSet);
        System.out.println("End of showing logical plans in Pareto set ****************************");

    }
    /*
    public static void updateCostValue(java.util.List<Cost> finalCostPlansList){
        java.util.List<Cost> tempList = historicData.dreamValue(finalCostPlansList);
        finalCostPlansList = tempList;
    }
    */
    public static java.util.List<Cost> updateCostValue(java.util.List<Cost> finalCostPlansList, java.util.List<List<Int>> finalSetPlansList){
        java.util.List<Cost> tempList = historicData.dreamValue(finalCostPlansList, finalSetPlansList);
        return tempList;
    }
    public static void main(String[] args){
        /*
        long start = System.nanoTime();
        int N = 10;
        for (int i = 0; i < N; i++) {
            logicalPlansList.clear();
            costPlansList.clear();
            setPlansList.clear();
            */
            test();
            /*
        }
        long stop = System.nanoTime();
        long time = (stop - start)/(1000*1000);
        System.out.println("Time for "+N+" timers is "+time + " micro seconds");
        */
    }
    public static java.util.List<LogicalPlan> finaParetoPlans(){
        //System.out.println("These are logical plans in Spark Optimize processing set --------------");
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<Cost> finalCostPlansList = filterCosts(costPlansList);
        java.util.List<List<Int>> finalSetPlansList = filterSets(setPlansList);
        //printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);
        //System.out.println("End of showing logical plans in Spark Optimize processing**************");
        java.util.List<Integer> currentFront = Front(finalCostPlansList);
        //printFront(currentFront);
        //System.out.println("These are logical plans in Pareto Optimize processing set ----------");
        java.util.List<LogicalPlan> finaParetoPlans = paretoPlans(finaLogicalPlansList,currentFront);
        java.util.List<Cost> finalParetoCost = paretoCost(finalCostPlansList,currentFront);
        java.util.List<List<Int>> finalParetoSet = paretoSet(finalSetPlansList,currentFront);
        //printAllLogicalPlans(finaParetoPlans,finalParetoCost,finalParetoSet);
        //System.out.println("End of showing logical plans in Pareto set ****************************");
        return finaParetoPlans;
    }
    public static java.util.List<Cost> finaCostPlans(){
        //System.out.println("These are logical plans in Spark Optimize processing set --------------");
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<Cost> finalCostPlansList = filterCosts(costPlansList);
        java.util.List<List<Int>> finalSetPlansList = filterSets(setPlansList);
        //printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);
        //System.out.println("End of showing logical plans in Spark Optimize processing**************");
        java.util.List<Integer> currentFront = Front(finalCostPlansList);
        //printFront(currentFront);
        //System.out.println("These are logical plans in Pareto Optimize processing set ----------");
        java.util.List<LogicalPlan> finaParetoPlans = paretoPlans(finaLogicalPlansList,currentFront);
        java.util.List<Cost> finalParetoCost = paretoCost(finalCostPlansList,currentFront);
        java.util.List<List<Int>> finalParetoSet = paretoSet(finalSetPlansList,currentFront);
        //printAllLogicalPlans(finaParetoPlans,finalParetoCost,finalParetoSet);
        //System.out.println("End of showing logical plans in Pareto set ****************************");
        return finalParetoCost;
    }
    public static java.util.List<LogicalPlan> setLogicalPlans() {
        return filterPlans(logicalPlansList);
    }
    public static  java.util.List<Cost> setCosts() {
        return filterCosts(costPlansList);
    }
    public static  java.util.List<List<Int>> setList() {
        return filterSets(setPlansList);
    }
    public static java.util.List<List<Int>> finaSetPlans(){
        //System.out.println("These are logical plans in Spark Optimize processing set --------------");
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<Cost> finalCostPlansList = filterCosts(costPlansList);
        java.util.List<List<Int>> finalSetPlansList = filterSets(setPlansList);
        //printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);
        //System.out.println("End of showing logical plans in Spark Optimize processing**************");
        java.util.List<Integer> currentFront = Front(finalCostPlansList);
        //printFront(currentFront);
        //System.out.println("These are logical plans in Pareto Optimize processing set ----------");
        java.util.List<LogicalPlan> finaParetoPlans = paretoPlans(finaLogicalPlansList,currentFront);
        java.util.List<Cost> finalParetoCost = paretoCost(finalCostPlansList,currentFront);
        java.util.List<List<Int>> finalParetoSet = paretoSet(finalSetPlansList,currentFront);
        //printAllLogicalPlans(finaParetoPlans,finalParetoCost,finalParetoSet);
        //System.out.println("End of showing logical plans in Pareto set ****************************");
        return finalParetoSet;
    }
    public static void setup(){

    }
}
