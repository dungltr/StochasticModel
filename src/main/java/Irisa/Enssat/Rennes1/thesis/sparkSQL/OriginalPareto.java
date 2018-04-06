package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import Scala.OriginalCost;
import Scala.MultipleCost;
import Scala.TestCostBasedJoinReorder$;
import Scala.TestOriginalCostBasedJoinReorder;
import Scala.TestOriginalCostBasedJoinReorder$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Int;
import scala.collection.immutable.List;

import java.util.ArrayList;

/**
 * Created by letrungdung on 04/04/2018.
 */
public class OriginalPareto {
    static  java.util.List<LogicalPlan> logicalPlansList = new ArrayList<>();
    static  java.util.List<OriginalCost> costPlansList = new ArrayList<>();
    static  java.util.List<List<Int>> setPlansList = new ArrayList<>();
    public static void addLogicalPlan (LogicalPlan LogicalPlan) {
        logicalPlansList.add(LogicalPlan);
    }
    public static void addCostPlan (OriginalCost cost) {
        costPlansList.add(cost);
    }
    public static void addSetPlan (List<Int> list) {
        setPlansList.add(list);
    }
    public static void filterPlans (){
        java.util.List<LogicalPlan> finaLogicalPlansList = new ArrayList<>();
        java.util.List<OriginalCost> finalCostPlansList = new ArrayList<>();
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
        java.util.List<OriginalCost> finalCostPlansList = new ArrayList<>();
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
    public static  java.util.List<OriginalCost> setCosts(int sizeItems) {
        return filterCosts(costPlansList,sizeItems);
    }
    public static  java.util.List<List<Int>> setList(int sizeItems) {
        return filterSets(setPlansList,sizeItems);
    }
    public static void test(){
        System.out.println("These are logical plans in Spark Optimize processing set --------------");
        printAllLogicalPlans(logicalPlansList,costPlansList,setPlansList);
        TestOriginalCostBasedJoinReorder$.MODULE$.testBigTable();
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<OriginalCost> finalCostPlansList = filterCosts(costPlansList);
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
                                            java.util.List<OriginalCost> finalCostPlansList ,
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
            /*
            if (setPlansList.get(i).size()>=sizeItems){
                finalSetPlansList.add(setPlansList.get(i));
            }
            */
        }/*
        for (int i = 0; i < logicalPlansList.size(); i++){
            if (setPlansList.get(i).size()>=sizeItems){
                finaLogicalPlansList.add(logicalPlansList.get(i));
            }
        }
        */
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
    public static java.util.List<OriginalCost> filterCosts (java.util.List<OriginalCost> costPlansList, int sizeItems){
        java.util.List<OriginalCost> finalCostPlansList = new ArrayList<>();
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
            /*
            if (setPlansList.get(i).size()>=sizeItems){
                finalSetPlansList.add(setPlansList.get(i));
            }
            */
        }/*
        for (int i = 0; i < logicalPlansList.size(); i++){
            if (setPlansList.get(i).size()>= sizeItems){
                finalCostPlansList.add(costPlansList.get(i));
            }
        }
        */
        return finalCostPlansList;
    }
    public static java.util.List<OriginalCost> filterCosts (java.util.List<OriginalCost> costPlansList){
        java.util.List<OriginalCost> finalCostPlansList = new ArrayList<>();
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
            /*
            if (setPlansList.get(i).size()>=sizeItems){
                finalSetPlansList.add(setPlansList.get(i));
            }
            */
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
}
