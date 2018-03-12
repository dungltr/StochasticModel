package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import Scala.TestCostBasedJoinReorder$;
import Scala.Cost;
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
        System.out.println("These are logical plans in Pareto set");
        for (int i = 0; i < finaLogicalPlansList.size(); i++){
            System.out.println(finaLogicalPlansList.get(i));
            System.out.println(finalCostPlansList.get(i));
            System.out.println(finalSetPlansList.get(i));
        }
        String logical = finaLogicalPlansList.get(0).toString();
        System.out.println("End of showing logical plans in Pareto set");
        System.out.println(logical);
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
    public static void test(){
        TestCostBasedJoinReorder$.MODULE$.testQuery();
        java.util.List<LogicalPlan> finaLogicalPlansList = filterPlans(logicalPlansList);
        java.util.List<Cost> finalCostPlansList = filterCosts(costPlansList);
        java.util.List<List<Int>> finalSetPlansList = filterSets(setPlansList);
        printAllLogicalPlans(finaLogicalPlansList,finalCostPlansList,finalSetPlansList);
    }
    public static void main(String[] args){
        test();
    }
}
