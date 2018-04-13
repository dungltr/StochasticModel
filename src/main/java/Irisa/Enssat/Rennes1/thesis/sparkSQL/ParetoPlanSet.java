package Irisa.Enssat.Rennes1.thesis.sparkSQL;


import Algorithms.Algorithms;
import Scala.SecondCost;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Int;
import scala.collection.immutable.List;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;


/**
 * Created by letrungdung on 07/03/2018.
 */
public class ParetoPlanSet {
    static  java.util.List<LogicalPlan> logicalPlansList = new ArrayList<>();
    static  java.util.List<SecondCost> costPlansList = new ArrayList<>();
    static  java.util.List<List<Int>> setPlansList = new ArrayList<>();

    static  java.util.List<LogicalPlan> ParetoLogicalPlansList = new ArrayList<>();
    static  java.util.List<SecondCost> ParetorCostPlansList = new ArrayList<>();
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


}
