/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Drap;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datanucleus.util.StringUtils;

/**
 *
 * @author letrung
 */
public class SparkControlPartitionSizeLocally {
    public static void main(String[] args) {
        JavaSparkContext sc = 
                new JavaSparkContext("local","localpartitionsizecontrol");
        String input = "four score and seven years ago our fathers "
                + "brought forth on this continent "
                + "a new nation conceived in liberty and "
                + "dedicated to the propostion that all men are created equal";
        List<String> lst = Arrays.asList(StringUtils.split(input, " "));

        for (int i = 1; i <= 30; i++) {
            JavaRDD<String> rdd = sc.parallelize(lst, i);
            System.out.println(rdd.partitions().size());
        }

    }
}
