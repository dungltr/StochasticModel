/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;
import com.sparkexample.App;
import org.moeaframework.Executor;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Solution;
/**
 *
 * @author letrung
 */
public class TestMOEA {
    static String MOEA_HOME = new App().readhome("MOEA_HOME");
    public static void main(String[] args) {
		//configure and run this experiment
		NondominatedPopulation result = new Executor()
				.withProblem(MOEA_HOME+"/UF1.dat")
				.withAlgorithm("NSGAII")
				.withMaxEvaluations(10000)
				.run();
		
		//display the results
		System.out.format("Objective1  Objective2%n");
		
		for (Solution solution : result) {
			System.out.format("%.4f      %.4f%n",
					solution.getObjective(0),
					solution.getObjective(1));
		}
	}
    
}
