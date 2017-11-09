/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

import com.sparkexample.App;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.moeaframework.Executor;
import org.moeaframework.analysis.plot.Plot;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Population;
import org.moeaframework.core.Problem;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.problem.CEC2009.UF1;
import org.moeaframework.util.ReferenceSetMerger;

/**
 *
 * @author letrungdung
 */
public class TestMOEA {
    static String MOEA_HOME = new App().readhome("MOEA_HOME");
    public static void main(String[] args) throws IOException {
		//configure and run this experiment
		NondominatedPopulation result = new Executor()
				.withAlgorithm("NSGAII")
				.withProblemClass(MyProblem.class)
				.withMaxEvaluations(10000)
				.run();
		
		for (Solution solution : result) {
			if (!solution.violatesConstraints()) {
				System.out.format("%10.3f      %10.3f%n",
						solution.getObjective(0),
						solution.getObjective(1));
			}
		}
                load();
	}
    public static void test(){
        
    }
    public static void load() throws IOException{
        Problem problem = new UF1(); // put your problem here
        File file = new File(MOEA_HOME+"/solutions.txt");

        // load your solutions from file
        String line = null;
        BufferedReader reader = null;
        Population manualSolutions = new Population();

        try {
            reader = new BufferedReader(new FileReader(file));

            while ((line = reader.readLine()) != null) {
                Solution solution = problem.newSolution();
                String[] tokens = line.split("\\s+"); // split by whitespace

                for (int i = 0; i < problem.getNumberOfVariables(); i++) {
                    EncodingUtils.setReal(solution.getVariable(i), Double.parseDouble(tokens[i]));
                }

                manualSolutions.add(solution);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        // evaluate the solutions
        for (Solution solution : manualSolutions) {
            problem.evaluate(solution);
        }
        
        NondominatedPopulation nsgaResults = new Executor()
                .withProblemClass(UF1.class) // put your problem here
                .withAlgorithm("NSGAII")
                .withMaxEvaluations(10000)
                .run();
        new Plot()
                .add("External", manualSolutions)
                .add("NSGA-II", nsgaResults)
                .show();
    /*    
        ReferenceSetMerger merger = new ReferenceSetMerger();
        merger.add("Manual", manualSolutions);
        merger.add("NSGA-II", nsgaResults);

        System.out.println("External: " + merger.getContributionFrom("External") + " / " + manualSolutions.size());
        System.out.println("NSGA-II: " + merger.getContributionFrom("NSGA-II") + " / " + nsgaResults.size());
    */
    }
}
