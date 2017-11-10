/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

import static Algorithms.ReadMatrixCSV.readMatrix;
import Algorithms.testScilab;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.moeaframework.Executor;
import org.moeaframework.analysis.plot.Plot;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Population;
import org.moeaframework.core.Problem;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.problem.CEC2009.UF1;
import org.moeaframework.util.ReferenceSetMerger;
import org.moeaframework.Instrumenter;
import org.moeaframework.analysis.collector.Accumulator;
import org.moeaframework.problem.AbstractProblem;

/**
 *
 * @author letrungdung
 */
public class TestMOEA {
    static String MOEA_HOME = new App().readhome("MOEA_HOME");
    public static void main(String[] args) throws IOException {
		//configure and run this experiment
	/*	NondominatedPopulation result = new Executor()
				.withAlgorithm("NSGAII")
				.withProblem("UF1")
				.withMaxEvaluations(10000)
				.run();
		//display the results
		System.out.format("Objective1  Objective2%n");
		for (Solution solution : result) {
			if (!solution.violatesConstraints()) {
				System.out.format("%10.3f      %10.3f%n",
						solution.getObjective(0),
						solution.getObjective(1));
			}
		}
        */  
        test2();
        //        load();
    }
    public static void test2() throws IOException{
        
        Problem problem = new AbstractProblem(1, 2) {

			@Override
			public void evaluate(Solution solution) {
                            String maxtrixFile = "/Users/letrung/maxtrix.csv";
                            int Max = 0;
                            try {
                                Max = CsvFileReader.count(maxtrixFile);
                            } catch (IOException ex) {
                                Logger.getLogger(TestMOEA.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            double[][] matrixMetrics = readMatrix(maxtrixFile, Max);
                            //testScilab.printMatrix(matrixMetrics);
				double x = EncodingUtils.getInt(solution.getVariable(0));
				//int[] x = EncodingUtils.getInt(solution);
				solution.setObjective(0, 2*x);//matrixMetrics[x][1]);
				solution.setObjective(1, x);//matrixMetrics[x][2]);
			}

			@Override
			public Solution newSolution() {
                            String maxtrixFile = "/Users/letrung/maxtrix.csv";
                            int Max = 0;
                            try {
                                Max = CsvFileReader.count(maxtrixFile);
                            } catch (IOException ex) {
                                Logger.getLogger(TestMOEA.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            double[][] matrixMetrics = readMatrix(maxtrixFile, Max);
                            //testScilab.printMatrix(matrixMetrics);
				Solution solution = new Solution(1, 2);
				solution.setVariable(0, EncodingUtils.newBinaryInt(-10, 10));//matrixMetrics.length));
				return solution;
			}
			
		};
        NondominatedPopulation result = new Executor()
				.withProblem(problem)
				.withAlgorithm("NSGAII")
				.withMaxEvaluations(10000)
				.run();
		
		// Finally, display the results.  There should be three solutions:
		//    (4, 0), (1, 1), and (0, 4)
		System.out.format("Objective1  Objective2%n");
		
		for (Solution solution : result) {
			System.out.format("%.4f      %.4f%n",
					solution.getObjective(0),
					solution.getObjective(1));
		}
    }
    public static void test(){
        Instrumenter instrumenter = new Instrumenter()
            .withReferenceSet(new File(MOEA_HOME+"/UF1.dat"))
            .withFrequency(100)
            .attachElapsedTimeCollector()
            .attachGenerationalDistanceCollector();
         
        new Executor()
            .withProblem("UF1")
            .withAlgorithm("NSGAII")
            .withMaxEvaluations(10000)
            .withInstrumenter(instrumenter)
            .run();
         
        Accumulator accumulator = instrumenter.getLastAccumulator();
        for (int i=0; i<accumulator.size("NFE"); i++) {
            System.out.println(accumulator.get("NFE", i) + "\t" + 
            accumulator.get("Elapsed Time", i) + "\t" +
            accumulator.get("GenerationalDistance", i));
        }
        
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
        for (Solution solution : nsgaResults) {
			if (!solution.violatesConstraints()) {
				System.out.format("%10.4f      %10.4f%n",
						solution.getObjective(0),
						solution.getObjective(1));
			}
		}
    /*    new Plot()
                .add("External", manualSolutions)
                .add("NSGA-II", nsgaResults)
                .show();
        
        ReferenceSetMerger merger = new ReferenceSetMerger();
        merger.add("Manual", manualSolutions);
        merger.add("NSGA-II", nsgaResults);

        System.out.println("External: " + merger.getContributionFrom("External") + " / " + manualSolutions.size());
        System.out.println("NSGA-II: " + merger.getContributionFrom("NSGA-II") + " / " + nsgaResults.size());
    */
    }
}
