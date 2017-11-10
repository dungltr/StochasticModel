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
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;
/**
 *
 * @author letrung
 */
public class MyProblem extends AbstractProblem {

	public MyProblem() {
		super(2, 2, 2);
	}

	@Override
	public void evaluate(Solution solution) {
		double x = ((RealVariable)solution.getVariable(0)).getValue();
		double y = ((RealVariable)solution.getVariable(1)).getValue();
		double f1 = Math.pow(x - 2.0, 2.0) + Math.pow(y - 1.0, 2.0) + 2.0;
		double f2 = 9.0*x - Math.pow(y - 1.0, 2.0);
		double c1 = Math.pow(x, 2.0) + Math.pow(y, 2.0) - 225.0;
		double c2 = x - 3.0*y + 10.0;
		
		solution.setObjective(0, f1);
		solution.setObjective(1, f2);
		solution.setConstraint(0, c1 <= 0.0 ? 0.0 : c1);
		solution.setConstraint(1, c2 <= 0.0 ? 0.0 : c2);
	}

	@Override
	public Solution newSolution() {
		Solution solution = new Solution(2, 2, 2);
		
		solution.setVariable(0, new RealVariable(-20.0, 20.0));
		solution.setVariable(1, new RealVariable(-20.0, 20.0));
		
		return solution;
	}

}

