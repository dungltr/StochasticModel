/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

import Algorithms.ReadMatrixCSV;
import Irisa.Enssat.Rennes1.thesis.sparkSQL.Writematrix2CSV;
import Irisa.Enssat.Rennes1.thesis.sparkSQL.utilities;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;
import org.moeaframework.Executor;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;
import org.moeaframework.util.Vector;

import java.io.IOException;

import static Algorithms.ReadMatrixCSV.readMatrix;
import static java.lang.Math.pow;

//import scala.collection.immutable.List;
//import scala.collection.immutable.Range;
/**
 *
 * @author letrung
 */
public class TestMO{
    public static class MO extends AbstractProblem {
    private int numberOfVariables;
    private int numberOfObjectives;
    private int numberOfConstraints;
    static String MOEA_HOME = new App().readhome("MOEA_HOME");
    //static String matrix = "plan";
    //String matrixFile = ReadFile.readhome(matrix)+".csv";
    String matrixFile = MOEA_HOME+"/Matrix.csv";
    String matrixResult = MOEA_HOME+ "/Matrix_result.csv";
    int Max = CsvFileReader.count(matrixFile);
    double[][] matrixMetrics = readMatrix(matrixFile, Max);

    public MO() throws IOException {
        super(1, 2, 1); // old is 2,3,1       
        numberOfVariables = 1;//matrixMetrics[0].length;
        numberOfObjectives = 2;//matrixMetrics.length;// old is 3
        numberOfConstraints = 1;
    }

    @Override
    public Solution newSolution() {
        Solution solution = new Solution(getNumberOfVariables(),getNumberOfObjectives(), getNumberOfConstraints());
        for (int i = 0; i < getNumberOfVariables(); i++) {
            solution.setVariable(i, new RealVariable(0.0, Max));
        }

        return solution;
    }

    @Override
    public void evaluate(Solution solution) {
        int[] x = EncodingUtils.getInt(solution);
        
        double[] f = new double[numberOfObjectives];
        double[] g = new double[numberOfConstraints];

        double[][] b = new double[numberOfObjectives][numberOfVariables];
        for (int i = 0; i < numberOfObjectives; i++){
            for (int j = 0; j < numberOfVariables; j++)
                b[i][j] = matrixMetrics[x[0]][i+1];
        }
      
        //Objectives.
        for (int i = 0; i < numberOfObjectives; i++) {
            f[i] = 0;
            for (int j = 0; j < numberOfVariables; j++) {
                f[i] -= b[i][j];
            }
        }      
        //Constraints:
        //constraints that are satisfied have a value of zero; violated constraints have
        //non-zero values (both positive and negative).
        
        for (int i = 0; i < numberOfConstraints; i++) {
            double sum = 0.0;
            for (int j = 0; j < numberOfVariables; j++) {
                sum += x[j];
            }
            if (sum <= Max) {
                g[i] = 0.0;
            }   else {
                g[i] = sum - Max;
                }
        }        
        //Negate the objectives since Knapsack is maximization.
        solution.setObjectives(Vector.negate(f));
        //solution.setObjectives(Vector.normalize(f));
        solution.setConstraints(g);
    }

    @Override
    public int getNumberOfConstraints() {
        return numberOfConstraints;
    }

    @Override
    public int getNumberOfObjectives() {
        return numberOfObjectives;
    }

    @Override
    public int getNumberOfVariables() {
        return numberOfVariables;
    }
}
    public static void main(String[] args) throws IOException {
        // 2.2.1 Create Excel workbook
        //XSSFWorkbook workBook = new XSSFWorkbook();
        // 2.2.2 Create Excel sheets by different iterations
        //XSSFSheet sheet1 = workBook.createSheet("Iteration");
        
        for (int k = 0; k < 2; k++) {
            int iteration = (int) pow(10, k + 1);
            System.out.println("Iteration: " + iteration);
            NondominatedPopulation result = new Executor()
                    .withProblemClass(MO.class)
                    .withAlgorithm("NSGAII")
                    .withMaxEvaluations(iteration)
                    .withProperty("populationSize", 50)
                    .run();
            System.out.println("Num of Solutions: "+ result.size());
            double[][] matrixResult = new double [result.size()][result.get(0).getNumberOfObjectives()+1];
            // 2.2.4 Read solutions
            for (int m = 0; m < result.size(); m++) {
                Solution solution = result.get(m);
                int[] x = EncodingUtils.getInt(solution);
                double[] objectives = solution.getObjectives();
                //Negate objectives to return them to their maximized form.
                objectives = Vector.negate(objectives);//.negate(objectives);
                System.out.println("\n    Solution " + (m + 1) + ":");

                for (int i=0; i < objectives.length; i++)
                System.out.print("      Obj "+i+": " + -objectives[i]);
                System.out.println("    Con 1: " + solution.getConstraint(0));

                for(int j=0;j<x.length;j++){
                    System.out.print("      Var " + (j+1) + ":" + x[j]+"\n");
                }
                for (int j=0; j < objectives.length ;j++){
                    matrixResult[m][j+1] = -objectives[j];
                }
            }
            String oldFile = new App().readhome("MOEA_HOME")+"/Matrix.csv";
            double[][] oldMatrix = ReadMatrixCSV.readMatrix(oldFile,CsvFileReader.count(oldFile));
            for (int i = 0; i < oldMatrix.length; i++){
                for (int j = 0; j < matrixResult.length; j++){
                    int check = 0;
                    for (int l = 1; l < matrixResult[0].length; l++){
                        if (oldMatrix[i][l]!=matrixResult[j][l]) check ++;
                    }
                    if (check == 0) matrixResult[j][0] = oldMatrix[i][0];
                }
            }
            System.out.println("Matrix of result-------------------------");
            for (int i = 0; i < matrixResult.length; i++){
                for (int j = 0; j< matrixResult[0].length; j++){
                    System.out.print(" " + matrixResult[i][j]);
                }
                System.out.println("\n");
            }
            utilities.renewFile(new App().readhome("MOEA_HOME")+"/Matrix_result.csv");
            Writematrix2CSV.addMatrix2Csv(new App().readhome("MOEA_HOME")+"/Matrix_result.csv", matrixResult);
        System.out.println("-----------------------------------------");    
        }
    }

    // 2.1 Create a class for excel writing
/*    private static void writeXLSX(boolean flag, Sheet sheet, int row, int cell, Object value){
        Row rowIn = sheet.getRow(row);
        if(rowIn == null) {
            rowIn = sheet.createRow(row);
        }
        Cell cellIn = rowIn.getCell(cell);
        if(cellIn == null) {
            cellIn = rowIn.createCell(cell);
        }

        if(value==null){
            cellIn.setCellValue("");
        }else{
            if (flag) {
                cellIn.setCellValue(Double.valueOf(value.toString()));
            } else {
                cellIn.setCellValue(value.toString());
            }
        }
    }
*/
}
