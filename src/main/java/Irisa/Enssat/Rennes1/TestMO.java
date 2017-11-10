/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;
import static Algorithms.ReadMatrixCSV.readMatrix;
import Algorithms.testScilab;
import static Irisa.Enssat.Rennes1.TestMOEA.MOEA_HOME;
import WriteReadData.CsvFileReader;
import com.sparkexample.App;
import java.io.BufferedReader;
import java.io.File;
import static java.lang.Math.pow;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.moeaframework.util.ReferenceSetMerger;
import org.moeaframework.Executor;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Population;
import org.moeaframework.core.Problem;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;
import org.moeaframework.util.Vector;
/**
 *
 * @author letrung
 */
public class TestMO {
    public static class MO extends AbstractProblem {
    private int numberOfVariables;
    private int numberOfObjectives;
    private int numberOfConstraints;
    static String MOEA_HOME = new App().readhome("MOEA_HOME");
    String maxtrixFile = MOEA_HOME+"/matrix.csv";
    int Max = CsvFileReader.count(maxtrixFile);
    double[][] matrixMetrics = readMatrix(maxtrixFile, Max);

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
    public static void main(String[] args) {
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
                    .withProperty("populationSize", 5000)
                    .run();
            System.out.println("Num of Solutions: "+ result.size());
            // 2.2.4 Read solutions
            for (int m = 0; m < result.size(); m++) {
                Solution solution = result.get(m);
                int[] x = EncodingUtils.getInt(solution);
                double[] objectives = solution.getObjectives();

                //Negate objectives to return them to their maximized form.
                objectives = Vector.negate(objectives);//.negate(objectives);

                //2.2.5 Print results

                
                System.out.println("\n    Solution " + (m + 1) + ":");
                for (int i=0; i < objectives.length; i++)
                System.out.print("      Obj "+i+": " + -objectives[i]);
                System.out.println("    Con 1: " + solution.getConstraint(0));

                for(int j=0;j<x.length;j++){
                    System.out.print("      Var " + (j+1) + ":" + x[j]+"\n");
                }
                
                    //2.2.6 Export results to Excel sheets by different iterations
        /*
                    if (k == 2) {
        // Create Title Row in Excel
        writeXLSX(false, sheet1, 0, 0, "Solution");
        writeXLSX(false,sheet1, 0, 1, "Obj_1");
        writeXLSX(false,sheet1, 0, 2, "Obj_2");
        writeXLSX(false,sheet1, 0, 3, "Obj_3");
        writeXLSX(false,sheet1, 0, 4, "Con_1");
        for(int n=5; n<x.length+5; n++){
        writeXLSX(false, sheet1, 0, n, "Var_"+(n-4));
        }
        // create data input row in excel
        writeXLSX(true, sheet1, m+1, 0, m+1);
        writeXLSX(true, sheet1, m+1, 1, objectives[0]);
        writeXLSX(true, sheet1, m+1, 2, objectives[1]);
        writeXLSX(true, sheet1, m+1, 3, objectives[2]);
        writeXLSX(true, sheet1, m+1, 4,
        solution.getConstraint(0));

                        for(int n=0; n<x.length; n++){
                        writeXLSX(true, sheet1, m+1, n+5, x[n]);
                        }
                    }
                   try{
                    FileOutputStream fileOut = new FileOutputStream("/Users/letrung/testMO.xlxs");
        //workBook.write(fileOut);
        fileOut.close();
        } catch(FileNotFoundException e){
        e.printStackTrace();
        } catch(IOException e){
        e.printStackTrace();
        }
        */
            }
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
