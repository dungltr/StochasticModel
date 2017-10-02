/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

import static IRES.TPCHQuery.readSQL;
import com.sparkexample.App;
import java.sql.*;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
import static javolution.testing.TestContext.fail;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import static org.apache.calcite.tools.Frameworks.getPlanner;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.sparkexample.TestPostgreSQLDatabase;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;



/**
 *
 * @author letrung
 */
public class parseQuery {
    static String username = System.getProperty("user.name");
    static String password = readpass();
    static String HOME=System.getenv().get("HOME");
    static String FILENAME = HOME + "/Documents/password.txt";
   String customer = "CREATE TABLE CUSTOMER "
           + "(custkey INT PRIMARY KEY NOT NULL,"
           + " name TEXT,"
           + " address TEXT,"
           + " nationkey INT,"
           + " phone TEXT,"
           + " acctbal INT,"
           + " mktsegment TEXT,"
           + " comment TEXT)";
   String region = "CREATE TABLE REGION_A "
           + "(regionkey INT PRIMARY KEY NOT NULL,"
           + " name TEXT,"
           + " comment TEXT)";
   String query = "SELECT * FROM COMPANY;";
   String query2 = "INSERT INTO REGION_A (regionkey,name,comment)"
               + "VALUES (1, 'Vietnam', 'Hanoi');";
    // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:postgres://localhost:5432/postgres";

   //  Database credentials
   static final String USER = "letrung";
   static final String PASS = "trung";
   
   public static void main (String query_name) {
        String[] datasets = checkDataSet("query0");
        String[][] dataBases = checkDataBases(datasets);
        for (int i = 0; i < dataBases.length; i++){
            System.out.println("\n Databases of " + datasets[i] + " are:");
            for (int j = 0; j < dataBases[i].length; j++)
                System.out.println(dataBases[i][j]);
        }
//        test_code();
//        Planner planner = getPlanner(null);
//        SqlNode parse = planner.parse("select * from \"emps\"");
//        try {
//            RelRoot rel = planner.rel(parse);
//            fail("expected error, got " + rel);
//        } catch (IllegalArgumentException e) {
            //assertThat(e.getMessage(),containsString("cannot move from STATE_3_PARSED to STATE_4_VALIDATED"));
//    }
    
//        Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(Frameworks.createRootSchema(false)).build());
//        SqlNode parsed = planner.parse("select * from orders, lineitem where l_orderkey = o_orderkey;");//                readSQL(new App().readhome("SQL")+"query0"));
//        planner.validate(parsed);
//        RelRoot relRoot = planner.rel(parsed);
    }
    public static void test_code() throws SQLException{
        String EXTERNAL_DRIVER = "org.postgresql.Driver";
	    try {
	        Class.forName(EXTERNAL_DRIVER);
	    } catch (ClassNotFoundException e) {
	        throw new SQLException("Could not find class " + EXTERNAL_DRIVER);
	    }

	    Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mydb", username, password);

	    String t1 = "T1( x int, y string )@python = {* yield (1, 'abc') *} \n";
	    String t2 = "T2( x int, y string ) = ( SELECT T1.x, T1.y FROM T1 ) \n";
	    String query = t1 + t2 +"SELECT T2.x, T2.y FROM T2 WHERE T2.x = 1";
	    query = "";
            PreparedStatement stmt = null; 
	    ResultSet rs = null;
	    try{
//	        stmt = conn.prepareStatement(query);
//	        stmt.execute();
//	        rs = stmt.getResultSet();
/*			
	        while (rs.next()) {
	            System.out.println("row " + rs.getRow() + ": " + rs.getInt(1)
	                + ", " + rs.getString(2));
	        }
*/	    }finally{
	        if(rs != null) rs.close();
	        if(stmt != null) stmt.close();
	        if(conn != null) conn.close();
	    }
    }
    public static void create() {
       Connection c = null;
       Statement stmt = null;
       try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mydb", username, password);
         System.out.println("Opened database successfully");

         stmt = c.createStatement();
         String sql = "CREATE TABLE REGION " +
                      "(ID INT PRIMARY KEY     NOT NULL," +
                      " NAME           TEXT    NOT NULL, " +
                      " AGE            INT     NOT NULL, " +
                      " ADDRESS        CHAR(50), " +
                      " SALARY         REAL)";
         
         stmt.executeUpdate(sql);
         stmt.close();
         c.close();
       } catch ( Exception e ) {
         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
         System.exit(0);
       }
       System.out.println("Table created successfully");
     }
    public static String[][] checkDataBases(String[] datasets){
        String[][] dataBases = new String[datasets.length][];
        for (int i = 0; i < datasets.length; i++){
            dataBases[i] = checkDatabase(datasets[i]);
        }
        return dataBases;  
    }
    public static String[] checkDataSet (String query_name){
        String Query_name = query_name;
        String SQL_folder = new App().readhome("SQL");
        String SQL_fileName = SQL_folder + Query_name;
        String readSQL = readSQL(SQL_fileName);
        System.out.println("\n"+SQL_fileName + "----SQL:---" + readSQL);
        String dataset = readSQL.substring(readSQL.indexOf("from ")+5, readSQL.indexOf("where"));
        String[] datasets = dataSets(dataset);
//        System.out.println("\n From--dataset--Where---:" + datasets[0]);
        return datasets;
    }
    public static String[] checkDatabase(String dataset){
        String dataBase = new App().readhome(dataset);
        String[] dataBases = dataSets(dataBase);
        return dataBases;
    }
    public static String[] dataSets(String DataSet){
        DataSet = DataSet.toLowerCase().replace(" ", "");
        String[] arr = DataSet.split(",");    
/*        for ( String ss : arr) {
            System.out.println(ss);
        }
*/        return arr;
    } 
     public static String readpass() {
       String sCurrentLine = "nothing";

       try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {
			while ((sCurrentLine = br.readLine()) != null) {
                                return sCurrentLine;
			}

		} catch (IOException e) {
			e.printStackTrace();                       
		}
       return sCurrentLine;
    }
    
}
