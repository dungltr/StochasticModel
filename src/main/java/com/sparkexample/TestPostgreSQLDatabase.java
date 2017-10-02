/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sparkexample;
import java.io.BufferedReader;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.io.Console;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import static java.lang.System.in;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;


/**
 *
 * @author letrung
 */
public class TestPostgreSQLDatabase {
    static String username = System.getProperty("user.name");  
    static String HOME=System.getenv().get("HOME");
    static String FILENAME = HOME + "/Documents/password.txt";
    static String password = readpass(FILENAME);
    static String customer = "CREATE TABLE CUSTOMER "
           + "(custkey INT PRIMARY KEY NOT NULL,"
           + " name TEXT,"
           + " address TEXT,"
           + " nationkey INT,"
           + " phone TEXT,"
           + " acctbal INT,"
           + " mktsegment TEXT,"
           + " comment TEXT)";
    static String region = "CREATE TABLE REGION_A "
           + "(regionkey INT PRIMARY KEY NOT NULL,"
           + " name TEXT,"
           + " comment TEXT)";
    static String query = "SELECT * FROM COMPANY;";
    static String query2 = "INSERT INTO REGION_A (regionkey,name,comment)"
               + "VALUES (1, 'Vietnam', 'Hanoi');";
    public void main() throws Exception { 
    switch (username) {
            case "letrungdung":  
                {
                username = "postgres";
                password = "postgres";
                }
                break;
            case "letrung":  
                {
                password = readpass(FILENAME);
                }
                break;
            case "le": 
                {
                username = "postgres";
                password = "postgres";
                }
                break;
            default:
                password = readpass(FILENAME);
                break;
    } 
//        testconnection();
//         create();
//        createdata(region);
//          insert();
//        insertquery(query2);
        select(query);
    }
    
    public void testconnection() throws SQLException{
       
        Connection c = null;
      try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/mydb",
            username, password);
         
      } catch (Exception e) {
         e.printStackTrace();
         System.err.println(e.getClass().getName()+": "+e.getMessage());
         System.exit(0);
      }
      System.out.println("Opened database successfully");
       
    }
    
    public void create()
     {
       Connection c = null;
       Statement stmt = null;
       try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/mydb", username, password);
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
    public void createdata(String fileSchemas){
        Connection c = null;
       Statement stmt = null;
       try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/mydb", username, password);
         System.out.println("Opened database successfully");

         stmt = c.createStatement();
         String sql = fileSchemas;
         
         stmt.executeUpdate(sql);
         stmt.close();
         c.close();
       } catch ( Exception e ) {
         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
         System.exit(0);
       }
       System.out.println("Table created successfully");
        
    }
    
    public void insert () {
      Connection c = null;
      Statement stmt = null;
      try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/mydb",
            username, password);
         c.setAutoCommit(false);
         System.out.println("Opened database successfully");

         stmt = c.createStatement();
         String sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (1, 'Paul', 32, 'California', 20000.00 );";
         stmt.executeUpdate(sql);

         sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
         stmt.executeUpdate(sql);

         sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );";
         stmt.executeUpdate(sql);

         sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );";
         stmt.executeUpdate(sql);

         stmt.close();
         c.commit();
         c.close();
      } catch (Exception e) {
         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
         System.exit(0);
      }
      System.out.println("Records created successfully");
    }
    public void insertquery (String query) {
      Connection c = null;
      Statement stmt = null;
      try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/mydb",
            username, password);
         c.setAutoCommit(false);
         System.out.println("Opened database successfully");

         stmt = c.createStatement();
         String sql = query;
         stmt.executeUpdate(sql);

         stmt.close();
         c.commit();
         c.close();
      } catch (Exception e) {
         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
         System.exit(0);
      }
      System.out.println("Records created successfully");
    }
   
    public void select(String query)
     {
       Connection c = null;
       Statement stmt = null;
       try {
       Class.forName("org.postgresql.Driver");
         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/mydb",
            username, password);
         c.setAutoCommit(false);
         System.out.println("Opened database successfully");

         stmt = c.createStatement();
         ResultSet rs = stmt.executeQuery(query);// "SELECT * FROM COMPANY;" );
         while ( rs.next() ) {
            int id = rs.getInt("id");
            String  name = rs.getString("name");
            int age  = rs.getInt("age");
            String  address = rs.getString("address");
            float salary = rs.getFloat("salary");
            System.out.println( "ID = " + id );
            System.out.println( "NAME = " + name );
            System.out.println( "AGE = " + age );
            System.out.println( "ADDRESS = " + address );
            System.out.println( "SALARY = " + salary );
            System.out.println();
         }
         rs.close();
         stmt.close();
         c.close();
       } catch ( Exception e ) {
         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
         System.exit(0);
       }
       System.out.println("Operation done successfully");
     }
    
    public static String readpass(String FILENAME) {
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
