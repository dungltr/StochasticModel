/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

/**
 *
 * @author letrung
 */
public class CreateDatabase {
    public static String setupDataBaseHive(String DataOut, String Schema){
        int i = 0;
        String SQL = "CREATE DATABASE IF NOT EXISTS mydb;DROP TABLE IF EXISTS mydb."+DataOut+";CREATE TABLE IF NOT EXISTS mydb."+DataOut+" (CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10)) row format delimited fields terminated by \"|\";";
        for (i = 0; i < 100; i++)
            SQL = SQL + "INSERT INTO mydb."+DataOut+" VALUES (4, \"LETrungDung"+i+"\", \"Lannion\", 911, 0780531416, 0.5, \"MKTSEGMENT\", \"COMMENT\", \"LAST\");";
        return SQL;
    }
    public static String setupDataBase_HIVE(String DataOut, String Schema){
        int i = 0;
        String SQL = "DROP TABLE IF EXISTS mydb."+DataOut+";CREATE TABLE IF NOT EXISTS mydb."+DataOut+" "+Schema+" ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';";
        for (i = 0; i < 100; i++)
            SQL = SQL + "INSERT INTO "+DataOut+" VALUES ("+i+", \'MALE\');";
        return SQL;
    }
    public static String setupDataBasePostgres(String DataOut, String Schema){
        int i = 0;
        String SQL = "DROP TABLE IF EXISTS "+DataOut+";CREATE TABLE IF NOT EXISTS "+DataOut+" "+Schema+";";
        for (i = 0; i < 100; i++)
            SQL = SQL + "INSERT INTO "+DataOut+" VALUES ("+i+", \'LETrungDung"+i+"\', \'Lannion\', 911, 0780531416, 0.5, \'MKTSEGMENT\', \'COMMENT\', \'LAST\');";
        return SQL;
    }
    public static String setupDataBasePostgresAll(String DataOut, String Schema){
        int i = 0;
        String SQL = "DROP TABLE IF EXISTS "+DataOut+";CREATE TABLE IF NOT EXISTS "+DataOut+" "+Schema+";";
        for (i = 0; i < 200; i++)
            SQL = SQL + "INSERT INTO "+DataOut+" VALUES (4, \'LETrungDung"+i+"\', \'Lannion\', 911, 0780531416, 0.5, \'MKTSEGMENT\', \'COMMENT\', \'LAST\');";
        return SQL;
    }
    public static String setupDataBaseHiveAll(String DataOut, String Schema){
        int i = 0;
        String SQL = "CREATE DATABASE IF NOT EXISTS mydb;DROP TABLE IF EXISTS mydb."+DataOut+";CREATE TABLE IF NOT EXISTS mydb."+DataOut+" (CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10)) row format delimited fields terminated by \"|\";";
        for (i = 0; i < 200; i++)
            SQL = SQL + "INSERT INTO mydb."+DataOut+" VALUES (4, \"LETrungDung"+i+"\", \"Lannion\", 911, 0780531416, 0.5, \"MKTSEGMENT\", \"COMMENT\", \"LAST\");";
        return SQL;
    }
}
