/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sparkexample;

/**
 *
 * @author letrung
 */
public class TestExample {
    static int int_localhost = 1323;
    static String name_host = "localhost";
    static String SPARK_HOME = new App().readhome("SPARK_HOME");
    static String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    static String HIVE_HOME = new App().readhome("HIVE_HOME");
    static String IRES_HOME = new App().readhome("IRES_HOME");
    static String ASAP_HOME = IRES_HOME;
    static String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    //    @Test
    public void testPosgreSQLEngine () throws Exception {
        TestPostgreSQLDatabase testPostgreSQLEngine = new TestPostgreSQLDatabase();
        testPostgreSQLEngine.main();
    }
//    @Test
    public void testSparkSQLEngine() throws Exception {
        TestSparkDatabase.main();
    }
    
//    @Test
    public void testPostgreSQL_IRES() throws Exception {
        MyTestPostgreSQL_IRES testPostgreSQL_IRES = new MyTestPostgreSQL_IRES();
        testPostgreSQL_IRES.testPostgreSQL_IRES(IRES_library, name_host, int_localhost);
    }
    //    @Test
    public void testMove_Postgres_Spark_IRES() throws Exception {
        MyTestMove_Postgres_Spark_IRES testMove_Postgres_Spark_IRES = new MyTestMove_Postgres_Spark_IRES();
        testMove_Postgres_Spark_IRES.testMove_Postgres_Spark_IRES(IRES_library, name_host, int_localhost);
    }
//    @Test
    public void testMove_Postgres_Hive_IRES() throws Exception {
        MyTestMove_Postgres_Hive_IRES testMove_Postgres_Hive_IRES = new MyTestMove_Postgres_Hive_IRES();
        testMove_Postgres_Hive_IRES.testMove_Postgres_Hive_IRES(IRES_library, name_host, int_localhost);
    }
//    @Test
    public void testMove_Hive_Postgres_IRES() throws Exception {
        MyTestMove_Hive_Postgres_IRES testMove_Hive_Postgres_IRES = new MyTestMove_Hive_Postgres_IRES();
        testMove_Hive_Postgres_IRES.testMove_Hive_Postgres_IRES(IRES_library, name_host, int_localhost);
    }
//    @Test
    public void testMove_Hive_Spark_IRES() throws Exception {
        MyTestMove_Hive_Spark_IRES testMove_Hive_Spark_IRES = new MyTestMove_Hive_Spark_IRES();
        testMove_Hive_Spark_IRES.testMove_Hive_Spark_IRES(IRES_library, name_host, int_localhost);
    }
//    @Test
    public void testMove_Spark_Hive_IRES() throws Exception {
        MyTestMove_Spark_Hive_IRES testMove_Spark_Hive = new MyTestMove_Spark_Hive_IRES();
        testMove_Spark_Hive.testMove_Spark_Hive_IRES(IRES_library, name_host, int_localhost);
    }
//    @Test
    public void testMove_Spark_Postgres_IRES() throws Exception {
        MyTestMove_Spark_Postgres_IRES testMove_Spark_Postgres = new MyTestMove_Spark_Postgres_IRES();
        testMove_Spark_Postgres.testMove_Spark_Postgres_IRES(IRES_library, name_host, int_localhost);
    }
    
}
