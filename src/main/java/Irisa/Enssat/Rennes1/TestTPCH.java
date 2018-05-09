/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;

import IRES.TPCHQuery;

/**
 *
 * @author letrung
 */
public class TestTPCH {
    public static void main(String[] args) throws Exception{
        //testSQL();
        //testJoin();
        testMoveDicom();
        /*
        TPCHQuery.Move(Math.random(),"tpch","100m","Postgres","Hive","Move","training");
        TPCHQuery.Move(Math.random(),"tpch","100m","Hive","Postgres","Move","training");
        TPCHQuery.WorkflowMove(Math.random(),"tpch","100m","Hive","Postgres","Move", "training");
        TPCHQuery.WorkflowMove(Math.random(),"tpch","100m","Postgres","Hive","Move", "training");
        */
    }
    public static void testSQL() throws Exception {
        double TimeOfDay = 24.00*Math.random()/24;
        TPCHQuery.SQL(TimeOfDay, "tpch","100m","Hive","Postgres", "SQL", "training");
    }
    public static void testJoin() throws Exception {
        double TimeOfDay = 24.00*Math.random()/24;
        TPCHQuery.Join(TimeOfDay, "tpch","100m","Hive","Postgres", "Join", "training");
    }
    public static void testMove() throws Exception {
        double TimeOfDay = 24.00*Math.random()/24;
        TPCHQuery.Move(TimeOfDay, "tpch","1000m","Hive","Postgres", "Move", "training");
    }
    public static void testMoveDicom() throws Exception {
        double TimeOfDay = 24.00*Math.random()/24;
        TPCHQuery.Move(TimeOfDay, "dicom","","Hive","Postgres", "Move", "training");
        TPCHQuery.Move(TimeOfDay, "dicom","","Hive","Postgres", "Move", "training");
    }
}
