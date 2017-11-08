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
    public static void main(String args[]) throws Exception{
        TPCHQuery.Move(Math.random(),"tpch","100m","Postgres","Hive","Move","training");
        TPCHQuery.Move(Math.random(),"tpch","100m","Hive","Postgres","Move","training");
    }
}
