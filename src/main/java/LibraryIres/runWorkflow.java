/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.WorkflowClient;

/**
 *
 * @author letrungdung
 */
public class runWorkflow {
    int int_localhost = 1323;
    String name_host = "localhost";
    public void runWorkflow(String workflow, String policy) throws Exception{

        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
               
        String materializedWorkflow = wcli.materializeWorkflow(workflow, policy);
        System.out.println(materializedWorkflow);
        System.out.println("Add materializedWorkflow successful"+workflow);
        /** Execution */
        String w = wcli.executeWorkflow(materializedWorkflow);                                            
        int count=0;
        while(true){
            long start = System.currentTimeMillis();
            wcli.waitForCompletion(w);
            long stop = System.currentTimeMillis();
            double actualTime = (double)(stop-start)/1000.0;// -12.0;
            System.out.println("Actual Time: "+actualTime+"---------------------------------------------------------------------------------");           
            count++;
            if(count>=1)// old value is 1000
	break;
        }
    }
    public double runWorkflow2(String workflow, String policy) throws Exception{

        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        WorkflowClient wcli = new WorkflowClient();
        wcli.setConfiguration(conf);
               
        String materializedWorkflow = wcli.materializeWorkflow(workflow, policy);
        System.out.println(materializedWorkflow);
        System.out.println("Add materializedWorkflow successful"+workflow);
        /** Execution */
        String w = wcli.executeWorkflow(materializedWorkflow);                                            
        int count=0;
        double actualTime = 0;
        while(true){
            long start = System.currentTimeMillis();
            wcli.waitForCompletion(w);
            long stop = System.currentTimeMillis();
            actualTime = (double)(stop-start)/1000.0;// -12.0;
            System.out.println("Actual Time: "+actualTime+"---------------------------------------------------------------------------------");           
            count++;
            if(count>=1)// old value is 1000
	break;
        }
        return actualTime;
    }
}
