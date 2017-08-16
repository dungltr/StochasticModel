/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package IRES;

import LibraryIres.Move_Data;
import com.sparkexample.App;

/**
 *
 * @author le
 */
public class Measure {
    String IRES_HOME = new App().readhome("IRES_HOME");
    String ASAP_HOME = IRES_HOME;
    String tools = ASAP_HOME+"/asap-tools/bin";
    String start = "cd "+tools+"\n"
            +"./asap monitor start";
    public String StartMeasure(){
        return start;
    }
    public String StopMeasure(Move_Data Data, double size){
        String stopMeasure = "cd "+tools+"\n"
                +"./asap report -m In0@size=" +size+" -cm -e " +runWorkFlowIRES.Nameop(Data)+"\n";
//                +"./asap monitor stop";
        return stopMeasure;
    }
}
