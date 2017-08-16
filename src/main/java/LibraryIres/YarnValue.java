/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

/**
 *
 * @author le
 */   
public class YarnValue {
        private double Ram;
        private double Core;
        
        public YarnValue (double Ram, double Core) {
        this.Ram = Ram;
        this.Core = Core;
       
    }
    public double get_Ram () {return Ram;}
    public void set_Ram (double Ram) {this.Ram = Ram;}
    
    public double get_Core () {return Core;}
    public void set_Core (double Ram) {this.Core = Core;}
}
