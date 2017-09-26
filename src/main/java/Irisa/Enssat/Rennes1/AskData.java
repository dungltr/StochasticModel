/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1;
    
/**
 *
 * @author letrung
 */
public class AskData {
    String Kind;
    String Db;
    String Size;
    String From;
    String To;
    String More;
/**/    
    public AskData (String Kind, String Db, String Size, String From, String To, String More) {
        this.Kind = Kind;
        this.Db = Db;
        this.Size = Size;
        this.From = From;
        this.To = To;
        this.More = More;
    }
    public String get_Kind () {return Kind;}
    public String get_Db () {return Db;}
    public String get_Size () {return Size;}
    public String get_From () {return From;}
    public String get_To () {return To;}
    public String get_More() {return More;}
    
    public void set_Kind (String Kind) {this.Kind = Kind;}
    public void set_Db (String Db) {this.Db = Db;}
    public void set_Size (String Size) {this.Size = Size;}
    public void set_From (String From) {this.From = From;}
    public void set_To (String To) {this.To = To;}
    public void set_More (String More) {this.More = More;}
}
