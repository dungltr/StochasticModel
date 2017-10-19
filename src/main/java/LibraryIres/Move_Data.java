/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

/**
 *
 * @author letrung
 */
public class Move_Data  {
    String Operator;
    String DataIn;
    String DataInSize;
    String DatabaseIn;
    String Schema;
    String From;
    String To;
    String DataOut;
    String DataOutSize;
    String DatabaseOut;
    
    public Move_Data (String Operator, String DataIn, String DataInSize, String DatabaseIn, String Schema, String From, String To, String DataOut, String DataOutSize, String DatabaseOut) {
        this.Operator = Operator;
        this.DataIn = DataIn;
        this.DataInSize = DataInSize;
        this.DatabaseIn = DatabaseIn;
        this.Schema = Schema;
        this.From = From;
        this.To = To;
        this.DataOut = DataOut;
        this.DataOutSize = DataOutSize;
        this.DatabaseOut = DatabaseOut;
    }
    public String get_Operator () {return Operator;}
    public String get_DataIn () {return DataIn;}
    public String get_DataInSize () {return DataInSize;}
    public String get_DatabaseIn () {return DatabaseIn;}
    public String get_Schema () {return Schema;}
    public String get_From () {return From;}
    public String get_To () {return To;}
    public String get_DataOut () {return DataOut;}
    public String get_DataOutSize () {return DataOutSize;}
    public String get_DatabaseOut () {return DatabaseOut;}
    
    public void set_Operator (String Operator) {this.Operator = Operator;}
    public void set_DataIn (String DataIn) {this.DataIn = DataIn;}
    public void set_DataInSize (String DataInSize) {this.DataInSize = DataInSize;}
    public void set_DatabaseIn (String DatabaseIn) {this.DatabaseIn = DatabaseIn;}
    public void set_Schema (String Schema) {this.Schema = Schema;}
    public void set_From (String From) {this.From = From;}
    public void set_To (String To) {this.To = To;}
    public void set_DataOut (String DataOut) {this.DataOut = DataOut;}
    public void set_DataOutSize (String DataOutSize) {this.DataInSize = DataOutSize;}
    public void set_DatabaseOut (String DatabaseOut) {this.DatabaseOut = DatabaseOut;}
}
