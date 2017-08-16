/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import com.sparkexample.App;
import gr.ntua.cslab.asap.operators.Dataset;


/**
 *
 * @author letrungdung
 */
public class createDataset {
    int int_localhost = 1323;
    String name_host = "localhost";
    String SPARK_HOME = new App().readhome("SPARK_HOME");
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String IRES_HOME = new App().readhome("IRES_HOME");
    String ASAP_HOME = IRES_HOME;
    String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    String directory_library = IRES_library+"/target/asapLibrary/";
    String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    String directory_datasets = IRES_library+"/target/asapLibrary/datasets/";
    String OperatorFolder = IRES_library+"/target/asapLibrary/operators/";
    public void createDataset(Move_Data Data, String SQL) throws Exception {
        Dataset d1 = new Dataset(Data.DataIn);
        d1.add("Constraints.DataInfo.Attributes.number","2");
	d1.add("Constraints.DataInfo.Attributes.Atr1.type","ByteWritable");
	d1.add("Constraints.DataInfo.Attributes.Atr2.type","List<ByteWritable>");
	d1.add("Constraints.Engine.DB.NoSQL.HBase.key","Atr1");
	d1.add("Constraints.Engine.DB.NoSQL.HBase.value","Atr2");
	d1.add("Constraints.Engine.DB.NoSQL.HBase.location","127.0.0.1");
	d1.add("Optimization.size","1TB");
	d1.add("Optimization.uniqueKeys","1.3 billion"); 
        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
    }
    
}
