/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import com.sparkexample.App;
import gr.ntua.cslab.asap.client.ClientConfiguration;
import gr.ntua.cslab.asap.client.OperatorClient;
import gr.ntua.cslab.asap.client.WorkflowClient;
import gr.ntua.cslab.asap.operators.AbstractOperator;
import gr.ntua.cslab.asap.operators.Dataset;
import gr.ntua.cslab.asap.operators.MaterializedOperators;
import gr.ntua.cslab.asap.operators.Operator;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow;
import gr.ntua.cslab.asap.workflow.AbstractWorkflow1;
import gr.ntua.cslab.asap.workflow.WorkflowNode;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 *
 * @author letrungdung
 */
public class createWorkflow {
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

    /**
     *
     * @throws Exception
     */
    public void createWorkflowMove(Move_Data Data, String SQL) throws Exception{
        String InPutData = Data.get_DataIn();//"asapServerLog";//Data.get_DataIn();
        String OutPutData = Data.get_DataOut();
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
        String NameOfAbstractWorkflow = NameOp+"_Workflow";
        String AlgorithmsName = NameOp + "_query";
//        createDataset(Data, SQL);
//        createOperatorMove(Data, SQL);
//        createAbstractOperatorMove(Data, SQL);
        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
        AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        Operator mop1 = new Operator(NameOp,"");
        Dataset d1 = new Dataset(InPutData);
        
        d1.inputFor(mop1, 0);
//        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp = new AbstractOperator(AbstractOp);//AopAbstractOperator);
        WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp);
		
	Dataset d2 = new Dataset(OutPutData);
	WorkflowNode t2 = new WorkflowNode(false,true,OutPutData);
	t2.setDataset(d2);

        t1.addOutput(0,op1);
                
	op1.addInput(0,t1);
	op1.addOutput(0,t2);
		
	t2.addInput(0,op1);
		
	abstractWorkflow1.addTarget(t2);  
        WorkflowClient wcli = new WorkflowClient();
	wcli.setConfiguration(conf);
    
        wcli.addAbstractWorkflow(abstractWorkflow1);

// To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
        abstractWorkflow.addInputEdge(d1,abstractOp,0);
	abstractWorkflow.addOutputEdge(abstractOp,d2,0);
        abstractWorkflow.getWorkflow(d2);
//	wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
    }
    
    public void createDatasetMove(Move_Data Data, String SQL) throws Exception {
        Dataset d1 = new Dataset(Data.get_DataIn());
        d1.add("Optimization.documents","1");
	d1.add("Optimization.size","500");
	d1.add("Execution.path","hdfs\\:///user/root/asap-server.log");
	d1.add("Constraints.Engine.FS","HDFS");       
        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
    }
    public void createOperatorMove(Move_Data Data, String SQL) throws Exception {
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String AlgorithmsName = NameOp + "_query";
        Operator mop1 = new Operator(NameOp,"");
        mop1.add("Constraints.Engine",Data.get_To());
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Input.number","1");
        mop1.add("Constraints.OpSpecification.Algorithm.name", AlgorithmsName);
        mop1.add("Optimization.model.execTime", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.outputSpace.execTime", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");
        mop1.add("Optimization.cost", "1.0");
        mop1.add("Optimization.execTime", "1.0");
 
        mop1.add("Execution.Arguments.number", "2");
        mop1.add("Execution.Argument0", "In0.path.local");
        mop1.add("Execution.Argument1","lines.out");
        mop1.add("Execution.Output0.path", "$HDFS_OP_DIR/lines.out");
        mop1.add("Execution.copyFromLocal","lines.out");
        mop1.add("Execution.copyToLocal", "In0.path");
        mop1.add("Execution.LuaScript",NameOp+".lua");
        mop1.add("Execution.command",NameOp+".sh");
        cli.addOperator(mop1); 
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
    }
    public void createAbstractOperatorMove(Move_Data Data, String SQL) throws IOException, Exception {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
        String AlgorithmsName = NameOp + "_query";        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);        
        AbstractOperator op = new AbstractOperator(AbstractOp);//AopAbstractOperator);//AopAbstractOperator);
        op.add("Constraints.Engine", Data.get_To());
        op.add("Constraints.Input.number","1");
	op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
	op.add("Constraints.Output.number", "1");
        op.writeToPropertiesFile(directory_library + "abstractOperators/" + op.opName);                      
        cli.addAbstractOperator(op);
        op.writeToPropertiesFile(op.opName);
    } 
    public void createWorkflowSQL(Move_Data Data, String SQL) throws Exception{
        String InPutData = Data.get_DataIn();
        String OutPutData = Data.get_DataOut();
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
        String NameOfAbstractWorkflow = NameOp+"_Workflow";
        String AlgorithmsName = NameOp + "_query";
//        createDatasetSQL(Data,SQL);
//        createOperatorSQL(Data,SQL);
//        createAbstractOperatorSQL(Data,SQL);
        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        
        AbstractWorkflow1 abstractWorkflow1 = new AbstractWorkflow1(NameOfAbstractWorkflow);		
        Operator mop1 = new Operator(NameOp,"");
        Dataset d1 = new Dataset(InPutData);
        
        d1.inputFor(mop1, 0);
//        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
	WorkflowNode t1 = new WorkflowNode(false,false,InPutData);
	t1.setDataset(d1);
                
        AbstractOperator abstractOp = new AbstractOperator(AbstractOp);//AopAbstractOperator);
        WorkflowNode op1 = new WorkflowNode(true,true,AbstractOp);//AopAbstractOperator);
	op1.setAbstractOperator(abstractOp);
		
	Dataset d2 = new Dataset(OutPutData);
	WorkflowNode t2 = new WorkflowNode(false,true,OutPutData);
	t2.setDataset(d2);

        t1.addOutput(0,op1);
                
	op1.addInput(0,t1);
	op1.addOutput(0,t2);
		
	t2.addInput(0,op1);
		
	abstractWorkflow1.addTarget(t2);  
        WorkflowClient wcli = new WorkflowClient();
	wcli.setConfiguration(conf);
        
        wcli.addAbstractWorkflow(abstractWorkflow1);

// To show in Materialized Workflow
        MaterializedOperators library =  new MaterializedOperators();                
        AbstractWorkflow abstractWorkflow = new AbstractWorkflow(library);
        abstractWorkflow.addInputEdge(d1,abstractOp,0);
	abstractWorkflow.addOutputEdge(abstractOp,d2,0);
        abstractWorkflow.getWorkflow(d2);
//	wcli.removeAbstractWorkflow(NameOfAbstractWorkflow);
        
    }
    public void createDatasetSQL(Move_Data Data, String SQL) throws Exception {
        Dataset d1 = new Dataset(Data.get_DataIn());
/*        d1.add("Execution.name",Data.get_DataIn());
	d1.add("Execution.schema",Data.get_Schema());
	d1.add("Constraints.Engine.SQL",Data.get_To());
	d1.add("Constraints.type","SQL");
	d1.add("Constraints.Engine.location",new App().getComputerName());
	d1.add("Constraints.Engine.DB.NoSQL.HBase.location","127.0.0.1");
	d1.add("Optimization.size","1TB");  
*/        d1.add("Optimization.documents","1");
	d1.add("Optimization.size","500");
	d1.add("Execution.path","hdfs\\:///user/root/asap-server.log");
	d1.add("Constraints.Engine.FS","HDFS");
        d1.writeToPropertiesFile(directory_datasets + d1.datasetName);
    }
    public void createOperatorSQL(Move_Data Data, String SQL) throws Exception {
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        Operator mop1 = new Operator(NameOp,"");
        String AlgorithmsName = NameOp + "_query";
        mop1.add("Constraints.Engine",Data.get_To());
        mop1.add("Constraints.Output.number","1");
        mop1.add("Constraints.Input.number","1");
        mop1.add("Constraints.OpSpecification.Algorithm.name", AlgorithmsName);
        mop1.add("Optimization.model.execTime", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.model.cost", "gr.ntua.ece.cslab.panic.core.models.UserFunction");
        mop1.add("Optimization.outputSpace.execTime", "Double");
        mop1.add("Optimization.outputSpace.cost", "Double");
        mop1.add("Optimization.cost", "1.0");
        mop1.add("Optimization.execTime", "1.0");
 
        mop1.add("Execution.Arguments.number", "2");
        mop1.add("Execution.Argument0", SQL);
        mop1.add("Execution.Argument1",Data.get_To());
        mop1.add("Execution.Output0.name", Data.get_DataOut());
        mop1.add("Execution.Output0.schema",Data.get_Schema());
        mop1.add("Execution.LuaScript",NameOp+".lua");
        mop1.add("Execution.command",NameOp+".sh");
        cli.addOperator(mop1); 
        mop1.writeToPropertiesFile(directory_operator+mop1.opName);
    }
    public void createAbstractOperatorSQL(Move_Data Data, String SQL) throws IOException, Exception {
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        String AbstractOp = "Abstract_"+NameOp;
        String AlgorithmsName = NameOp + "_query";        
        ClientConfiguration conf = new ClientConfiguration(name_host,int_localhost);
        OperatorClient cli = new OperatorClient();		
        cli.setConfiguration(conf);        
        AbstractOperator op = new AbstractOperator(AbstractOp);//AopAbstractOperator);//AopAbstractOperator);
        op.add("Constraints.Engine", Data.get_To());
        op.add("Constraints.Input.number","1");
	op.add("Constraints.OpSpecification.Algorithm.name",AlgorithmsName);
	op.add("Constraints.Output.number", "1");
        op.writeToPropertiesFile(directory_library + "abstractOperators/" + op.opName);                      
        cli.addAbstractOperator(op);
        op.writeToPropertiesFile(op.opName);
    }
    public void createDataSQL (Move_Data Data,String SQL) throws Exception {
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        LuaScript script = new LuaScript();
        String lua = script.LuaScript(Data);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
        switch (Data.get_From()) {
            case "HIVE": case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres"))
                    create_Data_Hive_Postgres(Data, SQL);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Hive_Spark(Data);
                }
                break;
            case "POSTGRES": case "postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Postgres_Spark(Data);
                else if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_SQL_Postgres(Data,SQL);
                }
                break;
            case "SPARK": case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }       
    }
    public void createDataSQL2 (Move_Data Data, String SQL, double Data_size, double Ram, double Core) throws Exception {
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        LuaScript script = new LuaScript();
        String lua = script.LuaScript2(Data,Data_size, Ram, Core);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
        switch (Data.get_From()) {
            case "HIVE": case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres"))
                    create_Data_Hive_Postgres(Data, SQL);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Hive_Spark(Data);
                }
                break;
            case "POSTGRES": case "postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Postgres_Spark(Data);
                else if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_SQL_Postgres(Data,SQL);
                }
                break;
            case "SPARK": case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }       
    }
    
    public void createDataMove (Move_Data Data,String SQL) throws Exception {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        LuaScript script = new LuaScript();
        String lua = script.LuaScript(Data);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
        switch (Data.get_From()) {
            case "HIVE": case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres"))
                    create_Data_Hive_Postgres(Data, SQL);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Hive_Spark(Data);
                }
                break;
            case "POSTGRES": case "postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Postgres_Spark(Data);
                else if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_SQL_Postgres(Data,SQL);
                }
                break;
            case "SPARK": case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }       
    }
    public void createDataMove2 (Move_Data Data,String SQL, double Data_size, double Ram, double Core) throws Exception {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        LuaScript script = new LuaScript();
        String lua = script.LuaScript2(Data,Data_size, Ram, Core);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".lua", lua);
        switch (Data.get_From()) {
            case "HIVE": case "hive":
                {
                if ((Data.get_To() == "POSTGRES") || (Data.get_To() == "postgres"))
                    create_Data_Hive_Postgres(Data, SQL);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Hive_Spark(Data);
                }
                break;
            case "POSTGRES": case "postgres":
                {
                if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Postgres_Hive(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "SPARK")|| (Data.get_To() == "spark"))
                    create_Data_Postgres_Spark(Data);
                else if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_SQL_Postgres(Data,SQL);
                }
                break;
            case "SPARK": case "spark":
                {
                if ((Data.get_To() == "POSTGRES")|| (Data.get_To() == "postgres"))
                    create_Data_Spark_Postgres(Data);//In, DatabaseIn, Schema, From, To, DataOut, DatabaseOut);
                else if ((Data.get_To() == "HIVE")|| (Data.get_To() == "hive"))
                    create_Data_Spark_Hive(Data);
                }
                break;
            default:                
                break;
        }       
    }
    public void create_Data_Hive_Postgres(Move_Data Data, String SQL) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();       
        String sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2Posgres() + script.Postgres_SQL(SQL);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    public void create_Data_Hive_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Hive2CSV() + script.CSV2HDFS() + script.HDFS2Parquet();
        ParquetCSV convertCSV2Parquet = new ParquetCSV();
        String PY = convertCSV2Parquet.CSV2Parquet();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", PY);
    }
    
    public void create_Data_Postgres_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres2CSV() + script.CSV2HDFS() + script.HDFS2Hive(); 
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
    }
    
    public void create_Data_Postgres_Spark(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres2CSV() + script.CSV2HDFS() + script.HDFS2Parquet();
        ParquetCSV convertCSV2Parquet = new ParquetCSV();
        String PY = convertCSV2Parquet.CSV2Parquet();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertCSV2Parquet.py", PY);
    }
    
    public void create_Data_Spark_Postgres(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Parquet2CSV() + script.CSV2Posgres();
        ParquetCSV convertParquet2CSV = new ParquetCSV();
        String PY = convertParquet2CSV.Parquet2CSV();
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", PY);
    }
    
    public void create_Data_Spark_Hive(Move_Data Data) {//In, String DatabaseIn, String SchemaIn, String From, String To, String DataOut, String DatabaseOut) {
        String NameOp = "Move_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Parquet2CSV() + script.CSV2HDFS() + script.HDFS2Hive();
        ParquetCSV convertParquet2CSV = new ParquetCSV();
        String PY = convertParquet2CSV.Parquet2CSV();
    
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, "convertParquet2CSV.py", PY);
    } 
    public void create_Data_SQL_Postgres(Move_Data Data, String SQL){
        String NameOp = "SQL_"+Data.get_From()+"_"+Data.get_To();
        Script script = new Script();
        String sh = script.top_sh(Data) + script.Postgres_SQL(SQL);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sh", sh);
        createfile(OperatorFolder + "/" + NameOp, NameOp + ".sql", SQL);
        
    }           
    public void createfile(String OperatorFolder,String filename, String content){        
        Writer writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(OperatorFolder+"/"+filename), "utf-8"));
            writer.write(content);
        } 
        catch (IOException ex) {
                // report
        } 
        finally {
            try {writer.close();} 
            catch (Exception ex) {/*ignore*/}
        }
    }
}



