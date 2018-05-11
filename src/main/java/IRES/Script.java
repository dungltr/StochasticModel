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
 * @author letrung
 */
public class Script {
    String KEY =  new App().readhome("key");
    String SPARK_HOME = new App().readhome("SPARK_HOME");
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String IRES_HOME = new App().readhome("IRES_HOME");
    String HDFS = new App().readhome("HDFS");
    String NODE_PC = new App().readhome("NODE_PC");
    String username = new App().readhome("username");
    String username_remote = new App().readhome("username_remote");

    String password = new App().readhome("password");
    String ipmaster = new App().readhome("ipmaster");
    String base_in = new App().readhome("BASE_IN");
    String base_out = new App().readhome("BASE_OUT");
    String POSTGRES_HOME = new App().readhome("POSTGRES_HOME");
    String ASAP_HOME = IRES_HOME;
    String IRES_library = ASAP_HOME+"/asap-platform/asap-server";
    String directory_operator = IRES_library+"/target/asapLibrary/operators/";
    String workflow = IRES_library+"/target/asapLibrary/workflows/";
    public String top_sh(Move_Data Data) {
        String NameOp = runWorkFlowIRES.Nameop(Data);
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = "\""+Data.get_Schema()+"\"";//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataIn();
        String OperatorDirectory = directory_operator+NameOp;
        String top_sh = "#!/bin/bash\n" +
"START=$(date +%s)\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export key="+KEY+"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" + 
"export POSTGRES_HOME="+POSTGRES_HOME+"\n" +                
"export OperatorDirectory="+OperatorDirectory+"\n" +                
"export NameOp="+NameOp+"\n" +
"export NameOpRemote="+NameOp+"_remote.sh"+"\n" + 
"export username="+username+"\n"+
"export username_remote="+username_remote+"\n"+
"export master="+ipmaster+"\n"+ 
//"export password="+password+"\n"+                
"HDFS="+HDFS+"\n" +
"export BASE=/mnt/Data/tmp\n" +
"export BASE_IN="+base_in+"\n" +
"export BASE_OUT="+base_out+"\n" +
"#DATABASE=$1\n" +
"export DATABASE="+Database_In+"\n" +                
"#TABLE=$2\n" +
"export TABLE="+Table_In+"\n" + 
"#SCHEMA=$3\n"+ 
"export SCHEMA="+Schema+"\n"+  
"SPARK_PORT=$4\n" +
"#SPARK_PORT=local[*]\n" +                
"export DATABASE_OUT="+Database_Out+"\n" +
"export TABLE_OUT="+Table_Out+"\n" +            
//"#rm -r "+workflow+"*"+"\n"+
"echo -e \"BASE_IN = \" $BASE_IN\n" +
"echo -e \"BASE_OUT = \" $BASE_OUT\n" +
"echo -e \"KEY = \" $key\n" +
"echo -e \"DATABASE = \" $DATABASE\n" +
"echo -e \"TABLE = \" $TABLE\n" +
"echo -e \"SCHEMA = \" $SCHEMA\n" +
"echo -e \"SPARK_PORT = \" $SPARK_PORT\n"+
"echo -e \"POSTGRES_HOME = \"$POSTGRES_HOME\n"+
"echo -e \"USERNAME = \"$username\n"+
"echo -e \"USERNAME_REMOTE = \"$username_remote\n"+
"echo -e \"REMOTE_HOST = \"$master\n";        
        return top_sh;
    }  
    public String top_sh_remote(Move_Data Data) {
        String NameOp = runWorkFlowIRES.Nameop(Data);
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = "\""+Data.get_Schema()+"\"";//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataIn();
        String OperatorDirectory = directory_operator+NameOp;
        String top_sh = "#!/bin/bash\n" +
"START=$(date +%s)\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export key="+KEY+"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" + 
"export POSTGRES_HOME="+POSTGRES_HOME+"\n" +                
"export OperatorDirectory="+OperatorDirectory+"\n" +                
"export NameOp="+NameOp+"\n" +
"export NameOpRemote="+NameOp+"_remote.sh"+"\n" + 
"export username="+username+"\n"+
"export username_remote="+username_remote+"\n"+
"export master="+ipmaster+"\n"+ 
//"export password="+password+"\n"+                
"HDFS="+HDFS+"\n" +
"export BASE=/mnt/Data/tmp\n" +
"export BASE_IN="+base_in+"\n" +
"export BASE_OUT="+base_out+"\n" +
"#DATABASE=$1\n" +
"export DATABASE="+Database_In+"\n" +                
"#TABLE=$2\n" +
"export TABLE="+Table_In+"\n" + 
"#SCHEMA=$3\n"+ 
"export SCHEMA="+Schema+"\n"+  
"#SPARK_PORT=$4\n" +
"SPARK_PORT=local[*]\n" +                
"export DATABASE_OUT="+Database_Out+"\n" +
"export TABLE_OUT="+Table_Out+"\n" +            

"echo -e \"BASE_IN = \" $BASE_IN\n" +
"echo -e \"BASE_OUT = \" $BASE_OUT\n" +
"echo -e \"KEY = \" $KEY\n" +
"echo -e \"DATABASE = \" $DATABASE\n" +
"echo -e \"TABLE = \" $TABLE\n" +
"echo -e \"SCHEMA = \" $SCHEMA\n" +
"echo -e \"SPARK_PORT = \" $SPARK_PORT\n"+
"echo -e \"POSTGRES_HOME = \"$POSTGRES_HOME\n"+
"echo -e \"USERNAME = \"$username\n"+
"echo -e \"USERNAME_REMOTE = \"$username_remote\n"+
"echo -e \"REMOTE_HOST = \"$master\n";        
        return top_sh;
    }
    public String bottom_sh() {
        String bottom_sh = "END=$(date +%s)\n" +
"DIFF=$(echo \"$END - $START\" | bc)\n" +
"echo \"It takes DIFF=$DIFF seconds to complete this task...\"";
        return bottom_sh;
    }
    public String Hive2CSV() {
        String Hive2CSV = "echo -e \"exporting table from HIVE\"\n" +
"if [ ! -e $BASE_OUT/$TABLE ]\n" +
"then\n" +
"	mkdir -p $BASE_OUT/$TABLE\n" +
"	chmod -R a+wrx $BASE_OUT/\n" +
"else\n" +
"	rm -r $BASE_OUT/$TABLE/*\n" +
"fi\n" +
"#$HADOOP_HOME/bin/hdfs dfs -copyToLocal $HDFS/$DATABASE.db/$TABLE/* /mnt/Data/tmp/$TABLE\n" +               
"$HIVE_HOME/bin/hive -e \"INSERT OVERWRITE LOCAL DIRECTORY '$BASE_OUT/$TABLE' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' select * from $DATABASE.$TABLE;\"\n"+                
"if [ ! -f $BASE_OUT/$TABLE/$TABLE.csv ]\n" +
"then\n" +
"	for x in $(ls $BASE_OUT/$TABLE/*);\n" +
"	do\n" +
"		echo $x\n" +
"		cat $x >> $BASE_OUT/$TABLE/$TABLE.csv\n" +
"	done\n" +
"fi\n" +
"chmod a+rw $BASE_OUT/$TABLE/$TABLE.csv\n" +
"#cp $BASE_OUT/$TABLE/$TABLE.csv $BASE_OUT/$TABLE/$TABLE.csv\n" +
"#mv $BASE_OUT/$TABLE/$TABLE.csv $BASE_OUT/$TABLE_OUT.csv\n" +  
"#ls -ltr\n"+ "\n";
        return Hive2CSV;
    }
    public String HIVE2CSV_remote(){
        String HIVE2CSV_remote = "echo -e \"Copy CSV from remote to local\"\n" +
"if [ ! -e $BASE_IN/$TABLE ]\n" +
"then\n" +
"	mkdir -p $BASE_IN/$TABLE\n" +
"	chmod -R a+wrx $BASE_IN/tmp\n" +
"else\n" +
"	rm $BASE_IN/$TABLE/*\n" +
"fi\n" +
"ssh -i $key $username_remote@$master 'bash -s' < $OperatorDirectory/$NameOpRemote \n"+
"scp -i $key $username_remote@$master:$BASE_OUT/$TABLE/$TABLE.csv $BASE_IN/$TABLE/$TABLE.csv \n"+
"ssh -i $key $username_remote@$master \"rm -r $BASE_OUT/$TABLE/*\"\n"  ;
        return HIVE2CSV_remote;
    }
    public String POSTGRES2CSV_remote(){
        String POSTGRES2CSV_remote = "echo -e \"Copy CSV from remote to local\"\n" +
"if [ ! -e $BASE_IN/$TABLE ]\n" +
"then\n" +
"	mkdir -p $BASE_IN/$TABLE\n" +
"	sudo chmod -R a+wrx $BASE_IN/STABLE\n" +
"else\n" +
"	rm $BASE_OUT/$TABLE/*\n" +
"fi\n" +
"ssh -i $key $username_remote@$master 'bash -s' < $OperatorDirectory/$NameOpRemote \n"+
"scp -i $key $username_remote@$master:$BASE_OUT/$TABLE/$TABLE.csv $BASE_IN/$TABLE/$TABLE.csv \n"+
"ssh -i $key $username_remote@$master \"rm -r $BASE_OUT/$TABLE/*\"\n"  ;
        return POSTGRES2CSV_remote;
    }
    
    public String CSV2HDFS() {
        String CSV2HDFS = "echo -e \"Uploading $TABLE.csv to HDFS\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -mkdir $HDFS/$TABLE\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyFromLocal $BASE_IN/$TABLE/$TABLE.csv $HDFS/$TABLE\n";               
        return CSV2HDFS;
    }
    public String HDFS2Parquet(Move_Data Data) {
        String operator = "Move";
        String NameOp = operator+"_"+Data.get_From()+"_"+Data.get_To();
        String OperatorDirectory = directory_operator+NameOp+"/";
        String HDFS2Parquet = "echo -e \"Converting $TABLE.csv to parquet\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE_OUT.parquet\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT "+OperatorDirectory+"convertCSV2Parquet.py $TABLE $TABLE_OUT\n" +
"#$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +                
"rm -r /mnt/Data/tmp/$TABLE\n" +
"#rm -r /mnt/Data/tmp/$TABLE_OUT.csv\n" +                
"#rm -r /mnt/Data/tmp/$TABLE.csv\n"+ "\n";
        return HDFS2Parquet;
    }
    public String CSV2Posgres() {
        String username = System.getProperty("user.name");
        String CSV2Postgres = "echo \"loading table to POSTGRES\"\n" +                
"psql -U "+username+" -d $DATABASE_OUT -c \"DROP TABLE IF EXISTS $TABLE_OUT;\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA;\"\n" +
//"psql -U "+username+" -d $DATABASE_OUT -c \"\\COPY $TABLE_OUT FROM '$BASE_IN/$TABLE/$TABLE.csv' WITH DELIMITER AS '|';\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"\\COPY $TABLE_OUT FROM '$BASE_IN/$TABLE/$TABLE.csv' WITH( FORMAT CSV, DELIMITER ',', ESCAPE '\\', NULL '\\N' );\"\n" +
//"SQL_CSV2Posgres=\"DROP TABLE IF EXISTS $TABLE_OUT; CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA; COPY $TABLE_OUT FROM '$BASE_IN/$TABLE/$TABLE.csv' WITH DELIMITER AS '|';\"\n" +
//"#rm /mnt/Data/tmp/$TABLE_OUT.csv\n" +
//"#rm /mnt/Data/tmp/$TABLE.csv\n" +                
                
"#rm -r $BASE_IN/$TABLE/*\n"+ "\n";
        return CSV2Postgres;
    }
    public String Postgres2CSV() {
        String username = System.getProperty("user.name");
        String Postgres2CSV = "echo \"loading Postgres to CSV\"\n" + 
"if [ ! -e $BASE_OUT/$TABLE ]\n" +
"then\n" +
"	mkdir -p $BASE_OUT/$TABLE\n" +
"	chmod -R a+wrx $BASE_OUT/$TABLE\n" +
"fi\n" +                
"psql -U "+username+" -d $DATABASE -c \"\\COPY (SELECT * FROM $TABLE) TO '$BASE_OUT/$TABLE/$TABLE.csv' WITH (DELIMITER '|', FORMAT csv)\"\n" +
"#cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE\n" +
"#mv /mnt/Data/tmp/$TABLE/tmp.csv /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#mv /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE.csv\n"+ "\n";
        return Postgres2CSV;
    }
    public String HDFS2Hive() {
        String HDFS2Hive = "echo \"loading table to HIVE\"\n" + 
"cd $HIVE_HOME\n" +
"$HIVE_HOME/bin/hive -e \"DROP TABLE IF EXISTS $DATABASE_OUT.$TABLE_OUT; CREATE TABLE IF NOT EXISTS $DATABASE_OUT.$TABLE_OUT $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; LOAD DATA INPATH '$HDFS/$TABLE/$TABLE.csv' OVERWRITE INTO TABLE $DATABASE_OUT.$TABLE_OUT;\"\n" +
"#$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" + 
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +                
"#rm /mnt/Data/tmp/$TABLE_OUT.csv\n" +
"#rm /mnt/Data/tmp/$TABLE.csv\n" +                
"#rm -r $BASE_IN/$TABLE\n" + "\n";
        return HDFS2Hive;
    }
    public String Parquet2CSV(Move_Data Data) {
        String operator = "Move";
        String NameOp = operator+"_"+Data.get_From()+"_"+Data.get_To();
        String OperatorDirectory = directory_operator+NameOp+"/";
        String Parquet2CSV = "echo \"Exporting table $TABLE from Spark\"\n" +
"echo \"Converting Parquet $TABLE.parquet to CSV\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT "+OperatorDirectory+"convertParquet2CSV.py $HADOOP_HOME $TABLE\n" +
"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"fi\n" +
"$HADOOP_HOME/bin/hdfs dfs -getmerge $HDFS/$TABLE.csv /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +

"sed -i 's/|\\\"/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"#sed -i 's/\\\"|/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +

"chmod a+rw /mnt/Data/tmp/$TABLE/$TABLE.csv \n" +
"#cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE.csv\n" +   
"#mv /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE_OUT.csv\n" +                
"#chmod a+rw /mnt/Data/tmp/$TABLE.csv \n"+ 
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" + "\n";
        return Parquet2CSV;
    }
    public String Postgres_SQL(String postgres_SQL){
        String username = System.getProperty("user.name");
        String SQL = "psql -U "+username+" -d $DATABASE_OUT -c \""+postgres_SQL+"\"\n";
        if ("".equals(postgres_SQL))
        return "";
        else return SQL;
    } 
    public String TPCH_Postgres_SQL(Move_Data Data, String TPCH_SQL){
        String username = System.getProperty("user.name");
        String NameOp = Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To();
        //String SQL = "$POSTGRES_HOME/psql -U "+username+" -d $DATABASE_OUT -c \""+TPCH_SQL+"\"\n";// + 
	String SQL = "psql -U "+username+" -d $DATABASE_OUT -c \""+TPCH_SQL+"\"\n";// + 
//                "rm -r /mnt/Data/tmp/$TABLE\n"+ "\n";
//        String SQL = "psql -U "+username+" -d $DATABASE -f "+directory_operator+NameOp+"/"+NameOp+".sql"+"\n";
        System.out.println("\n"+directory_operator+NameOp+"/"+TPCH_SQL);
        if ("".equals(TPCH_SQL))
        return "";
        else return SQL;
    }
    public String Hive_SQL(Move_Data Data, String Hive_SQL){
        String username = System.getProperty("user.name");        
        String SQL = "$HIVE_HOME/bin/hive -e \"USE "+Data.get_DatabaseIn()+";"+Hive_SQL+"\"\n";
        if ("".equals(Hive_SQL))
        return "";
        else return SQL;
    }
    public String TPCH_Hive_SQL(Move_Data Data, String Hive_SQL){
        String username = System.getProperty("user.name");        
        String NameOp = Data.get_Operator()+"_"+Data.get_From()+"_"+Data.get_To();
	String SQL = "$HIVE_HOME/bin/hive -e \"USE "+Data.get_DatabaseIn()+";"+Hive_SQL+"\"\n";
        if ("".equals(Hive_SQL))
        return "";
        else return SQL;
    }
    public String Hive_SQL_remote(Move_Data Data, String Hive_SQL){
        String HIVE_remote =
        "ssh -i $key $username_remote@$master 'bash -s' < $OperatorDirectory/$NameOpRemote \n";
        return HIVE_remote;
    }
    public String Postgres_SQL_remote(Move_Data Data, String Hive_SQL){
        String POSTGRES_remote =
        "ssh -i $key $username_remote@$master 'bash -s' < $OperatorDirectory/$NameOpRemote \n";
        return POSTGRES_remote;
    }

/*    public String readSQL(String filename) {
       String sCurrentLine = "nothing";
       try (BufferedReader br = new BufferedReader(new FileReader(System.getenv().get("HOME")+"/"+filename+".txt"))) {
			while ((sCurrentLine = br.readLine()) != null) {
                                return sCurrentLine;
			}

		} catch (IOException e) {
			e.printStackTrace();                       
		}
       return sCurrentLine;
    }
*/
}
