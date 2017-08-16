/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package LibraryIres;

import com.sparkexample.App;

/**
 *
 * @author letrung
 */
public class Script {
    String SPARK_HOME = new App().readhome("SPARK_HOME");
    String HADOOP_HOME = new App().readhome("HADOOP_HOME");
    String HIVE_HOME = new App().readhome("HIVE_HOME");
    String HDFS = new App().readhome("HDFS");
    public String top_sh2(Move_Data Data) {
        String operator = "Move";
        if (Data.get_From()==Data.get_To()) operator = "SQL";
        String NameOp = operator+"_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String top_sh = "#!/bin/bash\n" +
"\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +                
"HDFS="+HDFS+"\n" +
"BASE=/mnt/Data/tmp\n" +
"DATABASE=$1\n" +
"TABLE=$2\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
"SPARK_PORT=local[*]\n" +
"SCHEMA=$3\n";
        return top_sh;
    }
    public String top_sh(Move_Data Data) {
        String operator = "Move";
        if (Data.get_From()==Data.get_To()) operator = "SQL";
        String NameOp = operator+"_"+Data.get_From()+"_"+Data.get_To();
        String Database_In = Data.get_DatabaseIn();
        String Database_Out = Data.get_DatabaseOut();
        String Schema = Data.get_Schema();//"(CUSTKEY int, NAME varchar(25), ADDRESS varchar(40), NATIONKEY int, PHONE varchar(25), ACCTBAL float, MKTSEGMENT varchar(15), COMMENT varchar(120), LAST varchar(10))";
        String Table_In = Data.get_DataIn();
        String Table_Out = Data.get_DataOut();
        String top_sh = "#!/bin/bash\n" +
"\n" +
"echo -e \""+NameOp+"\\n\"\n" +
"\n" +
"export HADOOP_HOME="+HADOOP_HOME+"\n" +
"export SPARK_HOME="+SPARK_HOME+"\n" +
"export HIVE_HOME="+HIVE_HOME+"\n" +                
"HDFS="+HDFS+"\n" +
"BASE=/mnt/Data/tmp\n" +
"DATABASE="+Database_In+"\n" +
"TABLE="+Table_In+"\n" +
"DATABASE_OUT="+Database_Out+"\n" +
"TABLE_OUT="+Table_Out+"\n" +            
"SPARK_PORT=local[*]\n" +
"SCHEMA=\""+Schema+"\"\n";
        return top_sh;
    }
    public String Hive2CSV() {
        String Hive2CSV = "echo \"exporting table from HIVE\"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"else\n" +
"	rm /mnt/Data/tmp/$TABLE/*\n" +
"fi\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyToLocal $HDFS/$DATABASE.db/$TABLE/* /mnt/Data/tmp/$TABLE\n" +
"if [ ! -f /mnt/Data/tmp/$TABLE/$TABLE.csv ]\n" +
"then\n" +
"	for x in $(ls /mnt/Data/tmp/$TABLE/*);\n" +
"	do\n" +
"		#echo $x\n" +
"		cat $x >> /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"	done\n" +
"fi\n" +
"chmod a+rw /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE.csv\n" +
"mv /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE_OUT.csv\n" +  
"ls -ltr\n"+ "\n";
        return Hive2CSV;
    }
    public String CSV2HDFS() {
        String CSV2HDFS = "echo -e \"Uploading $TABLE.csv to HDFS\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$HADOOP_HOME/bin/hdfs dfs -copyFromLocal /mnt/Data/tmp/$TABLE.csv $HDFS\n" + "\n";
        return CSV2HDFS;
    }
    public String HDFS2Parquet() {
        String HDFS2Parquet = "echo -e \"Converting $TABLE.csv to parquet\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE_OUT.parquet\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertCSV2Parquet.py $TABLE $TABLE_OUT\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +                
"rm -r /mnt/Data/tmp/$TABLE\n" +
"rm -r /mnt/Data/tmp/$TABLE_OUT.csv\n" +                
"rm -r /mnt/Data/tmp/$TABLE.csv\n"+ "\n";
        return HDFS2Parquet;
    }
    public String CSV2Posgres() {
        String username = System.getProperty("user.name");
        String CSV2Postgres = "echo \"loading table to POSTGRES\"\n" +                
"psql -U "+username+" -d $DATABASE_OUT -c \"DROP TABLE IF EXISTS $TABLE_OUT;\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"CREATE TABLE IF NOT EXISTS $TABLE_OUT $SCHEMA;\"\n" +
"psql -U "+username+" -d $DATABASE_OUT -c \"COPY $TABLE_OUT FROM '/mnt/Data/tmp/$TABLE_OUT.csv' WITH DELIMITER AS '|';\"\n" +
"rm /mnt/Data/tmp/$TABLE_OUT.csv\n" +
"rm /mnt/Data/tmp/$TABLE.csv\n" +                
"rm -r /mnt/Data/tmp/$TABLE\n"+ "\n";
        return CSV2Postgres;
    }
    public String Postgres2CSV() {
        String username = System.getProperty("user.name");
        String Postgres2CSV = "echo \"loading Postgres to CSV\"\n" + 
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"fi\n" +                
"psql -U "+username+" -d $DATABASE -c \"COPY (SELECT * FROM $TABLE) TO '/mnt/Data/tmp/tmp.csv' WITH (DELIMITER '|', FORMAT csv)\"\n" +
"cp /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE\n" +
"mv /mnt/Data/tmp/$TABLE/tmp.csv /mnt/Data/tmp/$TABLE.csv\n" +
"mv /mnt/Data/tmp/tmp.csv /mnt/Data/tmp/$TABLE_OUT.csv\n"+ "\n";
        return Postgres2CSV;
    }
    public String HDFS2Hive() {
        String HDFS2Hive = "echo \"loading table to HIVE\"\n" + 
"cd $HIVE_HOME\n" +
"$HIVE_HOME/bin/hive -e \"DROP TABLE IF EXISTS $DATABASE_OUT.$TABLE_OUT; CREATE TABLE IF NOT EXISTS $DATABASE_OUT.$TABLE_OUT $SCHEMA ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; LOAD DATA INPATH '$HDFS/$TABLE.csv' OVERWRITE INTO TABLE $DATABASE_OUT.$TABLE_OUT;\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" + 
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE\n" +                
"rm /mnt/Data/tmp/$TABLE_OUT.csv\n" +
"rm /mnt/Data/tmp/$TABLE.csv\n" +                
"rm -r /mnt/Data/tmp/$TABLE\n" + "\n";
        return HDFS2Hive;
    }
    public String Parquet2CSV() {
        String Parquet2CSV = "echo \"Exporting table $TABLE from Spark\"\n" +
"echo \"Converting Parquet $TABLE.parquet to CSV\"\n" +
"$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS/$TABLE.csv\n" +
"$SPARK_HOME/bin/spark-submit --executor-memory 2G --driver-memory 512M  --packages com.databricks:spark-csv_2.10:1.4.0 --master $SPARK_PORT convertParquet2CSV.py $HADOOP_HOME $TABLE\n" +
"\n" +
"if [ ! -e /mnt/Data/tmp/$TABLE ]\n" +
"then\n" +
"	mkdir -p /mnt/Data/tmp/$TABLE\n" +
"	chmod -R a+wrx /mnt/Data/tmp\n" +
"fi\n" +
"$HADOOP_HOME/bin/hdfs dfs -getmerge $HDFS/$TABLE.csv /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +

"sed -i 's/|\\\"/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +
"sed -i 's/\\\"|/|/g' /mnt/Data/tmp/$TABLE/$TABLE.csv\n" +

"chmod a+rw /mnt/Data/tmp/$TABLE/$TABLE.csv \n" +
"cp /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE.csv\n" +   
"mv /mnt/Data/tmp/$TABLE/$TABLE.csv /mnt/Data/tmp/$TABLE_OUT.csv\n" +                
"chmod a+rw /mnt/Data/tmp/$TABLE.csv \n"+ "\n";
        return Parquet2CSV;
    }
    public String Postgres_SQL(String postgres_SQL){
        String username = System.getProperty("user.name");
        String SQL = "psql -U "+username+" -d $DATABASE -c \""+postgres_SQL+"\"\n";
        if ("".equals(postgres_SQL))
        return "";
        else return SQL;
    } 
    
}
