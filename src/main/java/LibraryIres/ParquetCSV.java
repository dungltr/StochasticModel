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
public class ParquetCSV {
    public String Parquet2CSV() {
        String py = "from pyspark import SparkContext, SparkConf\n" +
"from pyspark.sql import SQLContext, HiveContext\n" +
"from pyspark.sql.types import *\n" +
"\n" +
"import os\n" +
"import sys\n" +
"import subprocess\n" +
"\n" +
"def main():\n" +
"	conf = SparkConf().setAppName( \"Convert Parquet2CSV\")\n" +
"	sc = SparkContext( conf=conf)\n" +
"	sqlContext = SQLContext(sc)\n" +
"\n" +
"	yarn_home = sys.argv[ 1]\n" +
"        file = sys.argv[ 2]\n" +
"	print( yarn_home, file)\n" +
"        namenode = \"hdfs://localhost:9000\"\n" +
"        warehouse = \"/user/hive/warehouse\"\n" +
"        output = warehouse + \"/\" + file + \".csv\"\n" +
"        df = sqlContext.read.parquet( namenode + warehouse + \"/\" + file  + \".parquet\")\n" +
"        df.printSchema()\n" +
"		\n" +
"\n" +
"#        if os.path.exists( namenode + output):\n" +
"#            try:\n" +
"#                os.system( \"/opt/hadoop-2.7.0/bin/hdfs dfs -rm -r \" + output)\n" +
"#            except OSError:\n" +
"#                raise\n" +
"\n" +
"#        df.write.format( \"com.databricks.spark.csv\").options( delimiter='|').save( namenode  + output)\n" +
"        df.write.save(namenode + output, format=\"com.databricks.spark.csv\")\n" +
"        #merge output file to one\n" +
"        #fnames = subprocess.check_output( \"/opt/hadoop-2.7.0/bin/hdfs dfs -ls \" + output, shell = True)\n" +
"        #fnames = [ x for x in fnames.split() if x.startswith( warehouse) and not x.endswith( \"_SUCCESS\")]\n" +
"        #for file_part in fnames:\n" +
"         #   print( \"FILE IS:\", file_part)\n" +
"            #forschema = file.split( \"/\")[-1].split( \".\")[0]\n" +
"            #fileSchema = fileSchemas[ forschema]\n" +
"            #print( \"FILESCHEMA IS \", fileSchema)\n" +
"          #  df = sqlContext.read.format( \"com.databricks.spark.csv\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\").load( file_part)\n" +
"           # df.printSchema()\n" +
"            #df.write.format( \"com.databricks.spark.csv\").save( output + \"/\" + file + \".csv\", mode = \"append\")\n" +
"\n" +
"if __name__ == \"__main__\":\n" +
"    main()";
        return py;
    }
    public String CSV2Parquet () {
        String py = "from pyspark import SparkContext, SparkConf\n" +
"from pyspark.sql import SQLContext, HiveContext\n" +
"from pyspark.sql.types import *\n" +
"\n" +
"import os\n" +
"import sys\n" +
"import argparse\n" +
"import traceback\n" +
"import subprocess\n" +
"\n" +
"def main():\n" +
"\n" +
"	#define command line arguments\n" +
"	parser = argparse.ArgumentParser( description='Converts CSV files to Parquet and vice versa.')\n" +
"\n" +
"	parser.add_argument( 'src', metavar='sources', type=str, nargs='+',\n" +
"						 help='the directory where the files to be converted reside or a series of files'\n" +
"						 		+ ' to be converted.\\n\\n')\n" +
"\n" +
"	parser.add_argument( '--d', metavar='conversiondirection', type=str, nargs='*', choices=( 'parquet', 'csv'), default='parquet',\n" +
"						 help='define which conversion will take place i.e. from csv to parquet or vice versa. By default,'\n" +
"						 		+ ' the conversion is from csv to parquet. For the opposite conversion user should pass the' \n" +
"						 		+ ' the value \"csv\"\\n\\n')\n" +
"\n" +
"	parser.add_argument( '--sep', metavar='separator', type=str, default=' ',\n" +
"						 help='the column separator of each data file. Default -> \" \" i.e. space')\n" +
"\n" +
"	parser.add_argument( '--out', metavar='output', type=str, nargs='?', default='.',\n" +
"						 help='the output files will be saved in folder \"converted\" but the parent directory of this folder'\n" +
"						 		+ ' can be altered by user through this flag. Default -> . i.e. the output path is the current working'\n" +
"						 		+ ' directory\\n\\n')\n" +
"\n" +
"	args = parser.parse_args()\n" +
"\n" +
"	#results exportation\n" +
"	#path = args.out + \"/converted\"\n" +
"	#if not os.path.exists( path):\n" +
"	#	try: \n" +
"	#		os.makedirs( path)\n" +
"	#	except OSError:\n" +
"	#		if not os.path.isdir( path):\n" +
"	#			raise\n" +
"	#			exit( 1)\n" +
"\n" +
"	#read data files\n" +
"	#datafiles = []\n" +
"	#for file in args.src:\n" +
"		#check if it is a file or a directory\n" +
"	#	if os.path.isdir( file):\n" +
"	#		dfs = os.listdir( file)\n" +
"			#file -> directory, f -> actual file\n" +
"	#		dfs = [ file.rstrip( \"/\") + \"/\" + f for f in dfs if not os.path.isdir( f)]\n" +
"	#		datafiles = datafiles + dfs\n" +
"	#	else:\n" +
"	#		datafiles.append( file)\n" +
"\n" +
"\n" +
"	fileSchemas = {         \"customer\": StructType([	StructField( \"c_custkey\", IntegerType(), True),\n" +
"    						  					StructField( \"c_name\", StringType(), True),\n" +
"											  	StructField( \"c_address\", StringType(), True),\n" +
"					  						  	StructField( \"c_nationkey\", DecimalType( 38, 0), True),\n" +
"					  						  	StructField( \"c_phone\", StringType(), True),\n" +
"					  						  	StructField( \"c_acctbal\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"c_mktsegment\", StringType(), True),\n" +
"					  						  	StructField( \"c_comment\", StringType(), True)]),\n" +
"      				\"lineitem\": StructType([	StructField( \"l_orderkey\",DecimalType( 38, 0), True),\n" +
"											  	StructField( \"l_partkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"l_suppkey\", DecimalType( 38, 0), True),\n" +
"					  						  	StructField( \"l_linenumber\", IntegerType(), True),\n" +
"					  						  	StructField( \"l_quantity\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_extendedprice\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_discount\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_tax\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"l_returnflag\",StringType(), True),\n" +
"					  						  	StructField( \"l_linestatus\", StringType(), True),\n" +
"					  						  	StructField( \"l_shipdate\", DateType(), True),\n" +
"					  						  	StructField( \"l_commitdate\", DateType(), True),\n" +
"					  						  	StructField( \"l_receiptdate\", DateType(), True),\n" +
"					  						  	StructField( \"l_shipinstruct\", StringType(), True),\n" +
"					  						  	StructField( \"l_shipmode\", StringType(), True),\n" +
"					  						  	StructField( \"l_comment\", StringType(), True)]),\n" +
"      				\"nation\":   StructType([ 	StructField( \"n_nationkey\", IntegerType(), True),\n" +
"											  	StructField( \"n_name\", StringType(), True),\n" +
"											  	StructField( \"n_regionkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"n_comment\", StringType(), True)]),\n" +
"					\"orders\":   StructType([	StructField( \"o_orderkey\", IntegerType(), True),\n" +
"											  	StructField( \"o_custkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"o_orderstatus\", StringType(), True),\n" +
"					  						  	StructField( \"o_totalprice\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"o_orderdate\", DateType(), True),\n" +
"					  						  	StructField( \"o_orderpriority\", StringType(), True),\n" +
"					  						  	StructField( \"o_clerk\", StringType(), True),\n" +
"					  						  	StructField( \"o_shippriority\", IntegerType(), True),\n" +
"					  						  	StructField( \"o_comment\", StringType(), True)]),\n" +
"      				\"part\":     StructType([	StructField( \"p_partkey\", IntegerType(), True),\n" +
"											  	StructField( \"p_name\", StringType(), True),\n" +
"											  	StructField( \"p_mfgr\", StringType(), True),\n" +
"					  						  	StructField( \"p_brand\", StringType(), True),\n" +
"					  						  	StructField( \"p_type\", StringType(), True),\n" +
"					  						  	StructField( \"p_size\", IntegerType(), True),\n" +
"					  						  	StructField( \"p_container\", StringType(), True),\n" +
"					  						  	StructField( \"p_retailprice\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"p_comment\", StringType(), True)]),\n" +
"      				\"partsupp\": StructType([	StructField( \"ps_partkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"ps_suppkey\", DecimalType( 38, 0), True),\n" +
"											  	StructField( \"ps_availqty\", IntegerType(), True),\n" +
"					  						  	StructField( \"ps_supplycost\", StringType(), True),\n" +
"					  						  	StructField( \"ps_comment\", StringType(), True)]),\n" +
"      				\"region\":   StructType([	StructField( \"r_regionkey\", IntegerType(), True),\n" +
"											  	StructField( \"r_name\", StringType(), True),\n" +
"											  	StructField( \"r_comment\", StringType(), True)]),\n" +
"					\"supplier\":	StructType([	StructField( \"s_suppkey\", IntegerType(), True),\n" +
"                                              						  	StructField( \"s_name\", StringType(), True),\n" +
"                                                                                                StructField( \"s_address\", StringType(), True),\n" +
"					  						  	StructField( \"s_nationkey\", DecimalType( 38, 0), True),\n" +
"					  						  	StructField( \"s_phone\", StringType(), True),\n" +
"					  						  	StructField( \"s_acctbal\", DecimalType( 10, 2), True),\n" +
"					  						  	StructField( \"s_comment\", StringType(), True)]),\n" +
"      				\"part_agg\":   StructType([	StructField( \"agg_partkey\", DecimalType( 38, 0), True),\n" +
"                                                                StructField( \"avg_quantity\", DecimalType( 10, 2), True),\n" +
"                                                                StructField( \"agg_extendedprice\", DecimalType( 10, 2), True)])\n" +
"    			  }		  \n" +
"    			  \n" +
"	conf = SparkConf().setAppName( \"Convert CSV to Parquet\")\n" +
"	sc = SparkContext(conf=conf)\n" +
"	sqlContext = SQLContext(sc)\n" +
"        namenode = \"hdfs://localhost:9000\"\n" +
"        warehouse = \"/user/hive/warehouse\"\n" +
"        print( args.src)\n" +
"        forschema = args.src[ 0].split( \"/\")[-1].split( \".\")[0]\n" +
"        print( forschema)\n" +
"        output = args.src[ 1]\n" +
"        inputdir = warehouse \n" +
"        print(\"-------------------------------------------------\")\n" +
"        fnames = subprocess.check_output( \"hdfs dfs -ls \" + inputdir, shell = True)\n" +
"        fnames = [ x for x in fnames.split() if x.startswith( warehouse)]\n" +
"        for file in fnames:\n" +
"                print( \"FILE IS:\", file)\n" +
"                fileSchema = fileSchemas[ forschema]\n" +
"                print( \"FILESCHEMA IS \", fileSchema)\n" +
"#               df = sqlContext.read.format( \"com.databricks.spark.csv\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\").load( file, schema=fileSchema)\n" +
"                df = sqlContext.read.load(file, format =\"com.databricks.spark.csv\")\n" +
"                #, schema=fileSchema)\n" +
"                df.printSchema()\n" +
"                try:\n" +
"                	df.write.save(namenode + inputdir + \"/\" + output + \".parquet\", format = \"parquet\")\n" +
"#                    df.write.format(\".parquet\").save( namenode + warehouse + \"/\" + output, mode = \"append\").options( header=\"false\", delimiter=\"|\", inferSchema=\"true\")\n" +
"                except Exception:\n" +
"                    exc_type, exc_value, exc_traceback = sys.exc_info()\n" +
"                    formatted_lines = traceback.format_exc().splitlines()\n" +
"                    print(formatted_lines[0])\n" +
"                    print(formatted_lines[-1])\n" +
"                    print(\"*** format_exception:\")\n" +
"                    print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))\n" +
"                    pass\n" +
"\n" +
"if __name__ == \"__main__\":\n" +
"	main()";
        return py;
    }
    
}
