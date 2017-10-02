package thesis.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.multilevel_optimization.SequentialAlgorithm;

import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanCost;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.utilities.Pair;

import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.DPccp;
import thesis.enumeration_algorithms.DPccpException;

public abstract class QualityTestWithK {
	//static String dirname;
	//Clique goes after cycle, Star goes after clique
	static String[] types = {"Star"};
	//static int numberOfRelations = 50;
	static int[] sizeOfQueries = {20}; 
	
	//TODO: NEED TO DO "Chain", "Cycle", "Star" for 200.
	//			 Clique for 200, 300, 400, 500

	protected static int getKValue(String algorithm, String type, int querySize, 
			int numberOfSites) throws Exception
			{
		if(numberOfSites!=1 && numberOfSites!=3 && numberOfSites!=9)
			throw new Exception("Unrecognised number of sites!");

		if(algorithm.equals("IDP1ccp") || algorithm.equals("DistML") || algorithm.equals("SeqML")){
			BufferedReader br = new BufferedReader(new FileReader("/home/rob/Desktop/"+algorithm+"KValues.txt"));
			String line;
			while((line=br.readLine())!=null){
				String[] parts = line.split(",");
				int lineQuerySize = Integer.parseInt(parts[0]);
				String lineType = parts[1];
				int kValue = 0;

				switch(numberOfSites){
				case 1: 
					if(!parts[2].equals("-")){ 
						kValue=Integer.parseInt(parts[2]);
					}
					else
						kValue=-1;
					break;
				case 3: 
					if(!parts[3].equals("-")){ 
						kValue=Integer.parseInt(parts[3]);
					}
					else
						kValue=-1;
					break;
				case 9: 
					if(!parts[4].equals("-")){ 
						kValue=Integer.parseInt(parts[4]);
					}
					else
						kValue=-1;
					break;
				}

				if(lineQuerySize==querySize && lineType.equals(type))
					return kValue;
			}
			throw new Exception("K value not found!");
		}
		else throw new Exception("unrecognised algorithm");
			}

	public static void collectResults(String _basedir) throws Exception{
		for(int j : sizeOfQueries){
			//int j = 60;
			for(int i = 0; i<types.length; i++)
			{
				String type = types[i];
				String outputDir = _basedir+"SeqML/"+j+"-Queries/"+type+"/";
				int maxK = getKValue("SeqML", type, j, 3);

				for(int q=1; q<21; q++)
				{
				//int q = 8;
					PrintWriter p = new PrintWriter(outputDir+"AllResultsQuery"+q+".dat");
					for(int k = 2; k<=maxK; k++){
						BufferedReader br = new BufferedReader(new FileReader(outputDir+"Result"+q+":k"+k+".txt"));
						String[] lineParts = br.readLine().split(" ");
						p.print(k+" "+lineParts[0]);
						if(k<maxK)
							p.println();
						br.close();
					}
					p.close();
				}

		}
		}
	}

	/*
	public static void setUp() throws Exception{
		generateCatalog(dirname);
		String sites_filename = dirname+"/SystemSites.conf";
		String relations_filename = dirname+"/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);

		for(int i : sizeOfQueries){
			numberOfRelations=i;
			generateQueries(dirname+"/"+i+"-Queries/");
		}
	}

	public static void generateCatalog(String dir) throws CatalogException, FileNotFoundException{
		CatalogGenerator catalogGenerator = new CatalogGenerator(numberOfRelations, 
				numberOfSites, numberOfSites, dir);
		catalogGenerator.generate();
	}
	 */
	public static void generateQueries() throws Exception{
		//Generating queries
		
		String dir = System.getenv().get("HOME") + "/assembla_svn/Experiments/QualityTestWithK/randomRelSites_1Sites/";
		for(int n : sizeOfQueries){
			for(int i = 0; i<types.length; i++)
			{
				String type = types[i];
				for(int q=1; q<21; q++)
				{
					QueryGenerator gen = new QueryGenerator(n, dir+n+"-Queries/"+type+"/Query"+q);
					if(type.equals("Chain"))
						gen.generateChain();
					else if(type.equals("Cycle"))
						gen.generateCycle();
					else if(type.equals("Clique"))
						gen.generateClique();
					else if(type.equals("Star"))
						gen.generateStar();
					else throw new Exception("Unrecognised graph type");
				}
			}
		}
	}
}
