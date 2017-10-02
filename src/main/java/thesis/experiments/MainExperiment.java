package thesis.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.multilevel_optimization.SequentialAlgorithm;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.code_generators.TaskTableGenerator;
import thesis.enumeration_algorithms.DPccpException;

public class MainExperiment {
	static String dirname;
	//Clique goes after cycle, Star goes after clique
	static String[] types = {"Chain", "Cycle", "Clique", "Star", "Mixed"};
	static int numberOfRelations = 100;
	static int numberOfSites = 9;
	static int[] querySize = {20, 40, 60, 80, 100};
	static int[] _k = {5, 4, 4, 4, 4};
	static int numberOfQueries = 6;
	
	public static void collectResults() throws IOException{
		//for(int j : querySize){
		String dir = dirname+20+"-Queries/";
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];

			for(int q=1; q<numberOfQueries; q++)
			{
				PrintWriter p = new PrintWriter(dir+type+"/AllResultsQuery"+q+".dat");
				for(int k = 2; k<=12; k++){
					BufferedReader br = new BufferedReader(new FileReader(dir+type+"/Result"+q+":k"+k+".txt"));
					String cost = br.readLine().split(" ")[0];
					p.print(k+" "+cost);
					if(k<7 || q < 10)
						p.println();
					br.close();
				}
				p.close();
			}

		}
		//}
	}

	public static void setUp() throws Exception{
		generateCatalog();
		String sites_filename = dirname+"SystemSites.conf";
		String relations_filename = dirname+"Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);

		for(int i : querySize){
			numberOfRelations=i;
			generateQueries(dirname+i+"-Queries/");
		}
	}

	public static void generateCatalog() throws CatalogException, FileNotFoundException{
		System.out.println("D: "+dirname);
		CatalogGenerator catalogGenerator = new CatalogGenerator(numberOfRelations, 
				numberOfSites, numberOfSites, dirname);
		catalogGenerator.generate();
	}

	public static void generateQueries(String dir) throws Exception{
		//Generating queries
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];
			for(int q=1; q<numberOfQueries; q++)
			{
				QueryGenerator gen = new QueryGenerator(numberOfRelations, dir+type+"/Query"+q);
				System.out.println("Generated Query at: "+dir+type+"/Query"+q);
				if(type.equals("Chain"))
					gen.generateChain();
				else if(type.equals("Cycle"))
					gen.generateCycle();
				else if(type.equals("Clique"))
					gen.generateClique();
				else if(type.equals("Star"))
					gen.generateStar();
				else if(type.equals("Mixed"))
					gen.generateMixed();
				else throw new Exception("Unrecognised graph type");
				//Copy generated file into distributed 
			}
		}
	}
	public static void executeQueries(String queryDir, String resultsDestDir) throws NumberFormatException, IOException, JoinGraphException, ParseException, CatalogException, SchedulerException, DPccpException, QueryPlanTreeException{
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];
			for(int q=1; q<numberOfQueries; q++)
			{
				int k = _k[i];
				JoinGraph g = new JoinGraph(queryDir+type+"/Query"+q);
				long start = System.currentTimeMillis();
				SequentialAlgorithm seq = new SequentialAlgorithm(g);
				Plan tree = seq.optimize(k);
				long end = System.currentTimeMillis();
				System.out.println("Executed: "+queryDir+type+"/Query"+q+":k"+k);
				tree.outputToFile(resultsDestDir+type+"/tree"+q+":k"+k+".tex");

				TaskTree taskTree = new TaskTree(tree);

				//Create task table
				TaskTableGenerator gen = new TaskTableGenerator(taskTree);
				gen.generateCode(resultsDestDir+type+"/TaskTable"+q+".tex");

				PrintWriter p = new PrintWriter(resultsDestDir+type+"/Result"+q+":k"+k+".txt");
				p.println(tree.getCost().toString());
				p.println((end-start));
				p.close();

				Scheduler scheduler = tree.getSchedule();
				scheduler.outputToFile(resultsDestDir+type+"/schedule"+q+":k"+k+".txt");


			}
		}
	}
}
