package thesis.experiments;

import com.sparkexample.App;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.DPccp;
import thesis.enumeration_algorithms.DPccpException;
import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanCost;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.utilities.Pair;

import java.io.*;
import java.text.ParseException;

//import java.io.*;

public class DPccpExperiment {
	static String HOME_Thesis = new App().readhome("HOME_Thesis");
	static String dirname;
	//Clique goes after cycle, Star goes after clique
	static String[] types = {"Chain", "Cycle", "Clique", "Star"};
	static int numberOfRelations = 2;
	static int numberOfSites = 15;

	public static void main(String args[]) throws Exception, IOException{
		for(int i = 2; i<5; i++){
			numberOfRelations=i;
			dirname = HOME_Thesis + "/assembla_svn/Experiments/DPccpTest/Relations_resident_at_all_site/"+numberOfSites
			+"_Site/"+numberOfSites+
			"_Site_"+numberOfRelations+"_Relations";
			File f = new File(dirname);
                        System.out.println("create dir");
			if(!f.exists()) {
                            f.mkdirs();
                            System.out.println("creating dir suscessful");
                        }
			generateCatalog();
			String sites_filename = dirname+"/SystemSites.conf";
			String relations_filename = dirname+"/Catalog.conf";
			Catalog.populate(sites_filename, relations_filename);

			makeDirectoryStructure();
			generateQueries();
			executeQueries();
			produceAverageTimeFile();
			collectResults();
		}
	}

	public static void collectResults() throws IOException{
		PrintWriter p = new PrintWriter(dirname+"/Results.txt");
		p.print(numberOfRelations+" ");
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];
			BufferedReader br = new BufferedReader(
					new FileReader(dirname+"/"+type+"/AverageResult.txt"));
			String time = br.readLine();
			p.print(time);
			
			if(i<types.length-1)
				p.print(" ");
		}
		p.close();
	}
	
	public static void produceAverageTimeFile() throws IOException{
		int total = 0;
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];
			for(int q=1; q<11; q++)
			{
				if(type.equals("Chain") && q==1){
				}
				else{
					BufferedReader br = new BufferedReader(
							new FileReader(dirname+"/"+type+"/query"+q+"/Result"+q+".txt"));
					br.readLine();
					String time = br.readLine();
					total+=Integer.parseInt(time);
					br.close();
				}
				
			}
			double average = (double)total/(type.equals("Chain") ? 9.0 : 10.0);
			PrintWriter p = new PrintWriter(dirname+"/"+type+"/AverageResult.txt");
			p.println(average);
			p.close();
			total = 0;
		}
	}

	public static void generateCatalog() throws CatalogException, FileNotFoundException{
		CatalogGenerator catalogGenerator = new CatalogGenerator(numberOfRelations, 
				numberOfSites, numberOfSites, dirname);
		catalogGenerator.generate();

	}
	public static void executeQueries() throws NumberFormatException, IOException, JoinGraphException, ParseException, CatalogException, SchedulerException, DPccpException, QueryPlanTreeException{
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];
			for(int q=1; q<11; q++)
			{
				JoinGraph g = new JoinGraph(dirname+"/"+type+"/query"+q+"/Query"+q);
				long start = System.currentTimeMillis();
				DPccp d = new DPccp(g);
				Pair<Plan, PlanCost> tree = d.enumerate(false);
				long end = System.currentTimeMillis();
				System.out.println("Executed: "+dirname+"/"+type+"/query"+q+"/Query"+q);
				tree.getFirst().outputToFile(dirname+"/"+type+"/query"+q+"/tree"+q+".tex");
				PrintWriter p = new PrintWriter(dirname+"/"+type+"/query"+q+"/Result"+q+".txt");
				p.println(tree.getSecond().toString());
				p.println((end-start));
				p.close();

				Scheduler scheduler = tree.getFirst().getSchedule();
				scheduler.outputToFile(dirname+"/"+type+"/query"+q+"/schedule"+q+".txt");
			}
		}
	}
	/**
	 * Make directory structure from DPccpTest directory.
	 */
	public static void makeDirectoryStructure(){
		for(int i = 0; i<types.length; i++){
			File f = new File(dirname+"/"+types[i]);
			f.mkdir();
			makeQueryDirectories(dirname+"/"+types[i]);
		}
	}

	public static void makeQueryDirectories(String parentdir){
		for(int q=1; q<11; q++)
		{
			//making directory structure
			File f = new File(parentdir+"/query"+q);
			f.mkdir();
		}
	}

	public static void generateQueries() throws Exception{
		//Generating queries
		for(int i = 0; i<types.length; i++)
		{
			String type = types[i];
			for(int q=1; q<11; q++)
			{
				QueryGenerator gen = new QueryGenerator(numberOfRelations, dirname+"/"+type+"/query"+q+"/Query"+q);
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

