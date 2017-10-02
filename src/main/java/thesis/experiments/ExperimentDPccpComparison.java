package thesis.experiments;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;
import thesis.code_generators.TaskTableGenerator;

import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;

import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.DPccp;
import thesis.enumeration_algorithms.DPccpException;

public class ExperimentDPccpComparison {

	static String dir = "/auto/ecslab/c09rt/Desktop/assembla_svn/Experiments/DPccpExperiment8.7/";
	static String[] types = {"Chain"};//"Star", "Mixed", "Clique"};
	static int[] querySize = {20, 40, 60, 80, 100};
	static int[] numSites = {1};//,3};//,5,9};
	
	public static void main(String args[]) throws IOException, CatalogException, 
	ParseException, NumberFormatException, JoinGraphException, SchedulerException, DPccpException, QueryPlanTreeException{
		for(int j = 0; j<types.length; j++){
			for(int i = 0; i<querySize.length; i++){
				executeAtAllSites(querySize[i], types[j]);
			}
		}
	}

	private static void executeAtAllSites(int querySize, String type) throws IOException, 
	CatalogException, ParseException, NumberFormatException, JoinGraphException, SchedulerException,
	DPccpException, QueryPlanTreeException {
		for(int k = 0; k<numSites.length; k++){
			String sites_filename = dir+numSites[k]+"Sites/SystemSites.conf";
			String relations_filename = dir+numSites[k]+"Sites/Catalog.conf";
			Catalog.populate(sites_filename, relations_filename);
			for(int q = 1; q<6; q++){
				JoinGraph graph = new JoinGraph(dir+numSites[k]+"Sites/"+querySize+"-Queries/"+type+"/Query"+q);
				graph.outputToFile("Star.dot");
				DPccp d = new DPccp(graph);
				
				long start = System.currentTimeMillis();
				Plan tree = d.enumerate(false).getFirst();
				long end = System.currentTimeMillis();
				
				//updateProgressFile(querySize, type, q, numSites[k], (end-start));
				System.out.println(querySize+" "+type+" "+ q+ " "+numSites[k]+" " +(end-start));
				String resultsDestination = dir+numSites[k]+"Sites/Results/"+type+"/"+querySize+"Queries/";
				tree.outputToFile(resultsDestination+"/tree"+q+".tex");

				TaskTree taskTree = new TaskTree(tree);

				//Create task table
				TaskTableGenerator gen = new TaskTableGenerator(taskTree);
				gen.generateCode(resultsDestination+"/TaskTable"+q+".tex");

				PrintWriter p = new PrintWriter(resultsDestination+"/Result"+q+".txt");
				p.println(tree.getCost().toString());
				p.println((end-start));
				p.close();

				Scheduler scheduler = tree.getSchedule();
				scheduler.outputToFile(resultsDestination+"/schedule"+q+".txt");
			}
			
		}
	}

	private static void updateProgressFile(int querySize2, String type, int q, int numSites2, long time) throws IOException {
		FileWriter p = new FileWriter(dir+"progress.txt", true);
		p.write("Sites: "+numSites2+" Type: "+type+" QuerySize: "+querySize2+" QueryNum: "+q+" COMPLETED: "+time+"\n");
		p.close();
	}

}
