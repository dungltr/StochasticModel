package thesis.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.code_generators.TaskTableGenerator;

import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;

import thesis.multilevel_optimization.SequentialAlgorithm;

import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;

import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.IDP1ccp;

public class Experiment8part8 {

	static int[] numberOfSites = {9};
	static int[] sizeOfQueries = {20, 40, 60, 80, 100};
	static String[] types = {"Chain", "Clique", "Cycle", "Star", "Mixed"};
	static int numberOfQueries = 5;
	static String[] algorithms = {"IDP1ccp"};
	static String dirname; 

	public static void main(String args[]){
		//produceOptimalityGraphML();
		try {
			mainExperiment(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		//generateQueries();
	}
	/*
	 * Run experiments for seqML and IDPccp.
	 */
	public static void mainExperiment(String args[]) throws Exception
	{
		Plan tree = null;
		long start, end;

		for(int numSites : numberOfSites){
			//int numSites=1;
			dirname = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment8.9/CompareML-"+numSites+"Sites/";
			String sites_filename = dirname+"SystemSites.conf";
			String relations_filename = dirname+"Catalog.conf";
			Catalog.populate(sites_filename, relations_filename);
			for(String algorithm : algorithms){
				//String algorithm = "SeqML";
				for(String type : types){
					//String type = "Chain";
					for(int sizeOfQuery : sizeOfQueries){
						//int sizeOfQuery = 100;
						String outputDir = dirname+algorithm+"/"+sizeOfQuery+"-Queries/"+type+"/";
						int k = getKValue(algorithm, type, sizeOfQuery, numSites);
						if(k==-1)
							continue;
						else
						{

							//int q = 2;
							//if(type.equals("Mixed")){
							//	q=1;
							//}

							for(int l = 1; l<21; l++){
								JoinGraph graph = new JoinGraph(dirname+sizeOfQuery+"-Queries/"+type+"/Query"+l);
								start = System.currentTimeMillis();

								if(algorithm.equals("SeqML")){
									SequentialAlgorithm seq = new SequentialAlgorithm(graph);
									tree = seq.optimize(k);
								}
								else if(algorithm.equals("IDP1ccp")){
									IDP1ccp idp = new IDP1ccp(graph, k);
									tree= idp.enumerate();
								}
								else throw new Exception("unrecognised algorithm");

								end = System.currentTimeMillis();
								outputResult(tree, outputDir, l, (end-start));
								updateProgressFile(sizeOfQuery, type, l, numSites, (end-start));
								//System.out.println("COMLPETED: "+numSites+", "+algorithm+", "
								//		+type+", "+sizeOfQuery+", "+l+" "+(end-start));
							}

						}
					}
				}
			}
		}
	}

	private static void updateProgressFile(int querySize2, String type, int q, int numSites2, long time) throws IOException {
		FileWriter p = new FileWriter(System.getenv().get("HOME") + "/progress.txt", true);
		p.write("Sites: "+numSites2+" Type: "+type+" QuerySize: "+querySize2+" QueryNum: "+q+" COMPLETED: "+time+"\n");
		p.close();
	}
	
	private static void produceOptimalityGraphML() throws IOException{
		String line = "";
		final String basedirname = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment8.8/";

		for(int numSites : numberOfSites){
			String workingDir = basedirname+"CompareML-"+numSites+"Sites/";
			for(int j : sizeOfQueries){
				for(int i = 0; i<types.length; i++){
					String type = types[i];
					PrintWriter p = new PrintWriter(workingDir+"OptimalityComparisonGraph-Query"+j+"-"+type+".dat");

					for(int k = 1; k<6; k++){
						p.print(k+" ");
						BufferedReader brSeq = 
							new BufferedReader(new FileReader(workingDir+"SeqML/"+j+"-Queries/"+
									type+"/Result"+k+".txt"));
						String s = brSeq.readLine();
						String[] temp = s.split(" ");

						double seq = Double.parseDouble(temp[0]);
						brSeq.close();

						BufferedReader brDist = 
							new BufferedReader(new FileReader(workingDir+"DistML/"+j+"-Queries/"+
									type+"/Result"+k+".txt"));
						s = brDist.readLine();
						temp = s.split(" ");

						double dist = Double.parseDouble(temp[0]);
						brSeq.close();

						try{
							BufferedReader brIDP1 = 
								new BufferedReader(new FileReader(workingDir+"IDP1ccp/"+j+"-Queries/"+
										type+"/Result"+k+".txt"));
							s = brIDP1.readLine();
							temp = s.split(" ");

							double idp = Double.parseDouble(temp[0]);
							brIDP1.close();
							double min = Math.min(Math.min(seq, dist), idp);
							line+=(seq/min)+" "+(dist/min)+" "+(idp/min);
						}
						catch(FileNotFoundException e){
							double min = Math.min(seq, dist);
							line+=(seq/min)+" "+(dist/min)+" M";
						}
						p.println(line);
						line="";
					}
					p.close();
				}

			}
		}
	}

	public static void generateQueries() throws Exception{
		//Generating queries

		final String basedirname = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment8.8/";

		for(int s : numberOfSites){
			String workingDir = basedirname+"CompareML-"+s+"Sites/";
			String sites_filename = workingDir+"SystemSites.conf";
			String relations_filename = workingDir+"Catalog.conf";
			Catalog.populate(sites_filename, relations_filename);
			for(int numRel : sizeOfQueries){
				for(int i = 0; i<types.length; i++)
				{
					String type = types[i];
					for(int q=11; q<21; q++)
					{
						QueryGenerator gen = new QueryGenerator(numRel, workingDir+numRel+"-Queries/"+type+"/Query"+q);
						System.out.println("Generated Query at: "+workingDir+numRel+"-Queries/"+type+"/Query"+q);

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
		}
	}
	private static void outputResult(Plan tree, String outputDir, int q, long time) 
	throws FileNotFoundException, SchedulerException 
	{
		tree.outputToFile(outputDir+"/tree"+q+".tex");
		TaskTree taskTree = new TaskTree(tree);
		//Create task table
		TaskTableGenerator gen = new TaskTableGenerator(taskTree);
		gen.generateCode(outputDir+"/TaskTable"+q+".tex");
		PrintWriter p = new PrintWriter(outputDir+"/Result"+q+".txt");
		p.println(tree.getCost().toString());
		p.println(time);
		p.close();
		Scheduler scheduler = tree.getSchedule();
		scheduler.outputToFile(outputDir+"/schedule"+q+".txt");

	}

	private static int getKValue(String algorithm, String type, int querySize, 
			int numberOfSites) throws Exception
			{
		if(numberOfSites!=1 && numberOfSites!=3 && numberOfSites!=9)
			throw new Exception("Unrecognised number of sites!");

		if(algorithm.equals("IDP1ccp") || algorithm.equals("DistML") || algorithm.equals("SeqML")){
			BufferedReader br = new BufferedReader(new FileReader(System.getenv().get("HOME") + "/"+algorithm+"KValues.txt"));
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

}
