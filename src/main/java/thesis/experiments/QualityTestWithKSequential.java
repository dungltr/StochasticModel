package thesis.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.code_generators.TaskTableGenerator;

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
import thesis.enumeration_algorithms.DPccpException;
import thesis.enumeration_algorithms.IDP1ccp;

public class QualityTestWithKSequential extends QualityTestWithK{
	static String _basedir; 

	public static void executeQueries() throws Exception{
		_basedir = System.getenv().get("HOME") + "/assembla_svn/Experiments/QualityTestWithK/" +
		"randomRelSites_3Sites/";

		//int j = 100;
		for(int j : sizeOfQueries){

			for(int i = 0; i<types.length; i++)
			{
				String type = types[i];
				String workingDir =_basedir+j+"-Queries/"+type+"/";
				String outputDir = _basedir+"SeqML/"+j+"-Queries/"+type+"/";
				int maxK = 11;//getKValue("SeqML", type, j, 1);

				if(maxK==-1)
					continue;
				else{
					for(int q=1; q<21; q++)
					{
						//int q = 20;
						for(int k = 2; k<=maxK; k++){
							String sites_filename = _basedir+"SystemSites.conf";
							String relations_filename = _basedir+"Catalog.conf";
							Catalog.populate(sites_filename, relations_filename);

							JoinGraph g = new JoinGraph(workingDir+"Query"+q);

							long start = System.currentTimeMillis();
							SequentialAlgorithm d = new SequentialAlgorithm(g);
							Plan tree = d.optimize(k);
							long end = System.currentTimeMillis();

							PrintWriter p = new PrintWriter(outputDir+"Result"+q+":k"+k+".txt");
							p.println(tree.getCost().toString());
							p.println((end-start));
							p.close();
							FileWriter f = new FileWriter(System.getenv().get("HOME") + "/progress.txt", true);
							f.write("K: "+k+", "+j+"-Queries, "+type+", Query"+q+" COMPLETED\n");
							f.close();
						}
					}
				}
			}
		}

	}

	public static void executeQuery(int q) throws Exception{
		int[] sites= {1};//,3,9};

		for(int s : sites){
			_basedir = "/home/rob/Desktop/";
			//_basedir = "/home/rob/Desktop/QualityTestWithK/" +
			//"randomRelSites_"+s+"Sites/";
			//_basedir = System.getenv().get("HOME") + "/";
			//int j = 200;
			//for(int j : sizeOfQueries){

			//for(int i = 0; i<types.length; i++)
			//{
			//	String type = "Chain";//types[i];
			//_basedir+j+"-Queries/"+type+"/";
			String outputDir = "/home/rob/Desktop/";//_basedir+"IDP1ccp/"+j+"-Queries/"+type+"/";
			String type = "Chain";
			int k = 0;
			boolean idp = true;
			
			if(idp){
				k=getKValue("IDP1ccp", type, q, s);
			}
			else{
				k=getKValue("SeqML", type, q, s);
				double fLevelSize = -1;
				int kMax = -1;

				for(int i = 2; i<=k; i++){
					double numLevels = Math.ceil(q/(i-1)); 
					double finalLevel = q-(numLevels-1)*(i-1);

					if(finalLevel>=fLevelSize){
						fLevelSize=finalLevel;
						kMax=i;
					}
				}
				if(kMax==-1) throw new Exception("error finding k");
				k = kMax;
			}
			
			
			
			
			
			if(k==-1)
				continue;
			else{
				//for(int q=20; q<21; q++)
				//{
				//int q = 500;
				String sites_filename = _basedir+"SystemSites.conf";
				String relations_filename = _basedir+"Catalog.conf";
				Catalog.populate(sites_filename, relations_filename);

			//	String workingDir =_basedir+"SeqML/"+q+"-Queries/"+type+"/";

				JoinGraph g = new JoinGraph(_basedir+"Query-"+type+"-"+q);//_basedir+q+"-Queries/"+type+"/"+"Query"+2);

				long start = System.currentTimeMillis();
				Plan tree=null;
				if(idp){
					IDP1ccp d = new IDP1ccp(g, k);
					tree = d.enumerate();
				}
				else{
					SequentialAlgorithm d = new SequentialAlgorithm(g);
					tree = d.optimize(k);//.enumerate();
				}
				
				long end = System.currentTimeMillis();
				System.out.println("Time taken: "+(end-start));
				
				PrintWriter p = new PrintWriter(outputDir+"Result"+type+".txt");
				p.println(tree.getCost().toString());
				p.println((end-start));
				p.close();
				tree.getSchedule().outputToFile("/home/rob/workspace/CentralizedAlgorithm/schedule.txt");
				TaskTableGenerator gen = new TaskTableGenerator(new TaskTree(tree));
				gen.generateCode("/home/rob/workspace/CentralizedAlgorithm/Tasktable.tex");
				//FileWriter f = new FileWriter(System.getenv().get("HOME") + "/progress.txt", true);
				//f.write("num of sites: "+s+", "+j+"-Queries, "+type+", Query"+q+" COMPLETED\n");
				//f.close();
				//}
			}
			//	}
			//}
		}
	}

	public static void generateQuery(String type, int s, int numRelations) throws Exception{
		//Generating queries

		final String basedirname = System.getenv().get("HOME") + "/";
		String sites_filename = basedirname+"SystemSites.conf";
		String relations_filename = basedirname+"Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);

		QueryGenerator gen = new QueryGenerator(numRelations, basedirname+"QueryTest"+numRelations);

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

	public static void produceEPSGraphs() throws IOException{
		String dir = "/home/rob/Desktop/QualityTestWithK/";
		int[] sites = {1,3,9};
		int[] querySize = {20, 40, 60, 80, 100};
		String[] types = {"Chain", "Cycle", "Star", "Clique","Mixed"};
		String[] algorithms = {"SeqML", "DistML"};

		for(String alg : algorithms){
			for(int s : sites){
				for(int qs : querySize){
					for(String type : types){
						String workingDir = dir+"randomRelSites_"+s+"Sites/"+alg+"/"+qs+"-Queries/"
						+type+"/";
						Runtime rt = Runtime.getRuntime();
						System.out.println(workingDir);

						Process proc = rt.exec("/home/rob/Desktop/assembla_svn/" +
								"scripts/QualityTestWithK/graphAveraged.sh "+workingDir+
								"averagedGraphData "+type+" "+s+" "+alg+" "+qs);

						proc = rt.exec("epstopdf "+workingDir+"averagedGraphData.eps");
						//				"scripts/QualityTestWithK/graphSingle.sh "+workingDir+
						//				"averagedGraphData "+type+" "+s+" "+alg);
					}
				}
			}
		}
	}

	/**
	 * Goes through files and plots graphs of type containing 5
	 * sets of bar charts, each for the num of queries
	 * 
	 * chart type
	 * 20	40	60...
	 * S
	 * @param dir
	 * @throws Exception 
	 */
	private static void produceComparisonGraphs() throws Exception{

		int[] sites = {1,3,9};
		int[] querySize = {20,40,60,80,100};
		String[] types = {"Chain", "Cycle", "Star", "Clique"};

		for(int s : sites){
			String dir = "/home/rob/Desktop/QualityTestWithK/randomRelSites_"+s+"Sites/";
			for(String type : types){
				
				for(int qs : querySize){
					PrintWriter p = new PrintWriter(dir+type+qs+"-Comparison.dat");
					//0 : IDP, 1: SeqML, 2: DistML
					double[][] scaledResults = new double[20][3];
					boolean includeIDP = true;

					for(int i = 1; i<21; i++){
						double idp1ccp = getCostOfPlan(type, qs, "IDP1ccp",s,i);
						if(idp1ccp==-1)
							includeIDP=false;
						
						if(includeIDP){
							double distML = getCostOfPlan(type, qs, "DistML",s,i);
							double seqML = getCostOfPlan(type, qs, "SeqML",s,i);
							double min = Math.min(Math.min(distML, seqML),idp1ccp);
							//scaledResults[i-1][0] = idp1ccp/min;
							scaledResults[i-1][0] = distML/min;
							scaledResults[i-1][1] = seqML/min;
							scaledResults[i-1][2] = idp1ccp/min;
						}
						else{
							double distML = getCostOfPlan(type, qs, "DistML",s,i);
							double seqML = getCostOfPlan(type, qs, "SeqML",s,i);
							double min = Math.min(distML, seqML);
							//scaledResults[i-1][0] = idp1ccp/min;
							scaledResults[i-1][0] = distML/min;
							scaledResults[i-1][1] = seqML/min;
						}
					}
					double avg_idp1ccp = 0;
					double avg_distML = 0;
					double avg_seqML = 0;

					for(int i = 1; i<21; i++){
						avg_distML+=scaledResults[i-1][0];
						avg_seqML+=scaledResults[i-1][1];
						avg_idp1ccp+=scaledResults[i-1][2];
					}
					avg_idp1ccp/=20.0;
					avg_distML/=20.0;
					avg_seqML/=20.0;

					double minAvg = 1;
					if(includeIDP){
						minAvg = Math.min(Math.min(avg_distML, avg_seqML),avg_idp1ccp);
						avg_idp1ccp/=minAvg;
					}
					else
						minAvg = Math.min(avg_distML, avg_seqML);
					
					avg_distML/=minAvg;
					avg_seqML/=minAvg;
					p.print(qs+" "+avg_seqML+" "+avg_distML);
					
					if(includeIDP)
						p.print(" "+avg_idp1ccp);
					else {
						p.print(" NaN");
					}
					
					p.close();
					//if(qs!=100)
					//	p.println();
				}
				
			}
		}
	}

	private static double getCostOfPlan(String type, int qs, String alg, int s, int queryNum) throws Exception {
		if(!type.equals("Clique") && !type.equals("Chain") && !type.equals("Cycle") && 
				!type.equals("Star"))
			throw new Exception("Invalid Type");

		if(!alg.equals("IDP1ccp") && !alg.equals("DistML") && !alg.equals("SeqML"))
			throw new Exception("Invalid algorithms");

		int k = getKValue(alg, type, qs, s);
		if(alg.equals("IDP1ccp") || type.equals("Clique")){

		}
		else if (type.equals("Star")){
			k = 2;
		}
		else if (type.equals("Chain") || type.equals("Cycle")){
			/*String filename = "/home/rob/Desktop/QualityTestWithK/randomRelSites_"+s+"Sites/"
			+alg+"/"+qs+"-Queries/"+type+"/averagedGraphData.dat";
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = "";
			int minK = 0;
			double minCost = Double.MAX_VALUE;
			
			while((line=br.readLine())!=null){
				String[] parts = line.split(" ");
				int kT = Integer.parseInt(parts[0]);
				double cost = Double.parseDouble(parts[1]);
				if(cost<=minCost){
					minK = kT;
					minCost = cost;
				}
			}
			br.close();
			if(minCost==Double.MAX_VALUE)
				throw new Exception("k not found");
			k=minK;*/
			double fLevelSize = -1;
			int kMax = -1;

			for(int i = 2; i<=k; i++){
				double numLevels = Math.ceil(qs/(i-1)); 
				double finalLevel = qs-(numLevels-1)*(i-1);

				if(finalLevel>=fLevelSize){
					fLevelSize=finalLevel;
					kMax=i;
				}
			}
			if(kMax==-1) throw new Exception("error finding k");
			k = kMax;
		}

		if(s!=9){
			System.out.println("Query num: "+queryNum+" K value chosen: "+alg+" "+type+" "+qs+" "+k+" sites: "+s);
		}
		
		if(k==-1)
			return -1;

		String filename = "/home/rob/Desktop/QualityTestWithK/randomRelSites_"+s+"Sites/"
		+alg+"/"+qs+"-Queries/"+type+"/Result"+queryNum+":k"+k+".txt";
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String l = br.readLine();
		br.close();
		return Double.parseDouble(l.split(" ")[0]);
	}

	public static void main(String args[]) throws Exception{
		//produceEPSGraphs();
		//executeQueries();
		//collectResults(_basedir);
		//}
		//for(int j = 1; j<11; j++){
		//for(int i : querySize)
		//		collectQueryResults(dirname+20+"-Queries/", j);
		//}
		/*
		CatalogGenerator g = new CatalogGenerator(i, 1, 1, System.getenv().get("HOME") + "/");
		g.generate();
		generateQuery("Clique", 1, i);

		;*/
		/*String sites_filename = System.getenv().get("HOME") + "/SystemSites.conf";
		String relations_filename = System.getenv().get("HOME") + "/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		generateQueries();*/
		//int i = 120;
		//executeQuery(i);
		//executeQueries();
		//collectResults(System.getenv().get("HOME") + "/assembla_svn/Experiments/QualityTestWithK/" +
		//	"randomRelSites_3Sites/");
		//createAverageKDataFiles();
		//createAverageGraphs();
		//produceEPSGraphs();
		produceComparisonGraphs();
	}

	public static void createAverageGraphs() throws IOException{
		String dir = "/home/rob/Desktop/QualityTestWithK/";

		int[] sites = {1,3,9};
		int[] querySize = {20, 40, 60, 80, 100};
		String[] types = {"Chain", "Cycle", "Star", "Clique","Mixed"};
		String[] algorithms = {"SeqML", "DistML"};

		for(String alg : algorithms){
			for(int s : sites){
				for(int qs : querySize){
					for(String type : types){
						String workingDir = dir+"randomRelSites_"+s+"Sites/"+alg+"/"+qs+"-Queries/"
						+type+"/";
						Runtime rt = Runtime.getRuntime();
						Process proc = rt.exec("/home/rob/Desktop/assembla_svn/" +
								"scripts/QualityTestWithK/graphAveraged.sh "+workingDir+"averagedGraphData "+
								type+" "+s+" "+alg);

					}
				}
			}
		}
	}

	public static void createAverageKDataFiles() throws Exception{
		String dir = "/home/rob/Desktop/QualityTestWithK/";

		int[] sites = {1,3,9};
		int[] querySize = {20, 40, 60, 80, 100};
		String[] types = {"Chain", "Cycle", "Star", "Clique","Mixed"};
		String[] algorithms = {"SeqML", "DistML"};

		for(String alg : algorithms){
			for(int s : sites){
				for(int qs : querySize){
					for(String type : types){
						int kMax = getKValue(alg, type, qs, s);
						if(kMax!=-1){
							double[] scaledRunningSum = new double[kMax];
							String workingDir = dir+"randomRelSites_"+s+"Sites/"+alg+"/"+qs+"-Queries/"+type+"/";
							double validFiles = 20;

							for(int q = 1; q<21; q++){


								System.out.println("At: "+alg+" "+type+" "+qs+" "+q+" Site:"+s);
								double[] current = new double[kMax];
								double min = Double.MAX_VALUE;

								try{
									BufferedReader br = new BufferedReader(new FileReader(workingDir+"AllResultsQuery"+q+".dat"));
									String line = "";

									while((line=br.readLine())!=null){
										String[] parts = line.split(" ");
										int k = Integer.parseInt(parts[0]);
										double d = Double.parseDouble(parts[1]);
										current[k-1]=d;
										if(d<=min)
											min=d;
									}
									br.close();
									if(min==Double.MAX_VALUE){
										validFiles--;
										continue;
									}

									for(int i = 1; i<current.length; i++){
										current[i]=current[i]/min;
										scaledRunningSum[i]+=current[i];
									}
								}
								catch(FileNotFoundException e){
									validFiles--;
									System.out.println("VF "+validFiles);
								}	

							}

							if(validFiles==0)
								throw new Exception("There were no valid files.");

							for(int i = 1; i<scaledRunningSum.length; i++){
								scaledRunningSum[i]/=validFiles;
								//System.out.println("Dividing by "+validFiles);
							}

							PrintWriter p = new PrintWriter(workingDir+"averagedGraphData.dat");
							for(int i = 1; i<scaledRunningSum.length; i++){
								p.print((i+1)+" "+scaledRunningSum[i]);
								if(i<scaledRunningSum.length-1) 
									p.println();
							}
							p.close();
						}
					}
				}
			}
		}
	}
}
