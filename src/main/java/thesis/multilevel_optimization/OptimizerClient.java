package thesis.multilevel_optimization;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import thesis.code_generators.TaskTableGenerator;

import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.loggers.Logger;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.catalog.RelationEntry;
import thesis.enumeration_algorithms.IDP1ccp;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.Site;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;
import thesis.utilities.Pair;

/**
 * This class provides the master site code for the distributed multilevel
 * optimizer.  It merges relations into temporary nodes called stubs at 
 * each level and sends them off to server sites to be optimized.  Final
 * plan is built in a top down fashion.  Sites of stubs determine the plan
 * to be fetched from the appropriate server node and attached onto bottom
 * of tree.
 * 
 * @author rob
 *
 */
public class OptimizerClient extends MultilevelOptimizer{
	protected String _nextIdentifierStub = "_Stub_1";
	protected OptimizationTaskPool _optimizationTaskPool;
	protected ConcurrentHashMap<String, Boolean> _stubOptimized; 
	protected Logger _logger;
	protected List<RelationEntry> _previousStubEntries;
	//protected static String dirname = 
	//	System.getenv().get("HOME") + "/assembla_svn/Experiments" +
	//	"/Experiment8.6_ComparisonML/2_Optimization_Sites/CompareML-9Sites/";

	public OptimizerClient(JoinGraph graph, Site s) throws NumberFormatException, NotBoundException, IOException{
		super(graph);
		_logger = new Logger(s, "Master");
		_stubOptimized = new ConcurrentHashMap<String, Boolean>();
		_previousStubEntries=new LinkedList<RelationEntry>();
		_optimizationTaskPool = new OptimizationTaskPool(_stubOptimized, s);
	}	

	protected String nextStubIdentifier(){
		int n = Integer.parseInt(_nextIdentifierStub.substring(6, _nextIdentifierStub.length()));
		_nextIdentifierStub = "_Stub_"+(++n);
		return _nextIdentifierStub;
	}

	public Plan optimize(int k) throws JoinGraphException, QueryPlanTreeException, 
	CatalogException, RemoteException, InterruptedException, OptimizationTaskPoolException
	{
		//if(k<=2) throw new JoinGraphException("Optimizer must have k value >2");
		String stubIdentifier = "";
		JoinGraph subgraph = null;

		while(_joinGraph.getNumberOfVertices()>k)
		{
			Pair<String, String> minSelEdge = _joinGraph.getMiniumSelectivityEdge();
			TreeSet<String> S = new TreeSet<String>();
			S.add(minSelEdge.getFirst());
			S.add(minSelEdge.getSecond());
			double intermediateResultOfS = Catalog.getCardinality(minSelEdge.getFirst())*
			Catalog.getCardinality(minSelEdge.getSecond())*
			_joinGraph.getJoinInfo(minSelEdge.getFirst(), minSelEdge.getSecond()).getSelectivity();
			String v = "";
			double intermediateResult = Double.MAX_VALUE;

			while(S.size()<k)
			{
				intermediateResult = Double.MAX_VALUE;
				Set<String> neighbours = _joinGraph.getNeighbours(S);

				for(String n : neighbours)
				{
					double l = calculateIntermediateResult(S, intermediateResultOfS, n);
					if(l<=intermediateResult)
					{
						v = n;
						intermediateResult = l;
					}
				}

				S.add(v);
				intermediateResultOfS = intermediateResult;
			}

			stubIdentifier = nextStubIdentifier();

			if(intermediateResultOfS==0)
				intermediateResultOfS=1;

			RelationEntry relationEntry = new RelationEntry(stubIdentifier, 
					Math.ceil(intermediateResultOfS));

			for(String s : S){
				RelationEntry entry = Catalog.getRelationEntry(s);
				relationEntry.conjunct(entry);
			}

			Catalog.addRelation(relationEntry);

			subgraph = _joinGraph.absorb(S, stubIdentifier);
			//if(stubIdentifier.compareTo("_Stub_11")>0){
			//	subgraph.outputToFile(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/multilevel_graphs/"+stubIdentifier+".dot");
			//	_joinGraph.outputToFile(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/multilevel_graphs/Level_"+_level+".dot");
			//}
			_level++;
			_stubGraphMapping.put(stubIdentifier, subgraph);
			_optimizationTaskPool.submit(
					new OptimizationTask(subgraph, _previousStubEntries, stubIdentifier, true));

			_previousStubEntries.add(relationEntry);
		}

		stubIdentifier = nextStubIdentifier();
		_stubGraphMapping.put(stubIdentifier, _joinGraph);
		_optimizationTaskPool.submit(
				new OptimizationTask(_joinGraph, _previousStubEntries, stubIdentifier, false));
		return buildFinalPlan();
	}

	/**
	 * Checks if all optimizations have completed.
	 * @return
	 */
	protected boolean receivedAllResults(){
		for(Entry<String, Boolean> entry : _stubOptimized.entrySet())
		{
			if(!entry.getValue())
				return false;
		}
		return true;
	}
	/**
	 * Build the final plan by recursively replacing stub nodes from the 
	 * top level plan with their corresponding optimized query plans.
	 * @param stubIdentifier
	 * @return
	 * @throws QueryPlanTreeException 
	 * @throws InterruptedException 
	 * @throws RemoteException 
	 * @throws OptimizationTaskPoolException 
	 */
	protected Plan buildFinalPlan() throws QueryPlanTreeException, InterruptedException, 
	RemoteException, OptimizationTaskPoolException {
		_logger.print("Waiting for all subplans to be optimized...");
		while(!receivedAllResults()){
			Thread.sleep(1);
		}
		_optimizationTaskPool.stop();

		Plan optimal = _optimizationTaskPool.pullOptimizedTree(
				_nextIdentifierStub, Catalog.getQuerySite());
		//optimal.outputToFile(System.getProperty("user.home")+
		//		"/workspace/CentralizedAlgorithm/multilevel_trees/"+_nextIdentifierStub+".tex");
		List<Pair<String, PlanNode>> stubs = getStubEntries(optimal);

		while(!stubs.isEmpty())
		{
			for(Pair<String, PlanNode> p : stubs)
			{
				Plan stub = _optimizationTaskPool.pullOptimizedTree(
						p.getFirst(), p.getSecond().getSite());
				//stub.outputToFile(System.getProperty("user.home")+
				//		"/workspace/CentralizedAlgorithm/multilevel_trees/"+p.getFirst()+".tex");
				p.getSecond().getParent().replaceChild(p.getSecond(), 
						stub.getRoot());
			}
			stubs = getStubEntries(optimal);
		}

		return optimal;
	}

	private static int getKValue(String algorithm, String type, String querySize, 
			String numberOfSites) throws Exception
			{
		if(!numberOfSites.equals("1") && numberOfSites.equals("3") && numberOfSites.equals("9"))
			throw new Exception("Unrecognised number of sites!");

		BufferedReader br = new BufferedReader(new FileReader(System.getenv().get("HOME") + "/DistMLKValues.txt"));
		String line;
		while((line=br.readLine())!=null){
			String[] parts = line.split(",");
			int lineQuerySize = Integer.parseInt(parts[0]);
			String lineType = parts[1];
			int kValue = 0;

			if(numberOfSites.equals("1")){
				if(!parts[2].equals("-")){ 
					kValue=Integer.parseInt(parts[2]);
				}
				else
					kValue=-1;
			}
			else if(numberOfSites.equals("3")){
				if(!parts[3].equals("-")){ 
					kValue=Integer.parseInt(parts[3]);
				}
				else
					kValue=-1;
			}
			else if(numberOfSites.equals("9")){
				if(!parts[4].equals("-")){ 
					kValue=Integer.parseInt(parts[4]);
				}
				else
					kValue=-1;
			}

			if(lineQuerySize==Integer.parseInt(querySize) && lineType.equals(type))
				return kValue;
		}
		throw new Exception("K value not found!");
			}

	public static void collectResults(String _basedir) throws Exception{
		for(int j : sizeOfQueries){
			//int j = 60;
			for(int i = 0; i<types.length; i++)
			{
				String type = types[i];
				String outputDir = _basedir+"DistML/"+j+"-Queries/"+type+"/";
				int maxK = getKValue("DistML", type, ""+j, ""+numberOfSites);
				
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
	public static void executeQueries(String j, String type) throws Exception{
		String workingDir = _basedir+j+"-Queries/"+type+"/";
		String outputDir = _basedir+"DistML/"+j+"-Queries/"+type+"/";
		int maxK = getKValue("DistML", type, j, ""+numberOfSites);

		for(int q=13; q<21; q++)
		{
			System.out.println("Executing query: "+q);
			for(int k = 2; k<=maxK; k++){
				String sites_filename = _basedir+"SystemSites.conf";
				String relations_filename = _basedir+"Catalog.conf";
				Catalog.populate(sites_filename, relations_filename);
				JoinGraph g = new JoinGraph(workingDir+"Query"+q);
				long start = System.currentTimeMillis();

				OptimizerClient client = new OptimizerClient(g,
						new Site(InetAddress.getLocalHost().getHostAddress()).internalize());
				Plan tree = client.optimize(k);
				
				long end = System.currentTimeMillis();
				PrintWriter p = new PrintWriter(outputDir+"Result"+q+":k"+k+".txt");
				p.println(tree.getCost().toString());
				p.println((end-start));
				p.close();			
				client._optimizationTaskPool.reCatalog();
			}
			FileWriter f = new FileWriter(System.getenv().get("HOME") + "/progress.txt", true);
			f.write("num of sites: "+numberOfSites+", "+j+"-Queries, "+type+", Query"+q+" COMPLETED\n");
			f.close();
		}
	}
	
	public static void executeQuery(int q) throws Exception{
		int[] sites= {1};//,3,9};

		for(int s : sites){
			_basedir = "/home/rob/Desktop/";
			
			String workingDir =_basedir;
			String type = "Chain";
			
			int k = getKValue("DistML", type, ""+q, ""+1);
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
			
			if(k==-1)
				continue;
			else{
				
					String sites_filename = _basedir+"SystemSites.conf";
					String relations_filename = _basedir+"Catalog.conf";
					Catalog.populate(sites_filename, relations_filename);

					JoinGraph g = new JoinGraph(workingDir+"Query-"+type+"200");

					long start = System.currentTimeMillis();
					OptimizerClient client = new OptimizerClient(g,
							new Site(InetAddress.getLocalHost().getHostAddress()).internalize());
					Plan tree = client.optimize(k);
					long end = System.currentTimeMillis();
					System.out.println("Time taken: "+(end-start));
					PrintWriter p = new PrintWriter(workingDir+"Result"+type+".txt");
					p.println(tree.getCost().toString());
					p.println((end-start));
					p.close();
			}
		}
	}

	public static void generateQuery(String type, int s, int numRelations) throws Exception{
		//Generating queries

		final String basedirname = "/home/rob/Desktop/";
		String sites_filename = basedirname+"SystemSites.conf";
		String relations_filename = basedirname+"Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		
		QueryGenerator gen = new QueryGenerator(numRelations, basedirname+"Query-"+type+"-"+numRelations);

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
	
	public static void main(String args[]) throws Exception{
		//int i = 200;
		
		//CatalogGenerator g = new CatalogGenerator(500, 1, 1, "/home/rob/Desktop/");
		//g.generate();
		generateQuery("Chain", 1, 120);
		//executeQuery(200);
	}
	
	//static int[] numberOfSites = {1,3,9};
	static int numberOfSites = 3;
	static int[] sizeOfQueries = {20, 40, 60, 80, 100};
	static String[] types = {"Chain", "Cycle", "Clique", "Star", "Mixed"};
	//static int numberOfQueries = 5;
	static String[] algorithms = {"DistML"};
	static String _basedir; 

	//public static void main(String args[]) throws Exception{
	//	int t = 1;	
	//	numberOfSites = t;
	//	_basedir = System.getenv().get("HOME") + "/assembla_svn/Experiments/QualityTestWithK/" +
	//	"randomRelSites_"+numberOfSites+"Sites/";
		/*if(args.length!=3)
			throw new Exception("Incorrect number of arguments");

		String numSites = args[0];
		String type = args[1];
		String sizeOfQuery = args[2];

		assert Integer.parseInt(numSites)==t;*/
		//executeQueries(sizeOfQuery, type);
		//collectResults(_basedir);
		//System.exit(0);
	//}
		
		
	/*public static void main(String args[]) throws Exception
	{
		if(args.length!=3)
			throw new Exception("Incorrect number of arguments");

		String algorithm = "DistML";
		String numSites = args[0];
		String type = args[1];
		String sizeOfQuery = args[2];
		QueryPlanTree tree = null;
		long start, end;

		//System.out.println(numSites+", "+algorithm+", "
		//		+type+", "+sizeOfQuery);
		//for(int numSites : numberOfSites){

		dirname = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment8.9/CompareML-"+numSites+"Sites/";
		String sites_filename = dirname+"SystemSites.conf";
		String relations_filename = dirname+"Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);

		String outputDir = dirname+algorithm+"/"+sizeOfQuery+"-Queries/"+type+"/";
		int k = getKValue(algorithm, type, sizeOfQuery, numSites);
		if(k!=-1){
			//int q = 2;
			//if(type.equals("Mixed")){
			//	q=1;
			//}

			for(int l = 1; l<21; l++){
				JoinGraph graph = new JoinGraph(dirname+sizeOfQuery+"-Queries/"+type+"/Query"+l);
				start = System.currentTimeMillis();
				OptimizerClient client = new OptimizerClient(graph,
						new Site(InetAddress.getLocalHost().getHostAddress()).internalize());
				tree = client.optimize(k);
				end = System.currentTimeMillis();
				outputResult(tree, outputDir, l, (end-start));
				updateProgressFile(sizeOfQuery, type, l, numSites, (end-start));
				//System.out.println("COMPLETED: "+numSites+", "+algorithm+", "
				//		+type+", "+sizeOfQuery+", "+l+" "+(end-start));
			}

		}
		System.exit(0);
	}*/

	private static void updateProgressFile(String querySize2, String type, int q, String numSites2, long time) throws IOException {
		FileWriter p = new FileWriter(System.getenv().get("HOME") + "/progress_dist.txt", true);
		p.write("Sites: "+numSites2+" Type: "+type+" QuerySize: "+querySize2+" QueryNum: "+q+" COMPLETED: "+time+"\n");
		p.close();
	}

	/*public static void main(String args[]) throws Exception
	{
		String sites_filename = dirname+"SystemSites.conf";
		String relations_filename = dirname+"Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);

		//20, 40, 60, 80, 100
		//Chain, Cycle, Star, Clique
		String algorithm = "DistML";
		String sizeOfQuery = "100";
		String type = "Star";
		String outputDir = dirname+algorithm+"/"+sizeOfQuery+"-Queries/"+type+"/";

		//************************************************************************

		int k = 10;
		assert k<=Integer.parseInt(sizeOfQuery);
		long start, end;
		QueryPlanTree tree;
		int q = 1;

		JoinGraph graph = new JoinGraph(dirname+sizeOfQuery+"-Queries/"+type+"/Query1");	
		start = System.currentTimeMillis();
		OptimizerClient client = new OptimizerClient(graph, 
				new Site(InetAddress.getLocalHost().getHostAddress()).internalize());
		tree = client.optimize(k);

		end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+" ms for k = "+k);
		System.out.println(tree.getCost());
		outputResult(tree, outputDir, q, (end-start));
	}*/
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

	/*static int[] querySize = {20, 40, 60, 80, 100}; //
	static String[] types = {"Chain", "Cycle", "Clique", "Star", "Mixed"};
	static int numberOfQueries = 6;
	//static int[] _k = {35, 25, 8, 12, 8};
	//static int[] _k = {9, 7, 5, 7, 5};
	static int[] _k = {5, 4, 4, 4, 4};

	public static void main(String args[])
	{
		try {
			@SuppressWarnings("unused")
			Registry localreg = LocateRegistry.createRegistry(1099);

			String sites_filename = dirname+"SystemSites.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
			String relations_filename = dirname+"Catalog.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
			Catalog.populate(sites_filename, relations_filename);

			for(int t : querySize){
				//String dir = dirname+t+"-Queries/";
				for(int i = 0; i<types.length; i++)
				{
					String type = types[i];
					for(int q=1; q<numberOfQueries; q++)//numberOfQueries; q++)
					{
						int k = _k[i];
						String queryDir = dirname+t+"-Queries/";
						String outputDir = dirname+"Distributed/"+t+"-Queries/"+type+"/";
						JoinGraph graph = new JoinGraph(queryDir+type+"/Query"+q);
						//graph.outputToFile(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/multilevel_graphs/Test.dot");
						OptimizerClient client = new OptimizerClient(graph, 
								new Site(InetAddress.getLocalHost().getHostAddress()).internalize());


						long start = System.currentTimeMillis();
						QueryPlanTree tree = client.optimize(k);
						long end = System.currentTimeMillis();
						System.out.println("Executed: "+queryDir+type+"/Query"+q);
						tree.outputToFile(outputDir+"tree"+q+":k"+k+".tex");

						TaskTree taskTree = new TaskTree(tree);

						//Create task table
						TaskTableGenerator gen = new TaskTableGenerator(taskTree);
						gen.generateCode(outputDir+"TaskTable"+q+".tex");

						PrintWriter p = new PrintWriter(outputDir+"Result"+q+".txt");
						p.println(tree.getCost().toString());
						p.println((end-start));
						p.close();

						Scheduler scheduler = tree.getSchedule();
						scheduler.outputToFile(outputDir+"schedule"+q+":k"+k+".txt");


					}
				}
			}

			//client._logger.print("Total Optimization Time: "+(end-start)+"ms");

			//PrintWriter out = new PrintWriter("schedule.txt");
			//out.print(tree.getCost()._scheduler.toString());
			//out.close();

			//tree.outputToFile(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/multilevel_trees/FinalTree.tex");
			//client._logger.print("Plan cost: "+tree.getCost());
			System.exit(0);
		}
		catch (Exception e) {
			System.out.println("OptimizerClient exception: "
					+ e.getMessage());
			e.printStackTrace();
		}
	}*/
}
