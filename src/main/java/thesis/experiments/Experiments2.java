package thesis.experiments;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.multilevel_optimization.SequentialAlgorithm;

import thesis.code_generators.QueryPlanTreeCodeLatexCodeGenerator;
import thesis.code_generators.TaskTableGenerator;
import thesis.code_generators.TaskTreeLatexCodeGenerator;
import thesis.code_generators.TreeCodeGenerator;

import thesis.query_plan_tree.Plan;
import thesis.scheduler.Scheduler;
import thesis.task_tree.TaskTree;

import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.IDP1_Balanced;
import thesis.enumeration_algorithms.IDP1ccp;

import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;

/**
 * Run sequential experiment.
 * @author c09rt
 *
 */
public class Experiments2 {

	static int[] _numberOfRelations = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
	static int[] _numberOfSites = {2, 4, 6, 8, 10};
	static int[] _sizeOfK = {2, 4, 6, 8, 10, 12};
	static int[] _relationsResideAtNumberOfSites;
	static int[] _serverSites = new int[5];//{5, 10, 15, 20};
	static int _numberOfQueries = 10;
	static CatalogGenerator catalogGenerator;
	static String _baseDir = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment2/";

	public static void main(String args[]) throws Exception
	{
		_relationsResideAtNumberOfSites = new int[2];
		_relationsResideAtNumberOfSites[0] = 1;
		_serverSites[0]=-1;
		_serverSites[1]=5;
		_serverSites[2]=10;
		_serverSites[3]=15;
		_serverSites[4]=20;
		
		makeFirstDirectory();
		makeDirectoryStructureSequential();
		makeDirectoryStructureDistributed();
		generateCatalogFilesSequential();
		generateCatalogFilesDistributed();
		generateQueryFilesSequential();
		generateQueryFilesDistributed();
		//executeSequentialAlgorithm();
		
	}

	private static void makeFirstDirectory() throws IOException {
		File f = new File(_baseDir+"/Sequential"); 
		f.createNewFile();
		f = new File(_baseDir+"/Distributed"); 
		f.createNewFile();
	}

	private static void executeSequentialAlgorithm() throws Exception {
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						String dirname = getDirName(_numberOfRelations[i],_numberOfSites[j],
								_sizeOfK[k], _relationsResideAtNumberOfSites[l], -1);
						String sites_filename = dirname+"/SystemSites.conf";
						String relations_filename = dirname+"/Catalog.conf";
						Catalog.populate(sites_filename, relations_filename);
						
						String queryFile = null;
						String[] graph_types = {"Chain", "Star", "Clique", "Cycle", "Mixed"}; 
						
						for(int q = 1; q<_numberOfQueries+1; q++)
						{
							for(int t = 0; t<graph_types.length; t++){
								queryFile = dirname+"/Queries/"+graph_types[t]+"/Query"+q
											+"/Query"+q;
							
								JoinGraph graph = new JoinGraph(queryFile);
								String queryDir = dirname+"/Queries/"+graph_types[t]+"/Query"+q+"/Sequential";
								String result = queryDir+"/Result.txt";
								//Create result file.
								File f = new File(result); 
								f.createNewFile();
								
								long start = System.currentTimeMillis();
								SequentialAlgorithm de = new SequentialAlgorithm(graph);
								Plan tree = de.optimize(_sizeOfK[k]);
								long end = System.currentTimeMillis();
								
								PrintWriter p = new PrintWriter(result);
								p.println((end-start)+" "+tree.getCost());
								p.close();
								System.out.println("completed optimization "+queryFile+"/Sequential: "+(end-start));
								Thread.sleep(5000);
								TaskTree taskTree = new TaskTree(tree);
								Scheduler scheduler = new Scheduler(taskTree);
								scheduler.schedule();
								TreeCodeGenerator gc = new QueryPlanTreeCodeLatexCodeGenerator(tree);
								gc.generateCode(queryDir+"/tree.tex");
								TaskTableGenerator gen = new TaskTableGenerator(taskTree);
								gen.generateCode(queryDir+"/TaskTable.tex");
								TaskTreeLatexCodeGenerator g = new TaskTreeLatexCodeGenerator(taskTree);
								g.generateCode(queryDir+"/tasktree.tex");

								PrintWriter out = new PrintWriter(queryDir+"/schedule.txt");
								out.print(scheduler.toString());
								out.close();
							}
						}
					}
				}
			}
		}
	}

	private static String getDirName(int numberOfRelations, int numberOfSites, 
			int sizeOfK, int numberOfSitesRelationResidesAt, int serversites){
		
		if(serversites==-1)
		{
			return _baseDir+"/Sequential/R:"+numberOfRelations+"_S:"+numberOfSites+
			"_K:"+sizeOfK+"_RS:"+numberOfSitesRelationResidesAt;
		}
		else{
			return _baseDir+"/Distributed/R:"+numberOfRelations+"_S:"+numberOfSites+
			"_K:"+sizeOfK+"_RS:"+numberOfSitesRelationResidesAt+"_SS:"+serversites;
		}
		
	}

	private static void makeDirectoryStructureSequential() throws Exception {
		File f = null; 
	
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites = new int[2];
				_relationsResideAtNumberOfSites[0] = 1;
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{	
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						String dirname = getDirName(_numberOfRelations[i],_numberOfSites[j],
								_sizeOfK[k], _relationsResideAtNumberOfSites[l], -1);
						f= new File(dirname);

						try{
							if(f.mkdir())
								System.out.println("created file directory: "+dirname);

							dirname = dirname+"/"+"Queries";

							f= new File(dirname);
							if(f.mkdir())
								System.out.println("created file directory: "+dirname);

							String subdir = dirname+"/Star";

							f= new File(subdir);
							if(f.mkdir())
								System.out.println("created file directory: "+subdir);

							createQueryDirs(subdir);

							subdir = dirname+"/Clique";

							f= new File(subdir);
							if(f.mkdir())
								System.out.println("created file directory: "+subdir);

							createQueryDirs(subdir);
							subdir = dirname+"/Chain";

							f= new File(subdir);
							if(f.mkdir())
								System.out.println("created file directory: "+subdir);
							createQueryDirs(subdir);
							subdir = dirname+"/Cycle";

							f= new File(subdir);
							if(f.mkdir())
								System.out.println("created file directory: "+subdir);
							createQueryDirs(subdir);
							
							subdir = dirname+"/Mixed";

							f= new File(subdir);
							if(f.mkdir())
								System.out.println("created file directory: "+subdir);
							createQueryDirs(subdir);

						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	private static void makeDirectoryStructureDistributed() throws Exception {
		File f = null; 
	
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites = new int[2];
				_relationsResideAtNumberOfSites[0] = 1;
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{	
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						_serverSites[0]=_numberOfSites[j];
						
						for(int m = 0; m<_serverSites.length; m++)
						{
							String dirname = getDirName(_numberOfRelations[i],_numberOfSites[j],
									_sizeOfK[k], _relationsResideAtNumberOfSites[l], _serverSites[m]);
							f= new File(dirname);

							try{
								if(f.mkdir())
									System.out.println("created file directory: "+dirname);

								dirname = dirname+"/"+"Queries";

								f= new File(dirname);
								if(f.mkdir())
									System.out.println("created file directory: "+dirname);

								String subdir = dirname+"/Star";

								f= new File(subdir);
								if(f.mkdir())
									System.out.println("created file directory: "+subdir);

								createQueryDirs(subdir);

								subdir = dirname+"/Clique";

								f= new File(subdir);
								if(f.mkdir())
									System.out.println("created file directory: "+subdir);

								createQueryDirs(subdir);
								subdir = dirname+"/Chain";

								f= new File(subdir);
								if(f.mkdir())
									System.out.println("created file directory: "+subdir);
								createQueryDirs(subdir);
								subdir = dirname+"/Cycle";

								f= new File(subdir);
								if(f.mkdir())
									System.out.println("created file directory: "+subdir);
								createQueryDirs(subdir);
								
								subdir = dirname+"/Mixed";

								f= new File(subdir);
								if(f.mkdir())
									System.out.println("created file directory: "+subdir);
								createQueryDirs(subdir);

							}
							catch(Exception e)
							{
								e.printStackTrace();
							}
						}
						
					}
				}
			}
		}
	}
	private static void createQueryDirs(String subdir) throws IOException {
		File f = null;

		for(int i = 1; i<_numberOfQueries+1; i++)
		{
			f= new File(subdir+"/Query"+i);
			if(f.mkdir())
				System.out.println("created file directory: "+subdir+"/Query"+i);
		}
	}

	private static void generateCatalogFilesDistributed() throws CatalogException, FileNotFoundException{
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						_serverSites[0]=_numberOfSites[j];
						
						for(int m = 0; m<_serverSites.length; m++)
						{
							catalogGenerator = new CatalogGenerator(_numberOfRelations[i], 
									_numberOfSites[j], _relationsResideAtNumberOfSites[l],
									getDirName(_numberOfRelations[i],_numberOfSites[j], _sizeOfK[k]
									                   ,_relationsResideAtNumberOfSites[l], _serverSites[m]));
							catalogGenerator.generate();
						}
					}
				}
			}
		}
	}
	
	private static void generateCatalogFilesSequential() throws CatalogException, FileNotFoundException{
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						catalogGenerator = new CatalogGenerator(_numberOfRelations[i], 
								_numberOfSites[j], _relationsResideAtNumberOfSites[l],
								getDirName(_numberOfRelations[i],_numberOfSites[j], _sizeOfK[k]
								                   ,_relationsResideAtNumberOfSites[l], -1));
						catalogGenerator.generate();

					}
				}
			}
		}
	}

	private static void generateQueryFilesSequential() throws Exception{
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						String dirname = getDirName(_numberOfRelations[i],_numberOfSites[j], 
								_sizeOfK[k], _relationsResideAtNumberOfSites[l], -1);
						String sites_filename = dirname+"/SystemSites.conf";
						String relations_filename = dirname+"/Catalog.conf";
						Catalog.populate(sites_filename, relations_filename);
						
						//Generate cliques
						for(int s = 1; s<_numberOfQueries+1; s++){
							QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
							dirname+"/Queries/Clique/Query"+s+"/Query"+s);
							gen.generateClique();
						}
						
						//Generate stars
						for(int s = 1; s<_numberOfQueries+1; s++){
							QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
							dirname+"/Queries/Star/Query"+s+"/Query"+s);
							gen.generateStar();
						}
						
						//Generate chains
						for(int s = 1; s<_numberOfQueries+1; s++){
							QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
							dirname+"/Queries/Chain/Query"+s+"/Query"+s);
							gen.generateChain();
						}
						
						//Generate cycles
						for(int s = 1; s<_numberOfQueries+1; s++){
							QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
							dirname+"/Queries/Cycle/Query"+s+"/Query"+s);
							gen.generateCycle();
						}
						
						//Generate mixed
						for(int s = 1; s<_numberOfQueries+1; s++){
							QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
							dirname+"/Queries/Mixed/Query"+s+"/Query"+s);
							gen.generateMixed();
						}
					}
				}
			}
		}
	}

	private static void generateQueryFilesDistributed() throws Exception{
		for(int i = 0; i<_numberOfRelations.length; i++)
		{
			for(int j = 0; j<_numberOfSites.length; j++)
			{
				_relationsResideAtNumberOfSites[1] = _numberOfSites[j];

				for(int l = 0; l<_relationsResideAtNumberOfSites.length; l++)
				{
					for(int k = 0; k<_sizeOfK.length; k++)
					{
						_serverSites[0]=_numberOfSites[j];
						
						for(int m = 0; m<_serverSites.length; m++)
						{
							String dirname = getDirName(_numberOfRelations[i],_numberOfSites[j], 
									_sizeOfK[k], _relationsResideAtNumberOfSites[l], _serverSites[m]);
							String sites_filename = dirname+"/SystemSites.conf";
							String relations_filename = dirname+"/Catalog.conf";
							Catalog.populate(sites_filename, relations_filename);
							
							//Generate cliques
							for(int s = 1; s<_numberOfQueries+1; s++){
								QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
								dirname+"/Queries/Clique/Query"+s+"/Query"+s);
								gen.generateClique();
							}
							
							//Generate stars
							for(int s = 1; s<_numberOfQueries+1; s++){
								QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
								dirname+"/Queries/Star/Query"+s+"/Query"+s);
								gen.generateStar();
							}
							
							//Generate chains
							for(int s = 1; s<_numberOfQueries+1; s++){
								QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
								dirname+"/Queries/Chain/Query"+s+"/Query"+s);
								gen.generateChain();
							}
							
							//Generate cycles
							for(int s = 1; s<_numberOfQueries+1; s++){
								QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
								dirname+"/Queries/Cycle/Query"+s+"/Query"+s);
								gen.generateCycle();
							}
							
							//Generate mixed
							for(int s = 1; s<_numberOfQueries+1; s++){
								QueryGenerator gen = new QueryGenerator(_numberOfRelations[i], 
								dirname+"/Queries/Mixed/Query"+s+"/Query"+s);
								gen.generateMixed();
							}
						}
						
					}
				}
			}
		}
	}
	private static void performExperiment(int numberOfRelations, int numberOfSites, 
			int sizeOfK, int numberOfSitesRelationResidesAt) {



		/*String sites_filename = System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
		String relations_filename = System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		JoinGraph graph = new JoinGraph(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Queries/Query1");
		SequentialAlgorithm alg = new SequentialAlgorithm(graph);
		long start = System.currentTimeMillis();
		QueryPlanTree tree = alg.optimize(2);
		long end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+"ms");
		tree.outputToFile("multilevel_trees/FinalTree.tex");
		System.out.println("Plan cost: "+tree.getCost());

		PrintWriter out = new PrintWriter("schedule.txt");
		out.print(tree.getSchedule().toString());
		out.close();*/
	}
}
