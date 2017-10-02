package thesis.experiments;

import thesis.code_generators.TaskTableGenerator;
import thesis.multilevel_optimization.SequentialAlgorithm;
import thesis.query_plan_tree.Plan;
import thesis.task_tree.TaskTree;
import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.catalog.Catalog;
import thesis.enumeration_algorithms.DPccp;
import thesis.enumeration_algorithms.IDP1_Balanced;
import thesis.enumeration_algorithms.IDP1ccp;

public class ExperimentIDP1ccp {
	public static void main(String args[]) 
	throws Exception
	{
		//CatalogGenerator catalogGenerator = new CatalogGenerator(100, 
		//		3, 3, System.getenv().get("HOME") + "/temp/");
		//catalogGenerator.generate();
		String sites_filename = System.getenv().get("HOME") + "/temp/SystemSites.conf";
		String relations_filename = System.getenv().get("HOME") + "/temp/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		
		JoinGraph graph;
		long start;
		long end ;
		Plan tree;
		
		/*graph = new JoinGraph(System.getenv().get("HOME") + "/temp/Query5");	
		SequentialAlgorithm seq = new SequentialAlgorithm(graph);
		start = System.currentTimeMillis();
		int k = 60;
		tree = seq.optimize(k);
		end = System.currentTimeMillis();
		System.out.println("IDP1: Time taken: "+(end-start)+" ms for k = "+k);
		System.out.println(tree.getCost());
		*/
		//QueryGenerator gen = new QueryGenerator(10, System.getenv().get("HOME") + "/temp/Query1");
		//gen.generateStar();
		
		
		System.out.println("start");
		for(int i = 35; i<=35; i++){
			graph = new JoinGraph(System.getenv().get("HOME") + "/temp/Query5");	
			start = System.currentTimeMillis();
			
			//IDP1_Balanced de = new IDP1_Balanced(graph, i);
			IDP1ccp de = new IDP1ccp(graph, i);
			tree = de.enumerate();
			
			end = System.currentTimeMillis();
			System.out.println("IDP1: Time taken: "+(end-start)+" ms for k = "+i);
			System.out.println(tree.getCost());
			
			
			/*graph = new JoinGraph(System.getenv().get("HOME") + "/temp/Query5");
			
			start = System.currentTimeMillis();
			
			DPccp df = new DPccp(graph);
			tree=df.enumerate(false).getFirst();
			
			end = System.currentTimeMillis();
			System.out.println("DPccp: Time taken: "+(end-start)+" ms for k = "+i);
			System.out.println(tree.getCost());*/
			//SequentialAlgorithm seq = new SequentialAlgorithm(graph);
			//tree = seq.optimize(12);
			
		}
		
		
	}
}
