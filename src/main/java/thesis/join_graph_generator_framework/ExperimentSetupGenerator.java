package thesis.join_graph_generator_framework;

import thesis.catalog.Catalog;
import thesis.joingraph.JoinGraph;

public class ExperimentSetupGenerator {

	public static void main(String args[]) throws Exception
	{
		/*CatalogGenerator catalogGenerator = new CatalogGenerator(100, 3, 2);
		catalogGenerator.generate();
		String sites_filename = System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
		String relations_filename = System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		//System.out.println(Catalog.catalogToString());
		for(int i = 1; i<11; i++){
			Catalog.clear();
			Catalog.populate(sites_filename, relations_filename);
			QueryGenerator queryGenerator = new QueryGenerator(10, "Query"+i);
			queryGenerator.generate();
			JoinGraph g = new JoinGraph(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Queries/Query"+i);
			g.outputToFile(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/QueryGraphs/Query"+i+".dot");
			System.out.println("Query"+i+": "+g);
		}*/
	}
}
