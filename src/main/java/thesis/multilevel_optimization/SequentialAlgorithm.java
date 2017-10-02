package thesis.multilevel_optimization;

import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import thesis.code_generators.QueryPlanTreeCodeLatexCodeGenerator;
import thesis.code_generators.TaskTableGenerator;
import thesis.code_generators.TaskTreeLatexCodeGenerator;
import thesis.code_generators.TreeCodeGenerator;

import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.DPccp;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.Site;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;
import thesis.utilities.Pair;

/**
 * The sequential version of the multilevel algorithm.  Relations in the join
 * graph are grouped together into temporary relations called stubs at each level.
 * At each level the stub is optimized and the stub cardinality and sites are 
 * assigned according to the root of the optimized tree.  Final plan is built
 * in bottom up fashion.
 *  
 * @author rob
 *
 */
public class SequentialAlgorithm extends MultilevelOptimizer{
	/** Stores the optimized trees for a particular stub*/
	protected Map<String, Plan> _optimizedSubplans; 
	protected String _nextIdentifierStub = "_Stub_1";
	public SequentialAlgorithm(JoinGraph g)
	{
		super(g);
		_optimizedSubplans = new HashMap<String, Plan>();
	}

	protected String nextStubIdentifier(){
		int n = Integer.parseInt(_nextIdentifierStub.substring(6, _nextIdentifierStub.length()));
		_nextIdentifierStub = "_Stub_"+(++n);
		return _nextIdentifierStub;
	}
	public Plan optimize(int k) throws JoinGraphException, 
	QueryPlanTreeException, CatalogException, RemoteException
	{
		//if(k<=2) throw new JoinGraphException("Optimizer must have k value >2");
		String stubIdentifier = "";
		JoinGraph subgraph = null;


		while(_joinGraph.getNumberOfVertices()>k)
		{
			Pair<String, String> minSelEdge = //_joinGraph.getMinimumIntermediateSizeEdge();
				_joinGraph.getMiniumSelectivityEdge();
			TreeSet<String> S = new TreeSet<String>();
			S.add(minSelEdge.getFirst());
			S.add(minSelEdge.getSecond());
			
			double intermediateResultOfS = Catalog.getCardinality(minSelEdge.getFirst())*
			Catalog.getCardinality(minSelEdge.getSecond())*
			_joinGraph.getJoinInfo(minSelEdge.getFirst(), 
					minSelEdge.getSecond()).getSelectivity();
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
			subgraph = _joinGraph.absorb(S, stubIdentifier);
			//subgraph.outputToFile("multilevel_graphs/"+k+"_"+stubIdentifier+".dot");
			//_joinGraph.outputToFile("multilevel_graphs/"+k+"_Level_"+_level+".dot");
			_level++;
			_stubGraphMapping.put(stubIdentifier, subgraph);

			Plan tree = optimizeTask(subgraph, true);
			_optimizedSubplans.put(stubIdentifier, tree);

			int stub_tuple_size = 0;
			Set<Site> sites = new HashSet<Site>();
			sites.add(tree.getRoot().getSite());

			for(String s : S){
				stub_tuple_size+=Catalog.getTupleSize(s);
			}

			//Protected against very small selectivities killing relation sizes.
			if(intermediateResultOfS==0)
				intermediateResultOfS=1;
			
			Catalog.addRelation(stubIdentifier, 
					Math.ceil(intermediateResultOfS), 
					stub_tuple_size, sites);
			assert (tree.getRoot().getOutputCardinality()==intermediateResultOfS);
			//tree.outputToFile("multilevel_trees/"+k+"_"+stubIdentifier+".tex");
		}

		stubIdentifier = nextStubIdentifier();
		_stubGraphMapping.put(stubIdentifier, _joinGraph);
		Plan tree = optimizeTask(_joinGraph, false);
		_optimizedSubplans.put(stubIdentifier, tree);

		//tree.outputToFile("multilevel_trees/"+stubIdentifier+".tex");
		return buildFinalPlan();
	}

	/**
	 * Build the final plan by recursively replacing stub nodes from the 
	 * top level plan with their corresponding optimized query plans.
	 * @param stubIdentifier
	 * @return
	 * @throws QueryPlanTreeException 
	 */
	protected Plan buildFinalPlan() throws QueryPlanTreeException {
		Plan tree = _optimizedSubplans.get(_nextIdentifierStub);
		List<Pair<String, PlanNode>> stubs = getStubEntries(tree);

		while(!stubs.isEmpty())
		{
			for(Pair<String, PlanNode> p : stubs)
			{
				p.getSecond().getParent().replaceChild(p.getSecond(), 
						_optimizedSubplans.get(p.getFirst()).getRoot());
			}
			stubs = getStubEntries(tree);
		}

		return tree;
	}


	/**
	 * Optimize using DPccp. 
	 * @param g
	 * @return
	 * @throws JoinGraphException 
	 */
	private Plan optimizeTask(JoinGraph g, boolean isGreedy) throws JoinGraphException {
		DPccp dpccp = new DPccp(g);
		try {
			Plan tree =  dpccp.enumerate(isGreedy).getFirst();
			return tree;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String args[]) throws NumberFormatException, 
	JoinGraphException, IOException, QueryPlanTreeException, CatalogException, SchedulerException, ParseException
	{
		String sites_filename = System.getProperty("user.home")+"/Desktop/parallelproblem/SystemSites.conf";
		String relations_filename = System.getProperty("user.home")+"/Desktop/parallelproblem/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		JoinGraph graph = new JoinGraph(System.getProperty("user.home")+"/Desktop/parallelproblem/Query4");
		SequentialAlgorithm alg = new SequentialAlgorithm(graph);
		long start = System.currentTimeMillis();
		Plan tree = alg.optimize(5);
		long end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+"ms");

		//Create query plan tree
		TreeCodeGenerator gc = new QueryPlanTreeCodeLatexCodeGenerator(tree);
		gc.generateCode("tree.tex");

		TaskTree taskTree = new TaskTree(tree);

		//Create task table
		TaskTableGenerator gen = new TaskTableGenerator(taskTree);
		gen.generateCode("TaskTable.tex");

		//Create task tree
		TaskTreeLatexCodeGenerator g = new TaskTreeLatexCodeGenerator(taskTree);
		g.generateCode("tasktree.tex");

		//Create schedule
		PrintWriter out = new PrintWriter("schedule.txt");
		out.print(tree.getSchedule().toString());
		out.close();
		System.out.println(tree.getCost());
	}
}
