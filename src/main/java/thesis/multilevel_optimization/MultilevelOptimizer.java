package thesis.multilevel_optimization;

import thesis.iterators.QueryPlanTreeLeafIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.joingraph.JoinInfo;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.RelationNode;
import thesis.utilities.Pair;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;

public abstract class MultilevelOptimizer implements Serializable{

	private static final long serialVersionUID = -3746678837652243089L;
	protected Map<String, JoinGraph> _stubGraphMapping;
	protected JoinGraph _joinGraph;
	
	protected int _level = 1;
	
	public MultilevelOptimizer(JoinGraph joinGraph){
		_stubGraphMapping = new HashMap<String, JoinGraph>();
		_joinGraph = joinGraph;
		//_joinGraph.outputToFile(System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/multilevel_graphs/Level_"+_level+".dot");
		_level++;
	}
	
	public abstract Plan optimize(int k) throws Exception;
	
	/**
	 * Build the final plan by recursively replacing stub nodes from the 
	 * top level plan with their corresponding optimized query plans.
	 * @param stubIdentifier
	 * @return
	 * @throws QueryPlanTreeException 
	 */
	protected abstract Plan buildFinalPlan() throws Exception;
	
	protected List<Pair<String, PlanNode>> getStubEntries(Plan tree)
	{
		List<Pair<String, PlanNode>> stubEntries = 
			new LinkedList<Pair<String, PlanNode>>();
		
		QueryPlanTreeLeafIterator iter = new QueryPlanTreeLeafIterator(tree);
		while(iter.hasNext())
		{
			RelationNode node = iter.next();
	
			if(node.getName().length()>5 && node.getName().substring(0, 6).equals("_Stub_"))
			{
				stubEntries.add(new Pair<String, PlanNode>(node.getName(), node));
			}
		}
		
		return stubEntries;
	}
	
	/**
	 * Using the size of the intermediate result of S and the name of the relation
	 * determine the size of the intermediate result if n was joined with s.
	 * @param s 
	 * @param intermediateResultOfS
	 * @param n
	 * @return
	 * @throws JoinGraphException 
	 * @throws CatalogException 
	 */
	protected double calculateIntermediateResult(TreeSet<String> s, double intermediateResultOfS, 
			String n) throws JoinGraphException, CatalogException {
		//Get product of all edges (selectivity) to n from  s
		JoinInfo joinInfo = new JoinInfo(1.0);

		Set<String> neighbours = _joinGraph.getNeighbours(n);
		neighbours.retainAll(s);
		
		for(String neighbour : neighbours)
		{
			joinInfo.conjunct(_joinGraph.getJoinInfo(n, neighbour));
		}
		return intermediateResultOfS*Catalog.getCardinality(n)*joinInfo.getSelectivity();
	}
	/**
	 * Produce the next stub identifier.  Identifiers take
	 * the form of _Stub_i where i is an integer.
	 * @return
	 */
	protected abstract String nextStubIdentifier(); 
}
