package thesis.enumeration_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import thesis.catalog.Catalog;
import thesis.joingraph.JoinGraph;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanCost;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.Site;
import thesis.utilities.Pair;

public abstract class DynamicProgramming {

	protected Map<TreeSet<String>, List<Plan>> _optPlans;
	protected TreeSet<String> _toDo;
	protected JoinGraph _joinGraph;

	public DynamicProgramming()
	{
		_optPlans = new HashMap<TreeSet<String>, List<Plan>>();
		_toDo = new TreeSet<String>();
	}

	public DynamicProgramming(JoinGraph graph)
	{
		_joinGraph=graph;
		TreeSet<String> relations = new TreeSet<String>();
		for(Iterator<String> iter = graph.vertexIterator(); iter.hasNext();)
			relations.add(iter.next());

		_optPlans = new HashMap<TreeSet<String>, List<Plan>>();
		_toDo = relations;
	}

	public Plan enumerate() throws Exception
	{
		performFirstPhase();
		performSecondPhase();
		return performThirdPhase();
	}

	protected void performFirstPhase() throws Exception
	{
		for(String relation_name : _toDo)
		{
			for(Iterator<Site> iter = Catalog.getAvailableSitesIterator(relation_name); iter.hasNext();)
			{
				Site site = iter.next();
				RelationNode r = new RelationNode(site, relation_name);
				Plan tree = new Plan(r);
				TreeSet<String> relations = new TreeSet<String>();
				relations.add(relation_name);

				if(_optPlans.containsKey(relations))
				{
					_optPlans.get(relations).add(tree);
					prunePlans(_optPlans.get(relations));
				}
				else
				{
					List<Plan> trees = new ArrayList<Plan>();
					trees.add(tree);
					_optPlans.put(relations, trees);
				}
			}
		}
	}

	protected abstract void performSecondPhase() throws Exception;

	protected Plan performThirdPhase() throws Exception
	{
		List<Plan> trees = _optPlans.get(_toDo);

		Plan minTree = null;
		PlanCost minTreeCost = PlanCost.MAX_VALUE;
		Site querySite = Catalog.getQuerySite();

		for(Plan tree : trees)
		{

			if(!tree.getRoot().getSite().equals(querySite))
			{
				tree.shipToSite(querySite);
			}

			PlanCost cost = tree.getCost();

			if(cost.compareTo(minTreeCost)<=0)
			{
				minTree = tree;
				minTreeCost=cost;
			}   
		}

		if(minTree==null)
			throw new Exception("DP: Could not find tree with minimum cost in prune plans.");
		
		return minTree;
	}

	public void prunePlans(List<Plan> trees) throws Exception
	{	
		//We need to make sure that we do not prune plans from different sites.

		//List<QueryPlanTree> trees = _optPlans.get(list);

		//Map from site to minimum query plan tree and cost found in list.
		Map<Site, Pair<Plan, PlanCost>> minimumCostTrees
		= new HashMap<Site, Pair<Plan, PlanCost>>();

		for(Plan tree : trees)
		{
			PlanCost cost = tree.getCost();
			Site site = tree.getRoot().getSite();

			if(minimumCostTrees.containsKey(site))
			{
				if(cost.compareTo(minimumCostTrees.get(site).getSecond())<=0){
					minimumCostTrees.remove(site);
					minimumCostTrees.put(site, 
							new Pair<Plan, PlanCost>(tree, cost));
				}
			}
			else{
				minimumCostTrees.put(site, 
						new Pair<Plan, PlanCost>(tree, cost));
			}
		}

		//Clear all entries in _bestPlans for relations.
		trees.clear();

		//Add all minimum trees found into _bestPlans entry for relations.
		for(Pair<Plan, PlanCost> entry : minimumCostTrees.values())
			trees.add(entry.getFirst());
	}

	protected List<Plan> joinPlans(List<Plan> o, List<Plan> i) throws Exception
	{
		List<Plan> ret = new LinkedList<Plan>();

		for(Iterator<Site> site_iter = Catalog.siteIterator(); site_iter.hasNext();)
		{
			Site site = site_iter.next();
			for(Iterator<Plan> treesOuterIter = o.iterator(); treesOuterIter.hasNext();)
			{
				Plan outer = treesOuterIter.next();
				for(Iterator<Plan> treesInnerIter = i.iterator(); treesInnerIter.hasNext();)
				{
					Plan inner = treesInnerIter.next();

					Plan tree = Plan.join(site, outer, inner, _joinGraph);
					ret.add(tree);
					prunePlans(ret);
				}
			}
		}

		return ret;
	}

	public void printOptPlans()
	{
		System.out.println("---------");
		Iterator<Entry<TreeSet<String>, List<Plan>>> iter 
		= _optPlans.entrySet().iterator();
		Entry<TreeSet<String>, List<Plan>> next;

		while(iter.hasNext())
		{
			next = iter.next();
			System.out.print(next.getKey()+" : ");
			System.out.println(next.getValue());
		}

		System.out.println("---------");
	}
}
