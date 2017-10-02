package thesis.enumeration_algorithms;

import thesis.iterators.CombinationIterator;
import thesis.iterators.JoinGraphBreadthFirstIterator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.code_generators.QueryPlanTreeCodeLatexCodeGenerator;
import thesis.code_generators.TaskTableGenerator;
import thesis.code_generators.TaskTreeLatexCodeGenerator;
import thesis.code_generators.TreeCodeGenerator;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanCost;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.Site;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;
import thesis.utilities.Pair;

/**
 * Graph must be numbered in breadth first fashion! I have provided a mapping for this.
 * @author rob
 *
 */
public class DPccp {

	protected JoinGraph _joinGraph;
	protected String[] _relationIDs;
	protected Map<String, Integer> _relationIDMapping;
	protected Map<TreeSet<Integer>, List<Plan>> _bestPlans;
	protected TreeSet<Integer> _relations;
	protected List<Plan> _finalPlans;
	//public double _numberOfPlansConsidered;

	public DPccp(JoinGraph g) throws JoinGraphException
	{
		_joinGraph = g;
		_relationIDMapping = new HashMap<String, Integer>();
		_bestPlans = new HashMap<TreeSet<Integer>, List<Plan>>();
		_relations = new TreeSet<Integer>();
		_relationIDs = new String[g.getNumberOfVertices()+1];
		_finalPlans = new LinkedList<Plan>();
		//_numberOfPlansConsidered=0;
	}

	protected void performFirstPhase() throws JoinGraphException, CatalogException
	{
		JoinGraphBreadthFirstIterator iter = new JoinGraphBreadthFirstIterator(_joinGraph);
		int count = 1;
		String relationID = "";

		while(iter.hasNext())
		{
			relationID = iter.next();
			_relationIDs[count]=relationID;
			_relationIDMapping.put(relationID, count);

			for(Iterator<Site> site_iter = Catalog.getAvailableSitesIterator(relationID);
			site_iter.hasNext();)
			{
				Site site = site_iter.next();
				RelationNode r = new RelationNode(site, relationID);
				Plan tree = new Plan(r);

				TreeSet<Integer> relation = new TreeSet<Integer>();
				relation.add(count);

				if (_bestPlans.containsKey(relation))
					_bestPlans.get(relation).add(tree);
				else{
					List<Plan> trees = new LinkedList<Plan>();
					trees.add(tree);
					_bestPlans.put(relation, trees);
				}
			}

			_relations.add(count);
			count++;
		}
	}

	@SuppressWarnings("unchecked")
	protected void performSecondPhase() throws JoinGraphException, SchedulerException, IOException
	{
		//System.out.println(_relationIDMapping);
		if(_relations.size()==1)
			return;

		List<TreeSet<Integer>> csgs = new LinkedList<TreeSet<Integer>>();
		List<TreeSet<Integer>> cmps = new LinkedList<TreeSet<Integer>>();

		TreeSet<Integer> s = new TreeSet<Integer>();

		enumerateCsg(csgs);

		for(TreeSet<Integer> s1 : csgs)
		{
			enumerateCmp(s1, cmps);

			for(TreeSet<Integer> s2 : cmps)
			{

				s.clear();
				s.addAll(s1);
				s.addAll(s2);

				List<Plan> p1 = _bestPlans.get(s1);
				List<Plan> p2 = _bestPlans.get(s2);
				List<Plan> currentPlans = joinPlans(p1, p2);
			//	_numberOfPlansConsidered+=currentPlans.size();
				
				if(!_bestPlans.containsKey(s))
					_bestPlans.put((TreeSet<Integer>)s.clone(), new LinkedList<Plan>());

				for(Plan tree : currentPlans)
				{
					_bestPlans.get(s).add(tree);
					prunePlans(_bestPlans.get(s));
				}

			}
			cmps.clear();
		}
	}

	protected void prunePlans(List<Plan> trees) throws SchedulerException{
		//We need to make sure that we do not prune plans from different sites.

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

	protected Pair<Plan, PlanCost> performThirdPhase(boolean greedyFinalSite) 
	throws QueryPlanTreeException, SchedulerException {
		List<Plan> trees = _bestPlans.get(_relations);

		for(Plan tree : trees){
			_finalPlans.add((Plan)tree.clone());
			//System.out.println("Tree cost: "+tree.getCost());
		}

		List<Plan> minTrees = new ArrayList<Plan>();
		PlanCost minTreeCost = null;

		Site querySite = Catalog.getQuerySite();

		for(Plan tree : trees)
		{

			if(!greedyFinalSite && !tree.getRoot().getSite().equals(querySite))
			{
				tree.shipToSite(querySite);
			}

			PlanCost cost = tree.getCost();
			//  System.out.println("min cost: "+minTreeCost+" This cost: "+cost);
			if(minTreeCost==null )
			{
				minTrees.add(tree);
				minTreeCost=cost;
			}   
			else if(cost.compareTo(minTreeCost)<0)
			{
				minTrees.clear();
				minTrees.add(tree);
				minTreeCost=cost;
			}
			else if(cost.compareTo(minTreeCost)==0){
				minTrees.add(tree);
			}
		}
		// System.out.println("number of pos: "+minTrees.size());
		Plan t = minTrees.get(0);
		return new Pair<Plan, PlanCost>(t, minTreeCost);
	}

	protected List<Plan> joinPlans(List<Plan> o, List<Plan> i) 
	throws JoinGraphException, SchedulerException
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
					Plan treeReversed = Plan.join(site, inner, outer, _joinGraph);
					ret.add(treeReversed);
					prunePlans(ret);
				}
			}
		}

		return ret;
	}

	/**
	 * Input: A connected query graph G = (V,E).
	 * Precondition: nodes in V are numbers according to a breadth-first search.
	 * Output: emits all subsets of V inducing a connected subgraph of G
	 * @param graph
	 * @throws JoinGraphException
	 */
	protected void enumerateCsg(List<TreeSet<Integer>> csgs) throws JoinGraphException{
		TreeSet<Integer> vertices = new TreeSet<Integer>();
		//TreeSet<String> vertices = new TreeSet<String>();

		for(int i = _joinGraph.getNumberOfVertices(); i>=1; i--)
		{
			vertices.add(i);
			emit(vertices, csgs);
			enumerateCsgRec(vertices, B(i), csgs);
			vertices.clear();
		}
	}

	protected TreeSet<Integer> getNeighbours(TreeSet<Integer> k) throws JoinGraphException
	{
		TreeSet<String> l = new TreeSet<String>(convertToString(k));
		TreeSet<Integer> t = new TreeSet<Integer>();

		for(String p : _joinGraph.getNeighbours(l))
		{
			t.add(_relationIDMapping.get(p));
		}
		return t;
	}

	protected void enumerateCsgRec(TreeSet<Integer> s, TreeSet<Integer> treeSet,
			List<TreeSet<Integer>> csgs) throws JoinGraphException
			{
		TreeSet<Integer> n = getNeighbours(s);
		n.removeAll(treeSet);

		TreeSet<Integer> subset = new TreeSet<Integer>();

		for(int i = 1; i<=n.size(); i++){
			CombinationIterator<Integer> iter = new CombinationIterator<Integer>(n, i);
			while(iter.next_combination(subset))
			{
				subset.addAll(s);
				emit(subset, csgs);
			}
		}

		TreeSet<Integer> xUnionN = new TreeSet<Integer>();
		xUnionN.addAll(treeSet);
		xUnionN.addAll(n);

		for(int i = 1; i<=n.size(); i++){
			CombinationIterator<Integer> iter = new CombinationIterator<Integer>(n, i);
			while(iter.next_combination(subset))
			{
				subset.addAll(s);
				enumerateCsgRec(subset, xUnionN, csgs);
			}
		}
			}

	/**
	 * Input:  a connected query graph G = (V,E), a connected subset S1
	 * Precondition: nodes in V are numbered according to a breadth-first search.
	 * Output: emits all complements S2 for S1 such that (S2, S1) is a csg-cmp-pair.
	 * @throws JoinGraphException
	 */
	@SuppressWarnings("unchecked")
	protected void enumerateCmp(TreeSet<Integer> s1, List<TreeSet<Integer>> cmps) throws JoinGraphException
	{
		TreeSet<Integer> x = new TreeSet<Integer>();
		x.addAll(B(s1.first()));
		x.addAll(s1);

		//List<Pair<String, Double>> neighbours = _joinGraph.getNeighbours(s1);
		TreeSet<Integer> n = getNeighbours(s1);
		n.removeAll(x);

		TreeSet<Integer> prohibitedArea = (TreeSet<Integer>)n.clone();
		TreeSet<Integer> vertex = new TreeSet<Integer>();

		Iterator<Integer> iter = n.descendingIterator();
		TreeSet<Integer> intersection = new TreeSet<Integer>();
		while(iter.hasNext())
		{
			int i = iter.next();
			vertex.add(i);
			emit(vertex, cmps);
			intersection = B(i);
			intersection.retainAll(n);
			prohibitedArea.addAll(x);
			prohibitedArea.addAll(intersection);
			enumerateCsgRec(vertex, prohibitedArea, cmps);
			vertex.clear();
			intersection.clear();
			prohibitedArea.clear();
		}
	}

	protected TreeSet<String> convertToString(TreeSet<Integer> s)
	{
		TreeSet<String> k = new TreeSet<String>();

		for(Integer i : s)
			k.add(_relationIDs[i]);
		return k;
	}

	protected TreeSet<Integer> B(int i)
	{
		TreeSet<Integer> set = new TreeSet<Integer>();
		for(int j = 1; j<=i; j++)
		{
			set.add(j);
		}
		return set;
	}
	@SuppressWarnings("unchecked")
	protected void emit(TreeSet<Integer> vertices, List<TreeSet<Integer>> csgs)
	{
		//System.out.println(csgs.size());
		csgs.add((TreeSet<Integer>)vertices.clone());
		vertices=null;
	}

	public Pair<Plan, PlanCost> enumerate(boolean greedyFinalSite) throws JoinGraphException, CatalogException,
	SchedulerException, DPccpException, QueryPlanTreeException, IOException
	{
		performFirstPhase();
		performSecondPhase();
		return performThirdPhase(greedyFinalSite);
	}

	/**
	 * Get the list of 
	 * @return
	 */
	public List<Plan> getFinalTreePossibilities(){
		return _finalPlans;
	}

	public static void main(String args[]) throws Exception
	{
		//CatalogGenerator g = new CatalogGenerator(100, 10, 10, System.getProperty("user.home")+"/Desktop/");
		//g.generate();
		
		//Add relation and sites into catalog.
		String sites_filename = System.getProperty("user.home")+"/Desktop/SystemSites.conf";
		String relations_filename = System.getProperty("user.home")+"/Desktop/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		QueryGenerator gen = new QueryGenerator(10, System.getProperty("user.home")+"/Desktop/Query1");
		gen.generateClique();
		JoinGraph graph = new JoinGraph(System.getProperty("user.home")+"/Desktop/Query1");

		DPccp ccp = new DPccp(graph);

		//Perform optimization
		long start = System.currentTimeMillis();
		Pair<Plan, PlanCost> optimal = ccp.enumerate(false);
		long end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+" ms");

		//Create query plan tree
		/*TreeCodeGenerator gc = new QueryPlanTreeCodeLatexCodeGenerator(optimal.getFirst());
		gc.generateCode("tree.tex");

		TaskTree taskTree = new TaskTree(optimal.getFirst());

		//Create task table
		TaskTableGenerator gen = new TaskTableGenerator(taskTree);
		gen.generateCode("TaskTable.tex");

		//Create task tree
		TaskTreeLatexCodeGenerator g = new TaskTreeLatexCodeGenerator(taskTree);
		g.generateCode("tasktree.tex");*/

		//Create schedule
		PrintWriter out = new PrintWriter("schedule.txt");
		out.print(optimal.getFirst().getSchedule().toString());
		out.close();
	}

	public String treeSetToString(TreeSet<Integer> set)
	{
		String s = "[";
		for(Integer i : set)
		{
			s+="R"+(i-1)+", ";
		}
		s=s.substring(0, s.length()-2);
		s+="]";
		return s;
	}
}