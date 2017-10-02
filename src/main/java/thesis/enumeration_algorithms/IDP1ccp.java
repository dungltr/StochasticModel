package thesis.enumeration_algorithms;

import thesis.iterators.CombinationIterator;
import thesis.iterators.JoinGraphBreadthFirstIterator;
import thesis.iterators.QueryPlanTreeLeafIterator;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanCost;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.Site;
import thesis.scheduler.SchedulerException;
import thesis.utilities.Pair;

/**
 * Graph has to be numbered in breadth first fashion! This is the precondition
 * to use DPccp.
 * @author rob
 *
 */
public class IDP1ccp {

	protected JoinGraph _joinGraph; 
	protected Map<String, Plan> _optimizedSubplans; 
	protected int _k;
	protected final int _initialK;
	protected int _symbol;
	protected List<String> _relationIDs;
	protected Map<String, Integer> _relationIDMapping;
	protected TreeSet<Integer> _relations;
	protected Map<TreeSet<String>, List<Plan>> _bestPlans;
	protected Set<TreeSet<Integer>> _kSizedSubsets;
	protected int count = 1;

	public IDP1ccp(JoinGraph graph, int k) throws JoinGraphException
	{
		_optimizedSubplans = new HashMap<String, Plan>();
		_joinGraph = graph;
		_k = k;
		_initialK=k;
		_symbol = 1;
		_relations = new TreeSet<Integer>();
		_bestPlans = new HashMap<TreeSet<String>, List<Plan>>();
		_kSizedSubsets = new HashSet<TreeSet<Integer>>();
		renumber();
	}

	protected void addRelationMapping(String name)
	{
		_relationIDs.add(name);
		_relationIDMapping.put(name, count);
		count++;
	}

	protected void performFirstPhase() throws JoinGraphException, CatalogException
	{
		JoinGraphBreadthFirstIterator iter = new JoinGraphBreadthFirstIterator(_joinGraph);
		String relationID = "";

		while(iter.hasNext())
		{
			relationID = iter.next();

			for(Iterator<Site> site_iter = Catalog.getAvailableSitesIterator(relationID);
			site_iter.hasNext();)
			{
				Site site = site_iter.next();
				RelationNode r = new RelationNode(site, relationID);
				Plan tree = new Plan(r);

				TreeSet<String> relation = new TreeSet<String>();
				relation.add(relationID);

				if (_bestPlans.containsKey(relation))
					_bestPlans.get(relation).add(tree);
				else{
					List<Plan> trees = new LinkedList<Plan>();
					trees.add(tree);
					_bestPlans.put(relation, trees);
				}
			}

			_relations.add(_relationIDMapping.get(relationID));
		}
	}

	/**
	 * To ensure that the trees are balanced we ensure that block sizes are even 
	 * and less than or equal to ceil(|_toDo|/2).  Of course when _toDo size is 
	 * 2 we will just have to make the block size equal to 2.
	 * @return
	 */
	public int getBlockSize(){
		
		if(_joinGraph.getNumberOfVertices()<=_initialK)
			return _joinGraph.getNumberOfVertices();
		else
		{
			int b = (int)Math.ceil((double)_joinGraph.getNumberOfVertices()/2.0);
			if(b%2==1)
				b--;
			return Math.min(_initialK, b);
		}
		//return Math.min(_initialK, _joinGraph.getNumberOfVertices());
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

	@SuppressWarnings("unchecked")
	public void performSecondPhase() throws Exception
	{

		while (_joinGraph.getNumberOfVertices()>1)
		{

			_k = getBlockSize();
			List<TreeSet<Integer>> csgs = new LinkedList<TreeSet<Integer>>();
			List<TreeSet<Integer>> cmps = new LinkedList<TreeSet<Integer>>();
			TreeSet<Integer> s = new TreeSet<Integer>();

			enumerateCsg(_joinGraph, _k-1, csgs);

			for(TreeSet<Integer> s1 : csgs)
			{
				
				enumerateCmp(_joinGraph, s1, _k, cmps);

				for(TreeSet<Integer> s2 : cmps)
				{
					s.clear();
					
					s.addAll(s1);
					s.addAll(s2);
					
					if(s.size()==_k)
						_kSizedSubsets.add((TreeSet<Integer>)s.clone());

					List<Plan> p1 = _bestPlans.get(convertToString(s1));
					List<Plan> p2 = _bestPlans.get(convertToString(s2));
					List<Plan> joined = joinPlans(p1, p2);
					//_numberOfPlansConsidered+=joined.size();
					
					if(!_bestPlans.containsKey(convertToString(s))){
						_bestPlans.put(convertToString((TreeSet<Integer>)s.clone()), 
								new LinkedList<Plan>());
					}

					for(Plan tree : joined)
					{
						_bestPlans.get(convertToString(s)).add(tree);
						prunePlans(_bestPlans.get(convertToString(s)));
					}
				}
				cmps.clear();
			}
			
			//Is this the last stub?
			Pair<TreeSet<Integer>, Plan> p = performThirdPhase(_k!=_joinGraph.getNumberOfVertices());
			
			TreeSet<Integer> V = p.getFirst();
			Plan minTree = p.getSecond();
			
			_kSizedSubsets.clear();

			if(V.isEmpty()){
				throw new Exception("Error finding V");
			}

			String stubIdentifier = processNewTempRelation(minTree);

			JoinGraph removed = _joinGraph.absorb(convertToString(V), stubIdentifier);

			//if(_k>_joinGraph.getNumberOfVertices()){
				IDP1ccp removedCcp = new IDP1ccp(removed, _k);
				List<TreeSet<String>> names= removedCcp.enumerateCsg(removed, _k);

				for(TreeSet<String> n : names)
					_bestPlans.remove(n);
				renumber();
			//}
		}
		
	}

	protected String processNewTempRelation(Plan minTree) throws CatalogException{
		String stubIdentifier = generateSymbol();

		TreeSet<Integer> symbol = new TreeSet<Integer>();
		symbol.add(count);
		addRelationMapping(stubIdentifier);
		_optimizedSubplans.put(stubIdentifier, minTree);

		Set<Site> sites = new HashSet<Site>();
		sites.add(minTree.getRoot().getSite());
		
		double card = Math.ceil(minTree.getRoot().getOutputCardinality());
		
		if(card==0)
			card=1;
			
		Catalog.addRelation(stubIdentifier, card, 
				minTree.getRoot().getOutputTupleSizeInBytes(), sites);
		RelationNode r = new RelationNode(minTree.getRoot().getSite(), stubIdentifier);

		TreeSet<String> id = new TreeSet<String>();
		id.add(stubIdentifier);
		Plan tree = new Plan(r);
		List<Plan> trees = new LinkedList<Plan>();
		trees.add(tree);
		_bestPlans.put(id, trees);
		return stubIdentifier;
	}
	protected Pair<TreeSet<Integer>, Plan> performThirdPhase(boolean greedyFinalSite) 
	throws QueryPlanTreeException, SchedulerException {

		TreeSet<Integer> V = new TreeSet<Integer>();
		List<Pair<TreeSet<Integer>, Plan>> trees = 
			new LinkedList<Pair<TreeSet<Integer>, Plan>>();
		
		for(TreeSet<Integer> subset : _kSizedSubsets)
		{
			for(Plan tree : _bestPlans.get(convertToString(subset))){
					V = (TreeSet<Integer>)subset.clone();
					trees.add(new Pair<TreeSet<Integer>, Plan>(V, tree));
			}
		}
	
		List<Plan> minTrees = new ArrayList<Plan>();
		PlanCost minTreeCost = null;
		TreeSet<Integer> minV = new TreeSet<Integer>();
		
		Site querySite = Catalog.getQuerySite();

		for(Pair<TreeSet<Integer>, Plan> q : trees)
		{
			Plan tree = q.getSecond();
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
				minV=q.getFirst();
			}   
			else if(cost.compareTo(minTreeCost)<0)
			{
				minTrees.clear();
				minTrees.add(tree);
				minTreeCost=cost;
				minV=q.getFirst();
			}
			else if(cost.compareTo(minTreeCost)==0){
				minTrees.add(tree);
				minV=q.getFirst();
			}
		}
		// System.out.println("number of pos: "+minTrees.size());
		Plan t = minTrees.get(0);
		return new Pair<TreeSet<Integer>, Plan>(minV, t);
	}
	
	public String generateSymbol()
	{
		return "_Stub_"+_symbol++;
	}

	public List<Plan> joinPlans(List<Plan> o, List<Plan> i) 
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

	public List<TreeSet<String>> enumerateCsg(JoinGraph graph, int k) throws JoinGraphException
	{
		List<TreeSet<Integer>> results = new LinkedList<TreeSet<Integer>>();
		enumerateCsg(graph, k, results);
		List<TreeSet<String>> relations = new LinkedList<TreeSet<String>>();

		for(TreeSet<Integer> i : results)
		{
			TreeSet<String> names = new TreeSet<String>();
			for(int l : i)
				names.add(_relationIDs.get(l-1));
			relations.add(names);
		}
		return relations;
	}
	/**
	 * Input: A connected query graph G = (V,E).
	 * Precondition: nodes in V are numbers according to a breadth-first search.
	 * Output: emits all subsets of V inducing a connected subgraph of G
	 * @param graph 
	 * @param graph
	 * @throws JoinGraphException 
	 * @throws JoinGraphException
	 */
	public void enumerateCsg(JoinGraph graph, int k, List<TreeSet<Integer>> result) throws JoinGraphException{
		if(k<1 || k>graph.getNumberOfVertices()) 
			throw new IndexOutOfBoundsException("\nsize of required subgraphs should be greater than 1 " +
			"and less than the number of vertices in graph.");

		TreeSet<Integer> vertices = new TreeSet<Integer>();

		for(int i = graph.getNumberOfVertices(); i>=1; i--)
		{
			vertices.add(i);
			emit(vertices, result);
			enumerateCsgRec(graph, vertices, B(i), k, result);
			vertices.clear();
		}
	}

	public void renumber() throws JoinGraphException{
		_relationIDs = new ArrayList<String>();
		_relationIDMapping = new HashMap<String, Integer>();
		count=1;

		JoinGraphBreadthFirstIterator iter = new JoinGraphBreadthFirstIterator(_joinGraph);
		String relationID = "";
		while(iter.hasNext())
		{
			relationID = iter.next();
			addRelationMapping(relationID);
		}
	}

	public void enumerateCsgRec(JoinGraph graph, TreeSet<Integer> s, TreeSet<Integer> x, 
			int k, List<TreeSet<Integer>> result) throws JoinGraphException
			{
		TreeSet<Integer> n = getNeighbours(graph, s);
		n.removeAll(x);

		TreeSet<Integer> subset = new TreeSet<Integer>();

		for(int i = 1; i<=Math.min(k-s.size(), n.size()); i++){
			CombinationIterator<Integer> iter = new CombinationIterator<Integer>(n, i);
			while(iter.next_combination(subset))
			{
				subset.addAll(s);
				emit(subset, result);
			}
		}

		TreeSet<Integer> xUnionN = new TreeSet<Integer>();
		xUnionN.addAll(x);
		xUnionN.addAll(n);

		for(int i = 1; i<=Math.min(k-s.size(), n.size()); i++){
			CombinationIterator<Integer> iter = new CombinationIterator<Integer>(n, i);
			while(iter.next_combination(subset))
			{
				subset.addAll(s);
				enumerateCsgRec(graph, subset, xUnionN, k, result);
			}
		}
			}

	protected TreeSet<Integer> getNeighbours(JoinGraph graph, TreeSet<Integer> k) throws JoinGraphException
	{
		TreeSet<String> l = new TreeSet<String>(convertToString(k));
		TreeSet<Integer> t = new TreeSet<Integer>();
		//System.out.println(l);
		for(String p : graph.getNeighbours(l))
		{
			t.add(_relationIDMapping.get(p));
		}
		return t;
	}

	protected TreeSet<String> convertToString(TreeSet<Integer> s)
	{
		TreeSet<String> k = new TreeSet<String>();

		for(Integer i : s)
			k.add(_relationIDs.get(i-1));
		return k;
	}

	@SuppressWarnings("unchecked")
	public void emit(TreeSet<Integer> subgraph, List<TreeSet<Integer>> result)
	{
		result.add((TreeSet<Integer>)subgraph.clone());
	}

	//
	// Input:  a connected query graph G = (V,E), a connected subset S1
	// Precondition: nodes in V are numbered according to a breadth-first search.
	// Output: emits all complements S2 for S1 such that (S2, S1) is a csg-cmp-pair.
	//
	@SuppressWarnings("unchecked")
	public void enumerateCmp(JoinGraph graph, TreeSet<Integer> s1, int l, List<TreeSet<Integer>> result) throws JoinGraphException
	{
		TreeSet<Integer> x = new TreeSet<Integer>();
		x.addAll(B(s1.first()));
		x.addAll(s1);

		TreeSet<Integer> n = getNeighbours(graph, s1);
		n.removeAll(x);

		TreeSet<Integer> prohibitedArea = (TreeSet<Integer>)n.clone();
		TreeSet<Integer> vertex = new TreeSet<Integer>();

		Iterator<Integer> iter = n.descendingIterator();
		TreeSet<Integer> intersection = new TreeSet<Integer>();
		while(iter.hasNext())
		{
			int i = iter.next();
			vertex.add(i);
			emit(vertex, result);
			intersection = B(i);
			intersection.retainAll(n);
			prohibitedArea.addAll(x);
			prohibitedArea.addAll(intersection);
			enumerateCsgRec(graph, vertex, prohibitedArea, l-s1.size(), result);
			vertex.clear();
			intersection.clear();
			prohibitedArea.clear();
		}
	}

	public TreeSet<Integer> B(int i)
	{
		TreeSet<Integer> set = new TreeSet<Integer>();
		for(int j = 1; j<=i; j++)
		{
			set.add(j);
		}
		return set;
	}
	public Plan enumerate() throws Exception
	{
		performFirstPhase();
		performSecondPhase();
		//for(Entry<String, QueryPlanTree> entry : _optimizedSubplans.entrySet())
		//	entry.getValue().outputToFile("multilevel_trees/"+entry.getKey()+".tex");
		Plan optimal = buildFinalPlan();
		optimal.outputToFile("multilevel_trees/FinalTree.tex");
		return optimal;
	}

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


	protected Plan buildFinalPlan() throws QueryPlanTreeException{
		Plan tree = _optimizedSubplans.get("_Stub_"+(_symbol-1));
		List<Pair<String, PlanNode>> stubs = getStubEntries(tree);

		while(!stubs.isEmpty())
		{
			//System.out.println(stubs);
			for(Pair<String, PlanNode> p : stubs)
			{
				p.getSecond().getParent().replaceChild(p.getSecond(), 
						_optimizedSubplans.get(p.getFirst()).getRoot());
			}
			stubs = getStubEntries(tree);
		}

		return tree;
	}
	public static void main(String args[]) 
	throws Exception
	{
		String sites_filename = "/home/rob/Desktop/SystemSites.conf";
		String relations_filename = "/home/rob/Desktop/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		JoinGraph graph = new JoinGraph("/home/rob/Desktop/Query1");
		long start = System.currentTimeMillis();
		IDP1ccp de = new IDP1ccp(graph, 10);
		de.enumerate();
		long end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+" ms");
		/*TaskTree taskTree = new TaskTree(tree);
		Scheduler scheduler = new Scheduler(taskTree);
		scheduler.schedule();
		long end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+" ms");

		TreeCodeGenerator gc = new QueryPlanTreeCodeLatexCodeGenerator(tree);
		gc.generateCode("tree.tex");
		TaskTableGenerator gen = new TaskTableGenerator(taskTree);
		gen.generateCode("TaskTable.tex");
		TaskTreeLatexCodeGenerator g = new TaskTreeLatexCodeGenerator(taskTree);
		g.generateCode("tasktree.tex");


		PrintWriter out = new PrintWriter("schedule.txt");
		out.print(scheduler.toString());
		out.close();*/
	}
}
