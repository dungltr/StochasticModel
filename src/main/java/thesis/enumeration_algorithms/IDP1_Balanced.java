package thesis.enumeration_algorithms;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import thesis.catalog.Catalog;

import thesis.joingraph.JoinGraph;
import thesis.code_generators.QueryPlanTreeCodeLatexCodeGenerator;
import thesis.code_generators.TaskTableGenerator;
import thesis.code_generators.TaskTreeLatexCodeGenerator;
import thesis.code_generators.TreeCodeGenerator;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanCost;
import thesis.scheduler.Scheduler;
import thesis.task_tree.TaskTree;
import thesis.iterators.CombinationIterator;

/**
 * Implements the IDP1 balanced best row variation from Donald Kossman paper
 * "Iterative Dynamic Programming".
 * 
 * @author rob
 */
public class IDP1_Balanced extends DynamicProgramming{
	protected final int _initialK;
	protected int _symbol=1;
	protected int _k;
	
	public IDP1_Balanced(JoinGraph g, int k)
	{
		super(g);
		_k = k;
		_initialK=k;
	}

	/**
	 * Uses the symbol number field to generate successive symbols to represent trees
	 * containing multiple relations.
	 * @return symbol
	 */
	public String generateSymbol()
	{
		return "_Stub_"+_symbol++;
	}
	
	/**
	 * To ensure that the trees are balanced we ensure that block sizes are even 
	 * and less than or equal to ceil(|_toDo|/2).  Of course when _toDo size is 
	 * 2 we will just have to make the block size equal to 2.
	 * @return
	 */
	public int getBlockSize(){
		if(_toDo.size()<=_initialK)
			return _toDo.size();
		else
		{
			int b = (int)Math.ceil((double)_toDo.size()/2.0);
			if(b%2==1)
				b--;
			return Math.min(_initialK, b);
		}
	}
	
	@SuppressWarnings("unchecked")
	protected void performSecondPhase() throws Exception
	{
		TreeSet<String> s = new TreeSet<String>();
		TreeSet<String> o = new TreeSet<String>();
		
		while (_toDo.size()>1)
		{
			//Balanced
			_k = getBlockSize();
			//System.out.println("k= "+_k);
			//Thread.sleep(5000);

			for(int i = 2; i<=_k; i++)
			{
				CombinationIterator<String> outerIter = 
					new CombinationIterator<String>(_toDo, i);

				while(outerIter.next_combination(s))
				{
					List<Plan> v = new ArrayList<Plan>();
					_optPlans.put((TreeSet<String>)s.clone(), v);

					for(int j = 1; j<i; j++)
					{
						CombinationIterator<String> innerIter = 
							new CombinationIterator<String>(s, j);

						while(innerIter.next_combination(o))
						{
							TreeSet<String> sWithoutO = ((TreeSet<String>)s.clone());
							sWithoutO.removeAll(o);
							
							List<Plan> o_Plans = _optPlans.get(o);
							List<Plan> sWithoutO_Plans = _optPlans.get(sWithoutO);
							//System.out.println("s: "+s);
							if(o_Plans==null){

								throw new Exception("No entry found for "+o);
							}

							if(sWithoutO_Plans==null){
								throw new Exception("No entry found for "+sWithoutO);
							}
							
							List<Plan> joined = joinPlans(o_Plans,sWithoutO_Plans);
							
							for(int k = 0; k<joined.size(); k++)
							{
								_optPlans.get(s).add(joined.get(k));
								prunePlans(_optPlans.get(s));
							}
						}
					}
				}
			}

			TreeSet<String> subset = new TreeSet<String>();
			TreeSet<String> V = new TreeSet<String>();
			PlanCost minCost = PlanCost.MAX_VALUE;
			
			CombinationIterator<String> iter = 
				new CombinationIterator<String>(_toDo, _k);

			List<Plan> trees = new ArrayList<Plan>();
				
			while(iter.next_combination(subset))
			{
				trees = _optPlans.get(subset);

				if(trees.isEmpty())
				{
					throw new Exception("optPlans "+subset+" has an entry but no trees!");
				}
				
				for(Plan tree : trees)
				{
					PlanCost cost = tree.getCost();
					if(cost.compareTo(minCost)<=0)
					{
						minCost = cost;
						V = (TreeSet<String>)subset.clone();
					}
				}
			}

			/*
			 * tempCost can sometimes by infinity....therefore you get no minimum.
			 * Fix this when you get the time....you just need to sort out the cost model!
			 */
			if(V.isEmpty()){
				throw new Exception("Error finding V");
			}
			
			TreeSet<String> symbol = new TreeSet<String>();
			symbol.add(generateSymbol());
			//Best Row
			_optPlans.put((TreeSet<String>)symbol.clone(), _optPlans.get(V));
			_toDo.removeAll(V);
			_toDo.add(symbol.first());
				
			for(int i = 1; i<V.size(); i++)
			{
				iter = new CombinationIterator<String>(V, i);

				while(iter.next_combination(subset))
				{
					_optPlans.remove(subset);
				}
			}
		}
	}

	public static void main(String args[]) throws Exception
	{
		String sites_filename = System.getenv().get("HOME") + "/temp/SystemSites.conf";
		String relations_filename = System.getenv().get("HOME") + "/temp/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		JoinGraph graph = new JoinGraph(System.getenv().get("HOME") + "/temp/Query1");
		long start = System.currentTimeMillis();
		IDP1_Balanced de = new IDP1_Balanced(graph, 4);
		de.enumerate();
		long end = System.currentTimeMillis();
		System.out.println("Time taken: "+(end-start)+" ms");
	}
}
