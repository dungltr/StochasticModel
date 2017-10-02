package thesis.iterators;

import java.util.LinkedList;
import java.util.Queue;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;

public class DFTreeIterator {
	
	protected Plan _tree;
	protected Queue<PlanNode> _currentPath;
	
	public DFTreeIterator(Plan tree)
	{
		_tree = tree;
		_currentPath = new LinkedList<PlanNode>();
		_currentPath.add(_tree.getRoot());
	}
	
	public boolean hasNext()
	{
		return !_currentPath.isEmpty();
	}
	
	public PlanNode next()
	{
		PlanNode next = _currentPath.poll();
		try 
		{
			for(int i = 0; i<next.getNumberOfChildren(); i++)
			{
				_currentPath.add(next.getChildAt(i));
			}
		} 
		catch (QueryPlanTreeException e) 
		{
			e.printStackTrace();
		}
		return next;
	}
}
