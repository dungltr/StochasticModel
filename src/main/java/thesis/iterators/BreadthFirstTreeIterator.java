package thesis.iterators;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;

public class BreadthFirstTreeIterator {
	Queue<PlanNode> _nodes;
	
	public BreadthFirstTreeIterator(Plan tree)
	{
		_nodes = new LinkedList<PlanNode>();
		addToQueue(tree.getRoot());
	}
	
	public void addToQueue(PlanNode node)
	{
		try 
		{
			_nodes.add(node);
			for(int i = node.getNumberOfChildren()-1; i>=0; i--)
			{
				addToQueue(node.getChildAt(i));
			}
		} 
		catch (QueryPlanTreeException e) 
		{
			e.printStackTrace();
		}
	}
	
	public boolean hasNext()
	{
		return !_nodes.isEmpty();
	}
	
	public PlanNode next() throws NoSuchElementException
	{
		if(!hasNext()) throw new NoSuchElementException("Tried to get the next value of an empty iterator!");
		else{
			return _nodes.poll();
		}
	}
}
