package thesis.iterators;

import java.util.NoSuchElementException;
import java.util.Stack;

import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;

public class PostOrderTreeIterator {
	Stack<PlanNode> _nodes;
	
	public PostOrderTreeIterator(Plan tree)
	{
		_nodes = new Stack<PlanNode>();
		addToStack(tree.getRoot());
	}
	
	public void addToStack(PlanNode node)
	{
		try 
		{
			_nodes.push(node);
			for(int i = node.getNumberOfChildren()-1; i>=0; i--)
			{
				addToStack(node.getChildAt(i));
			}
		} 
		catch (QueryPlanTreeException e) 
		{
			e.printStackTrace();
		}
	}
	
	public boolean hasNext()
	{
		return !_nodes.empty();
	}
	
	public PlanNode next() throws NoSuchElementException
	{
		if(!hasNext()) throw new NoSuchElementException("Tried to get the next value of an empty iterator!");
		else{
			return _nodes.pop();
		}
	}
}
