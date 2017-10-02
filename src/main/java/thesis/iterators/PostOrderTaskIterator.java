package thesis.iterators;

import java.util.NoSuchElementException;
import java.util.Stack;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.task_tree.DelimiterNode;
import thesis.task_tree.Task;

public class PostOrderTaskIterator {
	Stack<PlanNode> _nodes;
	
	public PostOrderTaskIterator(Task task)
	{
		_nodes = new Stack<PlanNode>();
		addToStack(task.getTree().getRoot());
	}
	
	public void addToStack(PlanNode node)
	{
		_nodes.push(node);
		for(int i = node.getNumberOfChildren()-1; i>=0; i--)
		{
			try {
				if(!node.getChildAt(i).getClass().equals(DelimiterNode.class))
				{
					addToStack(node.getChildAt(i));
				}
			} catch (QueryPlanTreeException e) {
				e.printStackTrace();
			}
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
