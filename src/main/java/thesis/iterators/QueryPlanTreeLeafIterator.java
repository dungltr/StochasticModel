package thesis.iterators;

import java.util.LinkedList;
import java.util.Queue;
import java.util.NoSuchElementException;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.RelationNode;

public class QueryPlanTreeLeafIterator {

	protected Queue<RelationNode> _nodes;
	
	public QueryPlanTreeLeafIterator(Plan tree)
	{
		_nodes = new LinkedList<RelationNode>();
		
		PostOrderTreeIterator iter = new PostOrderTreeIterator(tree);
		while(iter.hasNext())
		{
			PlanNode node = iter.next();
			if(node.getNumberOfChildren()==0)
				_nodes.add((RelationNode)node);
		}
	}
	
	public boolean hasNext()
	{
		return !_nodes.isEmpty();
	}
	
	public RelationNode next() throws NoSuchElementException
	{
		if(!hasNext()) throw new NoSuchElementException("Tried to get the next value of an empty iterator!");
		else{
			return _nodes.poll();
		}
	}
	
}
