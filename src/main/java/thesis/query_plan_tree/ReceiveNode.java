package thesis.query_plan_tree;

import thesis.task_tree.TreeVisitor;
import thesis.utilities.Pair;

public class ReceiveNode extends UnaryNode implements Cloneable{
	private static final long serialVersionUID = 336412393717688374L;

	public ReceiveNode(Site site, PlanNode child) {
		super(site, child);
		_outputCardinality = _child._outputCardinality;
		_outputTupleSizeInBytes = _child._outputTupleSizeInBytes;
	}

	public Pair<Double, Double> accept(CostPlanVisitor v) {
		return v.visit(this);
	}
	
	public void accept(TreeVisitor v) {
		v.visit(this);
	}
	
	public boolean equals(Object that) {
		if(this == that) return true;
		else if(getClass().equals(that.getClass()))
		{
			ReceiveNode node = ((ReceiveNode)that);
			
			if(_site.equals(node.getSite()) && _child.equals(node._child))
				return true;
			else
				return false;
		}
		else return false;
	}

	public String toString() {
		return "RECEIVE {"+_site+"}";
	}
	
	public int hashCode()
	{
		return _site.hashCode()+_child.hashCode();
	}
	
	public Object clone()
	{
		ReceiveNode clone = null;
		try 
		{
			clone = (ReceiveNode)super.clone();
			PlanNode child_clone = (PlanNode)_child.clone();
			clone.setChildAt(0, child_clone);
			child_clone.setParent(clone);
		} 
		catch (QueryPlanTreeException e) 
		{
			e.printStackTrace();
		}		
		return clone;
	}
}
