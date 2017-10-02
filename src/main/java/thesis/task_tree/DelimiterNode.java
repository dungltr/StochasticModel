package thesis.task_tree;

import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.CostPlanVisitor;
import thesis.query_plan_tree.Site;
import thesis.query_plan_tree.UnaryNode;
import thesis.utilities.Pair;

public class DelimiterNode extends UnaryNode implements Cloneable{
	
	private static final long serialVersionUID = 9091551643371508128L;

	public DelimiterNode(Site site, PlanNode child) {
		super(site, child);
		_outputCardinality = child.getOutputCardinality();
		_outputTupleSizeInBytes = child.getOutputTupleSizeInBytes();
	}
	
	public Pair<Double, Double> accept(CostPlanVisitor v) {
		return v.visit(this);	
	}

	public void accept(TreeVisitor v) {
		v.visit(this);
	}
	public boolean equals(Object that) {
		if(this == that)
			return true;
		else if(getClass().equals(that.getClass()))
		{
			try {
				return _child.equals(((DelimiterNode)that).getChildAt(0));
			} catch (QueryPlanTreeException e) {
				e.printStackTrace();
				return false;
			}
		}	
		else
			return false;
	}

	public int getNumberOfChildren() {
		return 1;
	}

	public String toString() {
		return "DELIMITER";
	}
	
	public int hashCode()
	{
		return 10+_child.hashCode();
	}
	
	public Object clone()
	{
		DelimiterNode clone = null;
		try 
		{
			clone = (DelimiterNode)super.clone();
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
