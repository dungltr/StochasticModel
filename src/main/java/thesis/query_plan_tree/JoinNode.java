package thesis.query_plan_tree;

import java.util.Set;
import java.util.TreeSet;

import thesis.task_tree.TreeVisitor;
import thesis.utilities.Pair;

import thesis.joingraph.JoinCondition;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.joingraph.JoinInfo;

import thesis.iterators.QueryPlanTreeLeafIterator;

public class JoinNode extends PlanNode {
	private static final long serialVersionUID = -1460944165999122832L;
	protected PlanNode _outer;
	protected PlanNode _inner;
	protected JoinCondition _joinCondition;
	//protected String _outer_attribute;
	//protected String _inner_attribute;
	
	
	public JoinNode(Site site, PlanNode outer, PlanNode inner, JoinGraph graph) throws JoinGraphException 
	{
		super(site);
		_outer = outer;
		_inner = inner;
		_outer.setParent(this);
		_inner.setParent(this);
		//JoinEntry entry = Configuration.getJoinEntry(this);	
		QueryPlanTreeLeafIterator leftIter = new QueryPlanTreeLeafIterator(new Plan(_outer));
		Set<String> outerRelations = new TreeSet<String>();
		while(leftIter.hasNext())
			outerRelations.add(leftIter.next().getName());
		
		QueryPlanTreeLeafIterator rightIter = new QueryPlanTreeLeafIterator(new Plan(_inner));
		Set<String> innerRelations = new TreeSet<String>();
		while(rightIter.hasNext())
			innerRelations.add(rightIter.next().getName());
		//System.out.println("Getting sel of "+outerRelations+" : "+innerRelations);
		//System.out.println("Sel: "+graph.getSelectivity(outerRelations, innerRelations));
		//System.out.println("outer cardinality: "+_outer._outputCardinality);
		//System.out.println("inner cardinality: "+_inner._outputCardinality);
		JoinInfo info = graph.getJoinInfo(outerRelations, innerRelations);
		_outputCardinality = Math.ceil(_outer._outputCardinality*_inner._outputCardinality*info.getSelectivity());
		//entry._selectivity;
		_joinCondition = info.getJoinCondition();
		_outputTupleSizeInBytes = _outer._outputTupleSizeInBytes+_inner._outputTupleSizeInBytes;
	}

	public PlanNode getChildAt(int c) throws QueryPlanTreeException
	{
		if(c==0) return _outer;
		else if(c==1) return _inner;
		else throw new QueryPlanTreeException("Tried to access child index "+c+
				" but valid child index is [0," + (getNumberOfChildren()-1)+"]");
	}

	public void setChildAt(int idx, PlanNode child)
	throws QueryPlanTreeException 
	{
		if(idx==0)  _outer = child;
		else if(idx==1) _inner = child;
		else throw new QueryPlanTreeException("Tried to set child index "+idx+
				" but valid child index is [0," + (getNumberOfChildren()-1)+"]");
	}
	
	public int getNumberOfChildren() 
	{
		return 2;
	}
	

	public boolean equals(Object that) 
	{
		if(this == that) return true;
		else if(getClass().equals(that.getClass()))
		{
			JoinNode node = ((JoinNode)that);
			return	node._inner.equals(_inner) && node._outer.equals(_outer)
				&& 	node._site.equals(_site);

		}
		else return false;
	}
	
	public JoinCondition getJoinCondition(){
		return _joinCondition;
	}
	public int hashCode()
	{
		return 	 _outer.hashCode()+_inner.hashCode();
	}
	
	public String toString()
	{
		String s = "JOIN ("+_joinCondition+"){"+_site+"}";
		return s;
	}
	
	public Object clone()
	{
		JoinNode clone = null;
		try 
		{
			clone = (JoinNode)super.clone();
			PlanNode _outerClone = (PlanNode)_outer.clone();
			PlanNode _innerClone = (PlanNode)_inner.clone();
			clone.setChildAt(0, _outerClone);
			clone.setChildAt(1, _innerClone);
			_outerClone.setParent(clone);
			_innerClone.setParent(clone);
		} 
		catch (QueryPlanTreeException e) 
		{
			e.printStackTrace();
		}		
		return clone;
	}

	public Pair<Double, Double> accept(CostPlanVisitor v) {
		return v.visit(this);
	}
	
	public void accept(TreeVisitor v) {
		v.visit(this);
	}
	
	public String recurseToString()
	{
		return "JOIN("+_site+") ["+_outer.recurseToString()+"] ["+_inner.recurseToString()+"]";
	}

	public void replaceChild(PlanNode child,
			PlanNode newChild) throws QueryPlanTreeException {
		
		if(_inner.equals(child))
			_inner=newChild;
		else if(_outer.equals(child))
			_outer = newChild;
		else throw new QueryPlanTreeException("Tried to replace child that does not exist!");
	}
}
