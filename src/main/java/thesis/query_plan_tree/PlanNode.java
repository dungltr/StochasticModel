package thesis.query_plan_tree;

import java.io.Serializable;

import thesis.task_tree.TreeVisitor;
import thesis.utilities.Pair;

import thesis.catalog.Catalog;

public abstract class PlanNode implements Cloneable, Serializable{
	private static final long serialVersionUID = 1218331883354579828L;
	public Site _site;
	protected PlanNode _parent;
	protected double _outputCardinality;
	protected int _outputTupleSizeInBytes;
	protected boolean _materializeOutput;
	
	public PlanNode(Site site)
	{ 
		_site = site; 
		_materializeOutput = true;
	}
	
	public double getOutputCardinality()
	{
		return _outputCardinality;
	}
	
	public boolean outputIsMaterialized()
	{
		return _materializeOutput;
	}
	
	public int getOutputTupleSizeInBytes()
	{
		return _outputTupleSizeInBytes;
	}
	
	public double getTotalOutputBytes()
	{
		return _outputCardinality*_outputTupleSizeInBytes;
	}
	
	public double getNumberOfOutputPages()
	{
		return Math.ceil(getTotalOutputBytes()/(double)Catalog._pageSize);
	}
	
	public boolean hasParent()
	{ 
		return _parent!=null;
	}
	
	public PlanNode getParent()
	{
		return _parent;
	}
	
	public void setParent(PlanNode parent)
	{
		_parent = parent;
	}
	
	public Site getSite()
	{
		return _site;
	}
	
	public abstract void replaceChild(PlanNode child, PlanNode new_child) throws QueryPlanTreeException;
	public abstract Pair<Double, Double> accept(CostPlanVisitor v);
	public abstract void accept(TreeVisitor v);
	public abstract int getNumberOfChildren();
	public abstract PlanNode getChildAt(int c) throws QueryPlanTreeException;
	public abstract void setChildAt(int idx, PlanNode child) throws QueryPlanTreeException;
	public abstract boolean equals(Object o);
	public abstract String toString();
	public abstract String recurseToString();
	
	public Object clone()
	{
		Object ret = null;
		try {
			ret = super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return ret;
	}
}
