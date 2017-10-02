package thesis.query_plan_tree;

import thesis.task_tree.TreeVisitor;
import thesis.utilities.Pair;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;

public class RelationNode extends PlanNode implements Cloneable, Comparable<RelationNode>{
	private static final long serialVersionUID = 5553289998771128472L;
	protected String _name;
	
	public RelationNode(Site site, String name) throws CatalogException {
		super(site);
		_name = name;
		_outputCardinality = Catalog.getCardinality(name);//Configuration.getRelationEntry(_name)._numberOfTuples;
		_outputTupleSizeInBytes = Catalog.getTupleSize(name);//Configuration.getRelationEntry(_name)._sizeOfTupleInBytes;
	}

	public PlanNode getChildAt(int c) throws QueryPlanTreeException {
		throw new QueryPlanTreeException("Tried to access child index "+c+
				" but valid child index is [0," + (getNumberOfChildren()-1)+"]");
	}

	public void setChildAt(int idx, PlanNode child)
	throws QueryPlanTreeException {
		throw new QueryPlanTreeException("Tried to access child index "+idx+
				" but valid child index is [0," + (getNumberOfChildren()-1)+"]");
	}
	
	public int getNumberOfChildren() {
		return 0;
	}

	public String getName()
	{
		return _name;
	}
	
	public void setName(String name)
	{
		_name = name;
	}
	
	public Pair<Double, Double> accept(CostPlanVisitor v) {
		return v.visit(this);
	}
	
	public void accept(TreeVisitor v) {
		v.visit(this);
	}
	
	public String toString() {
		return _name+" {"+_site+"}";
	}
	
	public String recurseToString()
	{
		return toString();
	}

	public boolean equals(Object that) {
		if(this == that) return true;
		else if(getClass().equals(that.getClass()))
		{
			RelationNode r = ((RelationNode)that);
			return r.getName().equals(_name) 
				&& _site.equals(r._site);
		}
		else return false;
	}
	
	public int hashCode()
	{
		return _name.hashCode();
	}
	
	public Object clone()
	{
		RelationNode r = null;
		r = (RelationNode) super.clone();
		return r;
	}

	public int compareTo(RelationNode r) {
		return _name.compareTo(r._name);
	}
	
	public void replaceChild(PlanNode child, PlanNode newChild)
	throws QueryPlanTreeException {
		throw new QueryPlanTreeException("Tried to replace child that does not exist!");
	}
}
