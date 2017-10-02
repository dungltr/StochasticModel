package thesis.query_plan_tree;

public abstract class UnaryNode extends PlanNode implements Cloneable{
	private static final long serialVersionUID = -5624140031125162847L;
	protected PlanNode _child;
	
	public UnaryNode(Site site, PlanNode child)
	{
		super(site);
		_child = child;
		_child.setParent(this);
	}
	
	public PlanNode getChildAt(int c) throws QueryPlanTreeException {
		if(c==0) return _child;
		else throw new QueryPlanTreeException("Tried to access child index "+c+
				" but valid child index is [0," + (getNumberOfChildren()-1)+"]");
	}

	public void setChildAt(int idx, PlanNode child)
	throws QueryPlanTreeException 
	{
		if(idx == 0) _child = child;
		else throw new QueryPlanTreeException("Tried to set child index "+idx+
		" but valid child index is [0," + (getNumberOfChildren()-1)+"]");
	}
	
	public int getNumberOfChildren() {
		return 1;
	}
	
	public String recurseToString()
	{
		return toString()+"("+_child.recurseToString()+")";
	}
	
	public Object clone()
	{
		Object ret = null;
		ret = super.clone();
		return ret;
	}
	
	public void replaceChild(PlanNode child, PlanNode newChild)
	throws QueryPlanTreeException {
		if (_child.equals(child))
			_child=newChild;
		else throw new QueryPlanTreeException("Tried to replace child that does not exist!");

}
}
