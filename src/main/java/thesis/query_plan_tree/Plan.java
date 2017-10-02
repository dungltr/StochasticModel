package thesis.query_plan_tree;

import java.io.Serializable;

import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;

import thesis.code_generators.QueryPlanTreeCodeLatexCodeGenerator;
import thesis.code_generators.TreeCodeGenerator;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;

public class Plan implements Cloneable, Serializable{
	private static final long serialVersionUID = -394399334368017755L;
	protected PlanNode _root;
	
	public Plan(PlanNode root)
	{
		_root = root;
	}
	
	public PlanNode getRoot()
	{
		return _root;
	}
	
	public void setRoot(PlanNode new_root)
	{
		_root = new_root;
	}
	
	public void shipToSite(Site site) throws QueryPlanTreeException{
		if(site.equals(_root.getSite()))
			throw new QueryPlanTreeException("Tried to attach ship node to tree who root is " +
					"at same site as destination.");
		else{
			SendNode s = new SendNode(_root.getSite(), _root);
			ReceiveNode r = new ReceiveNode(site, s);
			setRoot(r);
		}
	}
	
	public static Plan join(Site site, Plan outerTree, Plan innerTree, JoinGraph g) 
	throws JoinGraphException
	{
		Plan lhs = (Plan)outerTree.clone();
		Plan rhs = (Plan)innerTree.clone();
		
		//Add appropriate transmission nodes if sites of roots of 
		//trees do not match site of join
		if(!lhs.getRoot().getSite().equals(site))
		{
			SendNode s = new SendNode(lhs.getRoot().getSite(), lhs.getRoot());
			ReceiveNode r = new ReceiveNode(site, s);
			lhs.setRoot(r);
		}
		
		if(!rhs.getRoot().getSite().equals(site))
		{
			SendNode s = new SendNode(rhs.getRoot().getSite(), rhs.getRoot());
			ReceiveNode r = new ReceiveNode(site, s);
			rhs.setRoot(r);
		}
		
		JoinNode new_root = new JoinNode(site, lhs.getRoot(), rhs.getRoot(), g);
		//System.out.println("lhs: "+lhs.getRoot()+" rhs: "+rhs.getRoot()+" joined: "+new_root);
		return new Plan(new_root);
	}
	
	public PlanCost getCost() throws SchedulerException
	{
	   
	    return new PlanCost(this);
	}
	
	public Scheduler getSchedule() throws SchedulerException{
		TaskTree taskTree = new TaskTree(this);
	    Scheduler scheduler = new Scheduler(taskTree);
	    scheduler.schedule();
	    return scheduler;
	}
	public boolean isCheaperThan(Plan tree) throws SchedulerException
	{
		PlanCost thisCost = getCost();
		PlanCost thatCost = tree.getCost();
		return thisCost.compareTo(thatCost)==-1;	
	}
	
	public boolean equals(Object that)
	{
		return _root.equals(that);
	}
	
	public int hashCode()
	{
		return _root.hashCode();
	}
	
	public String toString()
	{
		return _root.recurseToString();//toString();
	}
	
	public String recurseToString()
	{
		return _root.recurseToString();
	}
	public Object clone()
	{
		Plan clone = null;
		try {
			clone = (Plan)super.clone();
			clone._root = (PlanNode)_root.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		assert(clone.getRoot()!=null);
		return clone;
	}
	
	public void outputToFile(String filename){
		TreeCodeGenerator gc = new QueryPlanTreeCodeLatexCodeGenerator(this);
		gc.generateCode(filename);
	}
}
