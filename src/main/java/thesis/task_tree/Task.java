package thesis.task_tree;

import java.util.ArrayList;
import java.util.List;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.Site;

public class Task {
//	static int _nextID = 0;
	Plan _tree;
	Task _parent;
	List<Task> _children;
	double _duration;
	int _id;
	
	public Task(PlanNode node, int id)
	{
		_tree = new Plan(node);
		_children = new ArrayList<Task>();
		_parent = null;
		_id = id;//_nextID++;
		_duration=0;
	}
	
	public int getID()
	{
		return _id;
	}
	
	public void addChild(Task node)
	{
		_children.add(node);
	}
	
	public double getDuration()
	{
		return _duration;
	}
	
	public void setDuration(double duration)
	{
		_duration = duration;
	}
	
	public Plan getTree()
	{
		return _tree;
	}
	
	public void setParent(Task parent)
	{
		_parent = parent;
	}
	
	public Task getParent()
	{
		return _parent;
	}
	
	public int getNumberOfChildren(){ return _children.size(); }
	
	public Task getChildAt(int i){ return _children.get(i); }
	
	public Site getSite()
	{
		return  _tree.getRoot().getSite();
	}
	
	public String toString(){ 
		
		return _tree.toString();
	}
	
	public boolean equals(Object that)
	{
		if(this==that) return true;
		else if(that.getClass().equals(Task.class))
		{
			if(_id == ((Task)that)._id)
				return true;
			else return false;
		}
		else return false;
	}
	public int hashCode()
	{
		return _id;
	}
}
