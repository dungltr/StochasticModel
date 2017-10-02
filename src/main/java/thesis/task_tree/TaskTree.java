package thesis.task_tree;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import thesis.code_generators.TaskTreeLatexCodeGenerator;

import thesis.iterators.PostOrderTaskTreeIterator;
import thesis.iterators.PostOrderTreeIterator;
import thesis.query_plan_tree.JoinNode;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.CostPlanVisitor;
import thesis.query_plan_tree.ReceiveNode;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.SendNode;
import thesis.utilities.Pair;

public class TaskTree implements TreeVisitor{

	protected Map<PlanNode, Task> _tasks;
	protected Task _root;
	protected int _nextTaskID;
	protected double _totalNumberOfIOs;
	protected double _totalNumberOfBytesSent;
	
	/*public TaskTree(Task root)
	{
		_root = root;
		_tasks = new HashMap<QueryPlanTreeNode, Task>();
	}*/
	public TaskTree(Plan tree)
	{
		_tasks = new HashMap<PlanNode, Task>();
		_root = null;
		_nextTaskID=1;
		_totalNumberOfIOs=0;
		_totalNumberOfBytesSent=0;
		
		Plan clonedTree = (Plan)tree.clone();
		PostOrderTreeIterator iter = new PostOrderTreeIterator(clonedTree);
		while(iter.hasNext())
		{
			PlanNode node = iter.next();
			node.accept(this);
		}
		
		_root = _tasks.get(clonedTree.getRoot());
		//clonedTree.outputToFile("treeWithDelimiter.tex");
		_tasks.clear();
		_tasks = null;
		calculateCost();
	}
	
	public void calculateCost()
	{
		int count = 1;
		PostOrderTaskTreeIterator iter = new PostOrderTaskTreeIterator(this);
		while(iter.hasNext())
		{
			Task node = iter.next();
			node._id=count;
			TaskCostEvaluator evaluator = new TaskCostEvaluator(node);
			node.setDuration(evaluator.getCost());
			_totalNumberOfBytesSent+=evaluator.getTotalBytesSentOverNetwork();
			_totalNumberOfIOs+=evaluator.getTotalNumberOfIOs();
			count++;
		}
		
	}
	public Task getRoot()
	{
		return _root;
	}
	
	public void setRoot(Task new_root)
	{
		_root = new_root;
	}
	
	public boolean equals(Object that)
	{
		return _root.equals(that);
	}
	
	public int hashCode()
	{
		return _root.hashCode();
	}
	

	public void visit(JoinNode node) {
		try {
			PlanNode outer = node.getChildAt(0);
			PlanNode inner = node.getChildAt(1);
			
			boolean merge = !outer.getClass().equals(ReceiveNode.class) &&
			!inner.getClass().equals(ReceiveNode.class) &&
			node._site.equals(outer._site) && node._site.equals(inner._site);
			
			if(merge)
			{
				
				Task outerTask = _tasks.get(outer);
				Task innerTask = _tasks.get(inner);
				_nextTaskID = Math.max(outerTask.getID(), innerTask.getID());
				Task currentTask = new Task(node, _nextTaskID++);
				
				for(int i = 0; i<outerTask.getNumberOfChildren(); i++)
				{
					Task child = outerTask.getChildAt(i);
					child.setParent(currentTask);
					currentTask.addChild(child);
				}
				
				for(int i = 0; i<innerTask.getNumberOfChildren(); i++)
				{
					Task child = innerTask.getChildAt(i);
					child.setParent(currentTask);
					currentTask.addChild(child);
				}
				
				_tasks.put(node, currentTask);
				_tasks.remove(outerTask);
				_tasks.remove(innerTask);
			}
			else
			{
				Task outerTask = _tasks.get(outer);
				Task innerTask = _tasks.get(inner);
				Task currentTask = new Task(node, _nextTaskID++);
				DelimiterNode outDelimiter = new DelimiterNode(outerTask._tree.getRoot()._site, 
						outerTask._tree.getRoot()); 
				outerTask._tree.getRoot().setParent(outDelimiter);
				
				DelimiterNode inDelimiter = new DelimiterNode(innerTask._tree.getRoot()._site,
						innerTask._tree.getRoot());
				innerTask._tree.getRoot().setParent(inDelimiter);
				node.setChildAt(0, outDelimiter);
				node.setChildAt(1, inDelimiter);
				
				outerTask.setParent(currentTask);
				innerTask.setParent(currentTask);
				currentTask.addChild(outerTask);
				currentTask.addChild(innerTask);
				_tasks.put(node, currentTask);
			}
			
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
		}
	}
	
	public double getTotalIO(){
		return _totalNumberOfIOs;
	}
	
	public double getBytesSentOverNetwork(){
		return _totalNumberOfBytesSent;
	}

	public void visit(ReceiveNode node) {
		try {
			
			Task childTask = _tasks.get(node.getChildAt(0));
			
			DelimiterNode delimiter = new DelimiterNode(childTask._tree.getRoot()._site, 
					childTask._tree.getRoot()); 
			childTask._tree.getRoot().setParent(delimiter);
			node.setChildAt(0, delimiter);
			
			Task currentTask = new Task(node, _nextTaskID++);
			currentTask.addChild(childTask);
			childTask.setParent(currentTask);
			_tasks.put(node, currentTask);
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
		}
	}

	public void visit(SendNode node) {
		try {	
			
			Task childTask = _tasks.get(node.getChildAt(0));
			DelimiterNode delimiter = new DelimiterNode(childTask._tree.getRoot()._site, 
					childTask._tree.getRoot()); 
			childTask._tree.getRoot().setParent(delimiter);
			node.setChildAt(0, delimiter);
			Task currentTask = new Task(node,  _nextTaskID++);
			currentTask.addChild(childTask);
			childTask.setParent(currentTask);
			_tasks.put(node, currentTask);
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
		}
	}

	public void visit(RelationNode node) {
		Task currentTask = new Task(node,  _nextTaskID++);
		_tasks.put(node, currentTask);
	}

	public void visit(DelimiterNode node) {
		throw new UnsupportedOperationException("Encountered delimiter node in Task Tree, not " +
		"implemented yet!");
	}
	
	protected String produceTabs(int tab_level)
	{
		if(tab_level<0)
			throw new IndexOutOfBoundsException("Tabbing level must be greater than or equal to 1");
		
		String tabs = "";
		for(int i = 0; i<tab_level; i++)
		{
			tabs+="\t";
			if(i<tab_level-1)
				tabs+=".";
		}
		return tabs;
	}
	
	public String toString()
	{
		int tab_level = 0;
		int count = 1;
		String s = "";
		
		Stack<Pair<Task, Integer>> node_stack = 
			new Stack<Pair<Task, Integer>>();
		
			node_stack.push(new Pair<Task, Integer>(_root, -1));
			Task node = node_stack.peek().getFirst();
			s+= produceTabs(tab_level)+"["+count+" - "+node+"\n";
			
			count++;
			System.out.println("s: "+s);
			while(node_stack.size()>0)
			{
				if(node_stack.peek().getFirst().getNumberOfChildren()==0){
					node_stack.pop();
				}
				else
				{
					if(node_stack.peek().getSecond()<
							node_stack.peek().getFirst().getNumberOfChildren()-1)
					{
						if(node_stack.peek().getSecond()==-1)
							tab_level++;
						
						int new_child_idx = node_stack.peek().getSecond()+1;
						node_stack.peek().setSecond(new_child_idx);
						node_stack.push(new Pair<Task, Integer>(
								node_stack.peek().getFirst().getChildAt(new_child_idx), -1));
						node = node_stack.peek().getFirst();
						s+= produceTabs(tab_level)+"["+count+" - "+node+"\n";
						count++;
					}
					else
					{
						node_stack.pop();
						
						s+= produceTabs(tab_level)+"]\n";
						tab_level--;
					}
				}
			}
			
			s+= produceTabs(tab_level)+"]\n";
		return s;
	}

	public void outputToFile(String filename) {
		TaskTreeLatexCodeGenerator gen = new TaskTreeLatexCodeGenerator(this);
		gen.generateCode(filename);
	}
}
