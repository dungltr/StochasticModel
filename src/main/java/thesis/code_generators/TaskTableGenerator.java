package thesis.code_generators;

import thesis.iterators.DFTaskTreeIterator;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Stack;
import thesis.query_plan_tree.JoinNode;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.CostPlanVisitor;
import thesis.query_plan_tree.ReceiveNode;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.SendNode;
import thesis.task_tree.DelimiterNode;
import thesis.task_tree.TaskTree;
import thesis.task_tree.Task;
import thesis.task_tree.TreeVisitor;
import thesis.utilities.Pair;

public class TaskTableGenerator 
implements TreeVisitor, TreeCodeGenerator{

	TaskTree _tree;
	int tab_level=0;
	PrintWriter out;
	
	public TaskTableGenerator(TaskTree tree)
	{
		_tree = tree;
	}
	
	public void generateCode(String filename)
	{
		try {
			out = new PrintWriter(filename);
			out.println("\\documentclass[landscape]{article}");
			out.println("\\usepackage{"+System.getProperty("user.home")+"/Desktop/assembla_svn/packages/qtree}");
			int width = 50;//(int)Math.ceil(Configuration._queryRelations.size()*2);
			int height = 30;//(int)Math.ceil(Configuration._queryRelations.size()*1.5);
			out.println("\\usepackage[margin=0.5in, paperwidth="+width+"in, paperheight="+height+"in]{geometry}");
			out.println("\\setlength{\\oddsidemargin}{-1in}");
			out.println("\\setlength{\\evensidemargin}{-1in}");
			out.println();
			out.println("\\begin{document}");
			out.println("\\begin{table}[ht]");
			out.println("\\centering");
			out.println("\\begin{tabular}{| c | c | c |}");
			out.println("\\hline\\hline");
			out.println("Task Number & Task & Duration\\\\ [0.5ex]");
			out.println("\\hline ");
			
			DFTaskTreeIterator iter = new DFTaskTreeIterator(_tree);
			while(iter.hasNext())
			{
				Task node = iter.next();
			
				out.println(node.getID()+" &");
				out.println("\\Tree ");
				
				Stack<Pair<PlanNode, Integer>> node_stack = 
					new Stack<Pair<PlanNode, Integer>>();
				
				if(node.getTree().getRoot().getClass().equals(RelationNode.class))
				{
					RelationNode relation = (RelationNode)node.getTree().getRoot();
					out.println("[."+relation.getName()+" ]");
				}
				else
				{
					node_stack.push(new Pair<PlanNode, Integer>(node.getTree().getRoot(), -1));
					node_stack.peek().getFirst().accept(this);
					
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
								
									if(node_stack.peek().getSecond()==-1){
										tab_level++;
									}
									
									int new_child_idx = node_stack.peek().getSecond()+1;
									node_stack.peek().setSecond(new_child_idx);
									PlanNode child = node_stack.peek().getFirst().getChildAt(new_child_idx);
									if(!child.getClass().equals(DelimiterNode.class)){
										node_stack.push(new Pair<PlanNode, Integer>(child, -1));
										node_stack.peek().getFirst().accept(this);
									}
							}
							else
							{
								node_stack.pop();
								tab_level--;
								out.println(produceTabs(tab_level)+" ]");
							}
						}
					}
				} 
				out.println(" & "+roundDouble(node.getDuration()));
				out.println("\\\\ [1ex]"); 
				out.println("\\hline");
			}
		
			out.println("\\end{tabular}");
			out.println("\\caption{Tasks}");
			out.println("\\end{table}");
			out.println("\\end{document}");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
		}
	}
	
	protected String produceTabs(int tab_level)
	{
		if(tab_level<0)
			throw new IndexOutOfBoundsException("Tabbing level must be greater than or equal to 1");
		
		String tabs = "";
		for(int i = 0; i<tab_level; i++)
		{
			tabs+="\t";
		}
		return tabs;
	}
	
	public void visit(JoinNode node) {
		String line = produceTabs(tab_level)+"[.$\\bowtie_{"+node.getJoinCondition()+"}^{"+node.getSite()+"}$";
		out.println(line);
	}

	public void visit(ReceiveNode node) {
		String line = produceTabs(tab_level)+"[.$RCV^{"+node.getSite()+"}$ ";
		out.println(line);
	}

	public void visit(SendNode node) {
		String line = produceTabs(tab_level)+"[.$SEND^{"+node.getSite()+"}$ ";
		out.println(line);
	}

	public void visit(RelationNode node) {
		String line = produceTabs(tab_level)+"$"+node.getName()+"^{"+node.getSite()+"}$ ";
		out.println(line);
	}

	public void visit(DelimiterNode node) {
		String line = produceTabs(tab_level)+"[.$Delimiter$";
		out.println(line);
	}
	public String roundDouble(double d){
		DecimalFormat df = new DecimalFormat("#.###");
		return df.format(d);
	}
}
