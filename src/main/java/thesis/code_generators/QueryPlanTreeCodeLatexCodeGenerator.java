package thesis.code_generators;

import thesis.iterators.QueryPlanTreeLeafIterator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Stack;

import thesis.query_plan_tree.JoinNode;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.CostPlanVisitor;
import thesis.query_plan_tree.ReceiveNode;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.SendNode;
import thesis.task_tree.DelimiterNode;
import thesis.task_tree.TreeVisitor;
import thesis.utilities.Pair;

public class QueryPlanTreeCodeLatexCodeGenerator 
implements TreeVisitor, TreeCodeGenerator{

	protected Plan _tree;
	protected int tab_level;
	protected PrintWriter out;
	
	public QueryPlanTreeCodeLatexCodeGenerator(Plan tree)
	{
		_tree = tree;
	}
	
	public void generateCode(String filename) {
		boolean ConsiderHome = false;
		
		try {
			tab_level=0;
			
			out = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
			out.println("\\documentclass[landscape]{article}");
			if(ConsiderHome)
				out.println("\\usepackage{/home/rob/Desktop/assembla_svn/packages/qtree}");
			else
				out.println("\\usepackage{"+System.getProperty("user.home")+"/Desktop/assembla_svn/packages/qtree}");
			
			int numberOfRelations = 0;
			
			QueryPlanTreeLeafIterator iter = new QueryPlanTreeLeafIterator(_tree);
			while(iter.hasNext()){
				iter.next();
				numberOfRelations++;
			}
			int width = (int)Math.ceil(numberOfRelations*3.75);
			int height = (int)Math.ceil(numberOfRelations*3.75);
			out.println("\\usepackage[margin=0.5in, paperwidth="+width+"in, paperheight="+height+"in]{geometry}");
			out.println("\\setlength{\\oddsidemargin}{-1in}");
			out.println("\\setlength{\\evensidemargin}{-1in}");
			out.println();
			out.println("\\begin{document}");
			out.println("\\Tree ");
			
			Stack<Pair<PlanNode, Integer>> node_stack = 
				new Stack<Pair<PlanNode, Integer>>();
			
			if(_tree.getRoot().getClass().equals(RelationNode.class))
			{
				RelationNode relation = (RelationNode)_tree.getRoot();
				out.println(relation.getName());
			}
			else
			{
				node_stack.push(new Pair<PlanNode, Integer>(_tree.getRoot(), -1));
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
							if(node_stack.peek().getSecond()==-1)
								tab_level++;
							
							int new_child_idx = node_stack.peek().getSecond()+1;
							node_stack.peek().setSecond(new_child_idx);
							node_stack.push(new Pair<PlanNode, Integer>(
									node_stack.peek().getFirst().getChildAt(new_child_idx), -1));
							node_stack.peek().getFirst().accept(this);
						}
						else
						{
							node_stack.pop();
							tab_level--;
							out.println(produceTabs()+"]");
						}
					}
				}
			}
			
			out.print("\\end{document}");
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
		}
		finally{
			out.close();
		}
		
	}
	
	protected String produceTabs()
	{
		return "";
		/*if(tab_level<0)
			throw new IndexOutOfBoundsException("Tabbing level must be greater than or equal to 1");
		
		String tabs = "";
		for(int i = 0; i<tab_level; i++)
		{
			tabs+="\t";
		}
		return tabs;*/
	}
	
	public void visit(JoinNode node) {
		String line = produceTabs()+"[.$\\bowtie_{"+node.getJoinCondition()+"}^{"+node.getSite()+"}"+
				"("+
			(int)Math.floor(node.getOutputCardinality()+0.5)
			+")$ ";
		out.println(line);
	}


	public void visit(ReceiveNode node) {
		String line = produceTabs()+"[.$RCV^{"+node.getSite()+"}"+
		"("+
		(int)Math.floor(node.getOutputCardinality()+0.5)
		+")$ ";
		out.println(line);
	}

	public void visit(SendNode node) {
		String line = produceTabs()+"[.$SEND^{"+node.getSite()+"}"+
		"("+
		(int)Math.floor(node.getOutputCardinality()+0.5)
		+")$ ";
		out.println(line);
	}

	public void visit(RelationNode node) {
		String line = produceTabs()+node.getName()+"^{"+node.getSite()+"}"+
		"("+
		(int)Math.floor(node.getOutputCardinality()+0.5)
		+") ";
		out.println(line);
	}

	public void visit(DelimiterNode node) {
		String line = produceTabs()+"[.$\\bot$";
		out.println(line);
		
	}
	
	public String roundDouble(double d){
		DecimalFormat df = new DecimalFormat("#.###");
		return df.format(d);
	}
}
