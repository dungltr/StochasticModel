package thesis.code_generators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Stack;

import thesis.task_tree.TaskTree;
import thesis.task_tree.Task;
import thesis.utilities.Pair;

public class TaskTreeLatexCodeGenerator {
	protected TaskTree _tree;
	protected int tab_level;
	protected PrintWriter out;
	
	public TaskTreeLatexCodeGenerator(TaskTree tree)
	{
		_tree = tree;
	}
	
	public void generateCode(String filename) {
		
		try {
			tab_level=0;
			
			out = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
			out.println("\\documentclass[landscape]{article}");
			out.println("\\usepackage{"+System.getProperty("user.home")+"/Desktop/assembla_svn/packages/qtree}");
			int width = 30;//(int)Math.ceil(Configuration._queryRelations.size()*2);
			int height = 10;//(int)Math.ceil(Configuration._queryRelations.size()*1.5);
			out.println("\\usepackage[margin=0.5in, paperwidth="+width+"in, paperheight="+height+"in]{geometry}");
			out.println("\\setlength{\\oddsidemargin}{-1in}");
			out.println("\\setlength{\\evensidemargin}{-1in}");
			out.println();
			out.println("\\begin{document}");
			out.println("\\Tree ");
			
			Stack<Pair<Task, Integer>> node_stack = 
				new Stack<Pair<Task, Integer>>();
			
				node_stack.push(new Pair<Task, Integer>(_tree.getRoot(), -1));
				Task node = node_stack.peek().getFirst();
				String line = produceTabs()+"[.$\\bigcirc_{"+node.getID()+"}^{"+node.getSite()+"}("
								+node.getDuration()+")$";
				out.println(line);
				
				while(node_stack.size()>0)
				{
					if(node_stack.peek().getFirst().getNumberOfChildren()==0){
						node_stack.pop();
						out.println(produceTabs()+"]");
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
							line = produceTabs()+"[.$\\bigcirc_{"+node.getID()+"}^{"+node.getSite()
									+"}("+node.getDuration()+")$";
							out.println(line);
						}
						else
						{
							node_stack.pop();
							tab_level--;
							out.println(produceTabs()+"]");
						}
					}
				}
			
			out.print("\\end{document}");
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			out.close();
		}
		
	}
	
	protected String produceTabs()
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
}
