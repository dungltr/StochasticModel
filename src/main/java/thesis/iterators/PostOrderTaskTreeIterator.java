package thesis.iterators;

import java.util.NoSuchElementException;
import java.util.Stack;
import thesis.task_tree.TaskTree;
import thesis.task_tree.Task;

public class PostOrderTaskTreeIterator {
	Stack<Task> _nodes;
	
	public PostOrderTaskTreeIterator(TaskTree tree)
	{
		_nodes = new Stack<Task>();
		addToStack(tree.getRoot());
	}
	
	public void addToStack(Task node)
	{
		_nodes.push(node);
		for(int i = node.getNumberOfChildren()-1; i>=0; i--)
		{
			addToStack(node.getChildAt(i));
		}
	}
	
	public boolean hasNext()
	{
		return !_nodes.empty();
	}
	
	public Task next() throws NoSuchElementException
	{
		if(!hasNext()) throw new NoSuchElementException("Tried to get the next value of an empty iterator!");
		else{
			return _nodes.pop();
		}
	}
}
