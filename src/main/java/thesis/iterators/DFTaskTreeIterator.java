package thesis.iterators;

import java.util.Stack;
import thesis.task_tree.TaskTree;
import thesis.task_tree.Task;

public class DFTaskTreeIterator {
	protected TaskTree _tree;
	protected Stack<Task> _currentPath;
	
	public DFTaskTreeIterator(TaskTree tree)
	{
		_tree = tree;
		_currentPath = new Stack<Task>();
		_currentPath.add(_tree.getRoot());
	}
	
	public boolean hasNext()
	{
		return !_currentPath.isEmpty();
	}
	
	public Task next()
	{
		Task next = _currentPath.pop();
			for(int i = next.getNumberOfChildren()-1; i>=0; i--)
			{
				_currentPath.add(next.getChildAt(i));
			}
		
		return next;
	}
}
