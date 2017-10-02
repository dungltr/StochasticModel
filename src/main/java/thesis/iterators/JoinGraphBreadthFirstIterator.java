package thesis.iterators;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;

public class JoinGraphBreadthFirstIterator {
	
	JoinGraph _graph;
	Queue<String> _nodesToBeExplorer;
	Queue<String> _visited;
	
	public JoinGraphBreadthFirstIterator(JoinGraph g) throws JoinGraphException
	{
		_graph = g;
		_nodesToBeExplorer = new LinkedList<String>();
		_visited = new LinkedList<String>();
		//Add any old vertex, the graph is connected so shouldnt matter.
		
		String node = g.vertexIterator().next();
		_nodesToBeExplorer.add(node);
		
		while(!_nodesToBeExplorer.isEmpty())
		{
			String s = _nodesToBeExplorer.poll();
			if(!_visited.contains(s))
				_visited.add(s);
			for(String n : _graph.getNeighbours(s)){
				if(!_visited.contains(n))
					_nodesToBeExplorer.add(n);
			}
			
		}
	}
	
	public boolean hasNext()
	{
		return !_visited.isEmpty();
	}
	
	public String next() throws NoSuchElementException
	{
		if(!hasNext()) throw new NoSuchElementException("Tried to get the next value of an empty iterator!");
		else{
			return _visited.poll();
		}
	}
}
