package thesis.joingraph;

import java.util.Iterator;
import java.util.Map.Entry;



public class JoinGraphEdgeIterator {
	protected JoinGraph _graph;
	protected String _currentVertex;
	protected Entry<String, JoinInfo> _currentEdge;
	protected Iterator<Entry<String, JoinInfo>> _edges;
	protected Iterator<String> _vertices;
	protected boolean _first;
	
	public JoinGraphEdgeIterator(JoinGraph g) throws JoinGraphException{
		_graph = g;
		
		if(_graph._graph.keySet().isEmpty())
			throw new JoinGraphException("Cannot create iterator on empty graph!");
		
		_vertices = _graph._graph.keySet().iterator(); 
		_currentVertex = _vertices.next();
		
		while(_graph._graph.get(_currentVertex).isEmpty() && _vertices.hasNext())
		{
			_currentVertex = _vertices.next();
		}
		
		if(_graph._graph.get(_currentVertex).isEmpty())
			throw new JoinGraphException("Cannot create iterator on graph with no edges!");
		
		_edges = _graph._graph.get(_currentVertex).entrySet().iterator();
		_currentEdge = _edges.next();
		_first=true;
	}
	
	public boolean hasNext(){
		if (_first)
			return true;
		else
		{
			while(!_edges.hasNext() && _vertices.hasNext()){
				_currentVertex=_vertices.next();
				_edges = _graph._graph.get(_currentVertex).entrySet().iterator(); 
			}
			
			//There are no more edges
			if(!_vertices.hasNext() && !_edges.hasNext())
				return false;
			else return true;
		}
	}
	
	public JoinGraphEdge next() throws JoinGraphException
	{
		if(_first){
			_first=false;
			return new JoinGraphEdge(_currentVertex, _currentEdge.getKey(), _currentEdge.getValue());
		}
		else
		{
			while(!_edges.hasNext() && _vertices.hasNext()){
				_currentVertex=_vertices.next();
				_edges = _graph._graph.get(_currentVertex).entrySet().iterator(); 
			}
			
			//There are no more edges
			if(!_vertices.hasNext() && !_edges.hasNext())
				throw new JoinGraphException("There are no more edges!");
			
			_currentEdge=_edges.next();
			return new JoinGraphEdge(_currentVertex, _currentEdge.getKey(), _currentEdge.getValue());
		}
	}
}