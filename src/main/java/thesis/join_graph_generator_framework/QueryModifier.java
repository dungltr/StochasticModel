package thesis.join_graph_generator_framework;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import thesis.joingraph.JoinCondition;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;

public abstract class QueryModifier {

	protected JoinGraph _joinGraph;
	
	public QueryModifier(JoinGraph joinGraph)
	{
		_joinGraph = joinGraph;
	}
	
	public void modify(Set<String> relations) throws Exception {
		preprocess(this, relations);
	}
	
	public abstract void performModification(Set<String> relations) 
		throws JoinGraphException, Exception;

	/**
	 * We need to treat the cases where the size of the graph to
	 * be added is less than 3.
	 * @param m
	 * @param relations
	 * @throws Exception 
	 */
	protected void preprocess(QueryModifier m, Set<String> relations) 
		throws Exception
	{	
		if(relations.size()==0)
			throw new IndexOutOfBoundsException("Cannot modify graph by adding no relations.");
		else if(relations.size()<3)
		{
			String entryVertex = getRandomEntryVertex();
			
			//Add new vertices to join graph
			for(String s : relations)
				_joinGraph.addVertex(s);
			
			String first = relations.iterator().next();
			
			//Add edges if there are any i.e. if relations.size()==2
			for(String r : relations)
			{
				if(!r.equals(first))
					addEdge(r, first, true);
			}
			
			//Connect up the old join graph with new graph
			if(entryVertex!=null){
				addEdge(first, entryVertex, true);
			}
		}
		else{
			m.performModification(relations);
		}
	}
	
	protected void addEdge(String relation1, String relation2, boolean b) throws Exception
	{
		_joinGraph.addEdge(relation1, relation2, 
				new JoinCondition(),
			 1, b);
		/*
		double d = new Random().nextDouble();
		double selectivity = 0.0;
		if(d<0.1){
			Field field1 = getRandomField(relation1);
			Field field2 = getRandomField(relation2);
			selectivity= 1.0/Math.max(Domain.getDomainCardinality(field1.getType()), 
				Domain.getDomainCardinality(field2.getType()));
		}
		else{
			//Getting key-foreign key selectivity
			Field field2 = getRandomField(relation2);
			selectivity= 1.0/(double)Catalog.getCardinality(relation1);
			_joinGraph.addEdge(relation1, relation2, 
					new JoinCondition(Catalog.getKey(relation1), field2.getName()),
				 selectivity, b);
		}*/
	}
	
	
	/**
	 * Randomly choses a current vertex in the graph.
	 * @return
	 */
	protected String getRandomEntryVertex() {
		if(_joinGraph.getNumberOfVertices()==0)
			return null;
		
		int r = new Random().nextInt(_joinGraph.getNumberOfVertices());
		int count = 0;
		
		for(Iterator<String> iter = _joinGraph.vertexIterator(); iter.hasNext();)
		{
			String next = iter.next();
			
			if(count==r){
				return next;
			}
			else {
				count++;
			}
		}
		
		throw new IndexOutOfBoundsException("Could not find random vertex to start from.");
	}
}
