package thesis.join_graph_generator_framework;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import thesis.joingraph.JoinGraph;

public class ChainQueryModifier extends QueryModifier{

	public ChainQueryModifier(JoinGraph joinGraph) {
		super(joinGraph);
	}

	public void performModification(Set<String> relations)
	throws Exception 
	{
		String entryVertex = getRandomEntryVertex();

		//Add new vertices to join graph
		for(String s : relations)
			_joinGraph.addVertex(s);

		Iterator<String> iter = relations.iterator();
		String previous=iter.next();

		//Add edges to create chain
		while(iter.hasNext())
		{
			String next = iter.next();
			addEdge(previous, next, true);
			previous = next;
		}

		//Connect up the old join graph with new chain
		if(entryVertex!=null)
			addEdge(entryVertex, relations.iterator().next(), true);
	}

	public static void main(String args[]) throws Exception
	{
		Set<String> relations = new TreeSet<String>();
		relations.add("A");
		relations.add("B");
		relations.add("C");
		relations.add("D");
		relations.add("E");

		JoinGraph joinGraph = new JoinGraph("DPccp_test1.txt");
		joinGraph.outputToFile("BeforeModification.dot");

		QueryModifier modifier = new ChainQueryModifier(joinGraph);
		modifier.modify(relations);

		joinGraph.outputToFile("Modified.dot");
	}
}
