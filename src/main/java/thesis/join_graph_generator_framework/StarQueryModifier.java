package thesis.join_graph_generator_framework;

import java.util.Set;
import java.util.TreeSet;
import thesis.joingraph.JoinGraph;

public class StarQueryModifier extends QueryModifier{


	public StarQueryModifier(JoinGraph joinGraph) {
		super(joinGraph);
	}
	
	public void performModification(Set<String> relations) throws Exception{
		String entryVertex = getRandomEntryVertex();
		
		//Add new vertices to join graph
		for(String s : relations)
			_joinGraph.addVertex(s);
		
		String center = relations.iterator().next();
		//Add edges to create star
		for(String r : relations)
		{
			if(!r.equals(center))
				addEdge(r, center, true);
		}
		
		//Connect up the old join graph with new star
		if(entryVertex!=null)
			addEdge(entryVertex, center, true);
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
		
		QueryModifier modifier = new StarQueryModifier(joinGraph);
		modifier.modify(relations);
		
		joinGraph.outputToFile("Modified.dot");
	}
}
