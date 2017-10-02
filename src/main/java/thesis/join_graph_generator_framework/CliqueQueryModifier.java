package thesis.join_graph_generator_framework;

import java.util.Set;
import java.util.TreeSet;
import thesis.joingraph.JoinGraph;

public class CliqueQueryModifier extends QueryModifier{
	
	public CliqueQueryModifier(JoinGraph joinGraph)
	{
		super(joinGraph);
	}
	
	public void performModification(Set<String> relations) throws Exception {
		String entryVertex = getRandomEntryVertex();
		
		//Add new vertices to join graph
		for(String s : relations)
			_joinGraph.addVertex(s);
		
		//Add edges to create clique
		for(String r : relations)
		{
			for(String s : relations)
			{
				if(!r.equals(s))
					addEdge(r, s, false);
			}
		}
		
		//Connect up the old join graph with new clique
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
		
		CliqueQueryModifier modifier = new CliqueQueryModifier(joinGraph);
		modifier.modify(relations);
		
		joinGraph.outputToFile("Modified.dot");
	}
}
