package thesis.code_generators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphEdge;
import thesis.joingraph.JoinGraphEdgeIterator;
import thesis.joingraph.JoinGraphException;

public class JoinGraphVizCodeGenerator implements TreeCodeGenerator{

	JoinGraph _graph;
	
	public JoinGraphVizCodeGenerator(JoinGraph graph)
	{
		_graph = graph;
	}
	
	public void generateCode(String filename)
	{	
		 try {
			PrintWriter out
			   = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
			out.println("digraph T {");
			
			//Vertices
			Iterator<String> iter = _graph.vertexIterator();
			//String[] relations = new String[_graph.getNumberOfVertices()];
			Map<String, Integer> relations = new HashMap<String, Integer>();
			
			int count = 1;
			while(iter.hasNext())
			{
				String relation = iter.next();
				out.println("\t"+count+" [shape=circle, label=\""+relation+"\"];");
				relations.put(relation, count);
				count++;
			}
				
			Map<JoinGraphEdge, JoinGraphEdge> 
							edgesAlreadyEncountered 
								= new HashMap<JoinGraphEdge,JoinGraphEdge>();
			//Edges
			if(relations.size()==1)
			{
				out.print("}");
				out.close();
				return;
			}
			
			JoinGraphEdgeIterator edgeIter;
			try {
				edgeIter = new JoinGraphEdgeIterator(_graph);
				while(edgeIter.hasNext())
				{
					JoinGraphEdge edge = edgeIter.next();
				
					if(!edgesAlreadyEncountered.containsKey(edge)){
						out.println("\t"+relations.get(edge._vertex1)+" -> "+
							relations.get(edge._vertex2)
							+" [color=black, arrowhead=none, ordering=out, " +
									"label=\""+edge._joinInfo+"\"];");
						
						edgesAlreadyEncountered.put(edge, edge);
					}
				}

				out.print("}");
				out.close();
			} catch (JoinGraphException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
