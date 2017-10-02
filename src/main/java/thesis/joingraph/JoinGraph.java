package thesis.joingraph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;

import thesis.code_generators.JoinGraphVizCodeGenerator;

import thesis.utilities.Pair;

public class JoinGraph implements Cloneable, Serializable{
	private static final long serialVersionUID = -6609031883455792078L;
	protected Map<String, Map<String, JoinInfo>> _graph;
	protected int _numberOfVertices;
	protected int _numberOfEdges;
	
	public JoinGraph() throws RemoteException
	{
		_graph = new HashMap<String, Map<String, JoinInfo>>();
		_numberOfEdges = 0;
		_numberOfVertices = 0;
	}
	
	public JoinGraph(String filename) 
	throws IOException, NumberFormatException, JoinGraphException, ParseException, CatalogException {
		this();
		//File format
		//line 1: name1 name2 name3 name4.....
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String[] parts = br.readLine().split(" ");
		for(int i = 0; i<parts.length; i++)
		{
			addVertex(parts[i]);
		}

		String line = "";
		
		while((line=br.readLine())!=null)
		{
			//Line format: This_Node n1 (n2 condition sel)+
			parts = line.split(" ");
			for(int i = 1; i<parts.length; i=i+3){
				if(!parts[i+1].contains("="))
					throw new ParseException("condition is not equijoin", 0);
				
				String[] condition = parts[i+1].split("=");
				addEdge(parts[0], parts[i], 
						new JoinCondition(condition[0], condition[1]),//.parse(parts, 2, parts.length-1),
						Double.parseDouble(parts[i+2]),
								//parts[i], condition[1],//Double.parseDouble(parts[parts.length-1]), 
						false);
			}
		}
		
		br.close();
	}

	public int getNumberOfEdges()
	{
		return _numberOfEdges;
	}
	
	/**
	 * Add vertex named s into the graph.
	 * @param s
	 * @return
	 */
	public boolean addVertex(String s)
	{
		if(_graph.containsKey(s)){
			return false;
		}
		else
		{
			_graph.put(s, new HashMap<String, JoinInfo>());
			_numberOfVertices++;
			return true;
		}
	}
	
	public int getDegree(String s) throws JoinGraphException{
		if(_graph.containsKey(s))
		{
			return _graph.get(s).size();
		}
		else 
			throw new JoinGraphException("Graph does not contain "+s);
	}
	public void setEdgeInfo(String v1, String v2, JoinInfo info)
	{
		_graph.get(v1).remove(v2);
		_graph.get(v1).put(v2, info);
		JoinInfo infoclone = (JoinInfo)info.clone();
		infoclone._joinCondition=JoinCondition.flip(infoclone._joinCondition);
		_graph.get(v2).remove(v1);
		_graph.get(v2).put(v1, infoclone);
	}
	/**
	 * Adds an edge between v1 and v2 with the selectivity sel.
	 * @param v1
	 * @param v2
	 * @param sel
	 * @param b 
	 * @return
	 * @throws JoinGraphException 
	 */
	public void addEdge(String v1, String v2, JoinCondition jc, double sel, 
			boolean addReverseEdge) throws JoinGraphException
	{
		if(!_graph.containsKey(v1) || !_graph.containsKey(v2))
			throw new JoinGraphException("Trying to add an edge where one vertex does not exist!");
		else{
			
			Map<String, JoinInfo> edges = _graph.get(v1);
			if(edges.containsKey(v2))
			{
				//Edge already exists so multiply the selectivities.
				JoinInfo info = edges.remove(v2);
				info.conjunct(new JoinInfo(sel, jc));
			}
			else
			{
				//Edge does not exist
				edges.put(v2, new JoinInfo(sel, jc));
				//edges.put(v2, sel);
			}
			
			if(addReverseEdge)
			{
				edges = _graph.get(v2);
				if(edges.containsKey(v1))
				{
					//Edge already exists so multiply the selectivities.
					JoinInfo info = edges.remove(v1);
					info.conjunct(new JoinInfo(sel, JoinCondition.flip(jc)));
				}
				else
				{
					edges.put(v1, new JoinInfo(sel, JoinCondition.flip(jc)));
				}
			}

			_numberOfEdges++;
		}
	}
	
	/**
	 * This overloaded method allows you to leave out the boolean value thereby making 
	 * the default behaviour to added reverse edge along with requested edge.
	 * @param v1
	 * @param v2
	 * @param sel
	 * @throws JoinGraphException
	 */
	public void addEdge(String v1, String v2, JoinCondition jc, double sel) throws JoinGraphException
	{
		addEdge(v1, v2, jc, sel, true);
	}
	
	/**
	 * Return the number of vertices present in this graph.
	 * @return
	 */
	public int getNumberOfVertices()
	{
		return _numberOfVertices;
	}
	
	/**
	 * Receives an input of a list of relations that should be merged into
	 * a stub relation.  This join graph should be changed to reflect this change.
	 * The join graph containing the absorbed portion is returned.
	 * @param relationIDs
	 * @param  
	 * @return
	 * @throws JoinGraphException 
	 * @throws RemoteException 
	 */
	public JoinGraph absorb(TreeSet<String> relationIDs, String stubIdentifier) 
	throws JoinGraphException, RemoteException
	{
		if(!_graph.keySet().containsAll(relationIDs))
			throw new JoinGraphException("Trying to absorb nodes that are not present!");
		else if(relationIDs.isEmpty())
			throw new JoinGraphException("If you are absorbing you should provide a non empty list " +
					"of nodes to absorb!");
	
		//Start of removing subgraph containing vertices in relationIDs and
		//edges connecting only vertices in relationIDs.
		JoinGraph subgraph = new JoinGraph();
		
		for(String s : relationIDs){
			subgraph.addVertex(s);
		}
		
		for(String s : relationIDs){
			
			for(String p : getNeighbours(s))
			{
				if(relationIDs.contains(p))
				{
					JoinInfo joinInfo = getJoinInfo(s, p);
					subgraph.addEdge(s, p, joinInfo._joinCondition, 
							joinInfo._selectivity, false);
				}
			}
		}
		
		//End of creating subgraph being removed.
		
		//Add an entry in _graph for the new vertex named stubIdentifier that will
		//be replacing the vertices in relationIDs.  If two edges join a neighbour
		//of a relationID to the new stub vertex then we need to multiply selectivities. 
		Set<String> neighbours = getNeighbours(relationIDs);
		Map<String, JoinInfo> edges = new HashMap<String, JoinInfo>();
		
		for(String s : neighbours){
			JoinInfo info = getJoinInfo(relationIDs, s);
			edges.put(s, info);
		}
		_graph.put(stubIdentifier, edges);
		
		//Add stub entry into each neighbour entry.  
		//Initially it is added with selectivity 1.0.
		for(String n : neighbours)
		{
			edges = _graph.get(n);
			if(edges.containsKey(stubIdentifier))
			{
				JoinInfo joinInfo = edges.remove(stubIdentifier);
				joinInfo.conjunct(getJoinInfo(stubIdentifier, n));
				edges.put(stubIdentifier, joinInfo);
			}
			else
			{
				edges.put(stubIdentifier, new JoinInfo(1.0));
			}
			
			//Go through relationIDs and multiply selectivity of stub entry
			//if the edges for the neighbour contains the relationID.
			//Remember to remove the relationID entry from the neighbour edges.
			for(String s : relationIDs)
			{
				if(edges.containsKey(s))
				{
					JoinInfo j = edges.remove(s);
					JoinInfo stubInfo = edges.remove(stubIdentifier);
					stubInfo.conjunct(j);
					edges.put(stubIdentifier, stubInfo);
				}
			}
		}
		
		//Remove the entries for the relationIDs.
		for(String s : relationIDs){
			_graph.remove(s);
			_numberOfVertices--;
		}
		
		//We have added a stub vertex.
		_numberOfVertices++;
		
		return subgraph;
	}

	public JoinInfo getJoinInfo(Set<String> r1, Set<String> r2) throws JoinGraphException
	{
		//Check that the two sets intersection is the empty set.
		for(String r : r1)
		{
			if(r2.contains(r))
				throw new JoinGraphException("Trying to get selectivity of two overlapping " +
						"sets of relations.  They must be disjoint!");
		}
		
		for(String r : r2)
		{
			if(r1.contains(r))
				throw new JoinGraphException("Trying to get selectivity of two overlapping " +
						"sets of relations.  They must be disjoint!");
		}
		
		
		//double selectivity = 1.0;
		JoinInfo joinInfo = new JoinInfo(1.0);
		
		for(String s : r2)
			joinInfo.conjunct(getJoinInfo(r1, s));
		
		return joinInfo;
		
	}
	private JoinInfo getJoinInfo(Set<String> relationIDs, String s) throws JoinGraphException {
		JoinInfo joinInfo = new JoinInfo(1.0);
		
		for(String relationID : relationIDs){
			JoinInfo tempInfo = getJoinInfo(relationID, s);
			joinInfo.conjunct(tempInfo);
			//selectivity*=getSelectivity(relationID, s);
		}
		return joinInfo;
	}

	public JoinInfo getJoinInfo(String v1, String v2) throws JoinGraphException
	{
		if(!_graph.containsKey(v1) || !_graph.containsKey(v2))
			throw new JoinGraphException("Trying to get selectivity between " +
					"two relations that do not exist: "+v1+" "+v2+" in join graph\n"+this);
		else
		{
			Map<String, JoinInfo> edges = _graph.get(v1);
			if(edges.containsKey(v2))
				return edges.get(v2);
			else 
				return new JoinInfo(1.0);
		}
	}
	/**
	 * Return the pair of relations who have a join condition with the minimum
	 * selectivity.
	 * @return
	 * @throws JoinGraphException 
	 */
	public Pair<String, String> getMiniumSelectivityEdge() throws JoinGraphException {
		double minSelectivity = Double.MAX_VALUE;
		String v1 = null;
		String v2 = null;
		
		JoinGraphEdgeIterator iter = new JoinGraphEdgeIterator(this);
		
		while(iter.hasNext())
		{
			JoinGraphEdge edge = iter.next();
			if(edge._joinInfo.getSelectivity()<minSelectivity)
			{
				minSelectivity = edge._joinInfo.getSelectivity();
				v1 = edge._vertex1;
				v2 = edge._vertex2;
			}
		}
		
		if(v1==null && v2==null)
			throw new JoinGraphException("Could not find edge with minimum selectivity!");
		else
			return new Pair<String, String>(v1, v2);
	}
	
	public Pair<String, String> getMinimumIntermediateSizeEdge() throws JoinGraphException, CatalogException{
		double minSize = Double.MAX_VALUE;
		String v1 = null;
		String v2 = null;
		
		JoinGraphEdgeIterator iter = new JoinGraphEdgeIterator(this);
		
		while(iter.hasNext())
		{
			JoinGraphEdge edge = iter.next();
			double tempSize = edge._joinInfo.getSelectivity()*Catalog.getCardinality(edge._vertex1)*
			Catalog.getCardinality(edge._vertex2);
			
			if(tempSize<minSize)
			{
				minSize = tempSize;
				v1 = edge._vertex1;
				v2 = edge._vertex2;
			}
		}
		
		if(v1==null && v2==null)
			throw new JoinGraphException("Could not find edge with minimum selectivity!");
		else
			return new Pair<String, String>(v1, v2);
	}
	/**
	 * Return the neighbours of the set of relation identifiers.
	 * The neighbours names are returns in a set.
	 * @param relationIDs
	 * @return
	 * @throws JoinGraphException 
	 */
	public Set<String> getNeighbours(TreeSet<String> relationIDs) throws JoinGraphException
	{
		Set<String> neighbours = new TreeSet<String>();
		
		for(String n : relationIDs)
		{
			Set<String> v_neighbours = getNeighbours(n);
			
			for(String p : v_neighbours)
			{
				if(!relationIDs.contains(p))
				{
					neighbours.add(p);
				}
			}
		}
		
		return neighbours;
	}
	
	/**
	 * Return the neighbours of a relation.
	 * @param s
	 * @return
	 * @throws JoinGraphException 
	 */
	public Set<String> getNeighbours(String s) throws JoinGraphException
	{
		if(!_graph.containsKey(s))
			throw new JoinGraphException("Tried to get neighbours of a node that does not exist! "+s);
		else{
			TreeSet<String> set = new TreeSet<String>();
			for(String v : _graph.get(s).keySet())
				set.add(v);
			return set;
		}
	}
	
	/*public Object clone()
	{
		JoinGraph clone;
		try {
			clone = new JoinGraph();
			clone._graph.putAll(_graph);
			clone._numberOfEdges = _numberOfEdges;
			clone._numberOfVertices = _numberOfVertices;
			return clone;
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(0);
		}
	
	}*/
	
	public Iterator<String> vertexIterator()
	{
		return _graph.keySet().iterator();
	}
	
	public String toString()
	{
		/*String s = "";
		for(Entry<String, >> e : _graph.entrySet())
		{
			s+=e.getKey()+" : "+e.getValue()+"\n";
		}
		return s;*/
		return _graph.toString();
	}
	
	/**
	 * Output as a graph file.
	 * @param filename
	 */
	public void outputToFile(String filename)
	{
		JoinGraphVizCodeGenerator gen = new JoinGraphVizCodeGenerator(this);
		gen.generateCode(filename);
	}
	
	/**
	 * Output as a text file.
	 * @param filename
	 * @throws FileNotFoundException 
	 */
	public void outputToTextFile(String filename) throws FileNotFoundException
	{
		PrintWriter p = new PrintWriter(filename);
		
		for(Iterator<String> iter = _graph.keySet().iterator(); iter.hasNext();)
		{
			p.print(iter.next());
			if(iter.hasNext())
				p.print(" ");
		}
		p.println();
		
		for(Iterator<Entry<String, Map<String, JoinInfo>>> iter = _graph.entrySet().iterator(); 
				iter.hasNext();)
		{
			Entry<String, Map<String, JoinInfo>> entry = iter.next();
			p.print(entry.getKey()+" ");
			
			for(Iterator<Entry<String, JoinInfo>> inner_iter = entry.getValue().entrySet().iterator();
				inner_iter.hasNext();)
			{
				Entry<String, JoinInfo> inner_entry = inner_iter.next();
				p.print(inner_entry.getKey()+" "+inner_entry.getValue()._joinCondition+
						" "+inner_entry.getValue()._selectivity);
				
				if(inner_iter.hasNext())
					p.print(" ");
			}
			
			if(iter.hasNext())
				p.println();
		}
		
		p.close();
	}
	
	public boolean equals(Object that){
		if(this == that) return true;
		else if (getClass().equals(that.getClass())){
			JoinGraph graph = (JoinGraph)that;
			if(graph._graph.equals(_graph) && graph._numberOfEdges==_numberOfEdges
					&& graph._numberOfVertices==_numberOfVertices)
				return true;
			else return false;
		}
		else return false;
	}
	
	public int hashCode(){
		return _graph.hashCode();
	}
	
	public static void main(String args[]) throws NumberFormatException, IOException, 
	JoinGraphException, ParseException, CatalogException
	{
		JoinGraph g = new JoinGraph(System.getenv().get("HOME") + "/assembla_svn/Experiments/CompareML-3Sites/Distributed/20-Queries/Chain/Query2");
		System.out.println(g);
		g.outputToFile("/ecslab/c09rt/workspace/CentralizedAlgorithm/multilevel_graphs/Query2.dot");
	}
}
