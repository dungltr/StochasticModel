package thesis.join_graph_generator_framework;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import thesis.joingraph.JoinCondition;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinInfo;
import thesis.utilities.Pair;
import thesis.catalog.Catalog;
import thesis.catalog.Domain;
import thesis.catalog.Field;
import thesis.catalog.Schema;

/**
 * Generates random join graphs according to probabilities of
 * generating sub graphs of particular structures i.e. star,
 * cycle, chain and clique.
 * 
 * @author rob
 *
 */
public class QueryGenerator {

	private final double _starProbability;
	private final double _cliqueProbability;
	private final double _chainProbability;
	private final double _cycleProbability;
	private Set<String> _availableRelations;
	private Set<String> _usedRelations;
	private int _sizeOfQuery;
	private QueryModifier _queryModifier;
	private JoinGraph _joinGraph;
	private Random _random;
	private QueryModifier[] _modifiers;
	private String _filename;
	
	public QueryGenerator(int sizeOfQuery, String filename) throws RemoteException{
		_filename = filename;
		_starProbability = 0.25;
		_cliqueProbability = 0.25;
		_chainProbability = 0.25;
		_cycleProbability = 0.25;
		_sizeOfQuery = sizeOfQuery;
		_availableRelations = new TreeSet<String>();
		_usedRelations = new TreeSet<String>();
		_random = new Random();
		_joinGraph = new JoinGraph();
		_modifiers = new QueryModifier[4];
		_modifiers[0] = new StarQueryModifier(_joinGraph);
		_modifiers[1] = new CliqueQueryModifier(_joinGraph);
		_modifiers[2] = new ChainQueryModifier(_joinGraph);
		_modifiers[3] = new CycleQueryModifier(_joinGraph);
		
		setAvailableRelations();
	}
	
	private void setAvailableRelations() {
		int count = 0;
		for(Iterator<String> iter = Catalog.relationsIterator(); iter.hasNext();)
		{
			if(count < _sizeOfQuery){
				_availableRelations.add(iter.next());
				count++;
			}
			else break;
		}
		
		if(count < _sizeOfQuery)
			throw new IndexOutOfBoundsException("Query is larger than number of relations in Catalog!");
	}

	private void generateComplete(int i) throws Exception
	{
		_queryModifier = _modifiers[i];
		Set<String> relations = obtainSubQueryRelations(_sizeOfQuery);
		_queryModifier.modify(relations);
		assignJoinEdgeInformation();
		_joinGraph.outputToTextFile(_filename);
	}
	
	public void generateStar() throws Exception{
		generateComplete(0);
	}
	
	public void generateClique() throws Exception{
		generateComplete(1);
	}
	
	public void generateChain() throws Exception{
		generateComplete(2);
	}
	
	public void generateCycle() throws Exception{
		generateComplete(3);
	}
	
	public void generateMixed() throws Exception{
		int nextQuerySize = 0;
		
		while(!_availableRelations.isEmpty())
		{
			setQueryModifier();
			nextQuerySize = getNextQuerySize();
			Set<String> relations = obtainSubQueryRelations(nextQuerySize);
			_queryModifier.modify(relations);
		}
		
		assignJoinEdgeInformation();
		_joinGraph.outputToTextFile(_filename);
	}
	
	protected class SortEntry implements Comparable<SortEntry>{
		public int _degree;
		public double _cardinality;
		public String _name;
		
		public SortEntry(String name, int degree, double d)
		{
			_degree = degree;
			_cardinality = d;
			_name=name;
		}
		
		public int compareTo(SortEntry s) {
			if(_degree>s._degree)
				return 1;
			else if (_degree < s._degree)
				return -1;
			else if (_cardinality>s._cardinality)
				return 1;
			else if (_cardinality<s._cardinality)
				return -1;
			else
				return 0;
		}
		
		public String toString(){
			return _name+":"+_degree+":"+_cardinality;
		}
		
	}
	protected void assignJoinEdgeInformation() throws Exception
	{
		//Sort by size of out degree
		//Then sort each of these by size of relations
		//Visit each node and assign join info.
		//Be careful about reverse edges i.e. remember what you have done and skip over if you have.
		List<SortEntry> l = new ArrayList<SortEntry>(); 
		
		for(Iterator<String> iter = _joinGraph.vertexIterator(); iter.hasNext();)
		{
			String s = iter.next();
			int degree = _joinGraph.getDegree(s);
			SortEntry entry = new SortEntry(s, degree, Catalog.getCardinality(s));
			l.add(entry);
		}
		
		Collections.sort(l);
		
		Map<Pair<String,String>, Pair<String, String>> visitedEdges = 
			new HashMap<Pair<String,String>, Pair<String, String>>();
		
		for(int i = l.size()-1; i>=0; i--)
		{
			SortEntry entry = l.get(i);
			Set<String> neighbours = _joinGraph.getNeighbours(entry._name);
			for(String s : neighbours){
				Pair<String, String> p = new Pair<String, String>(entry._name, s);
				Pair<String, String> q = new Pair<String, String>(s, entry._name);
				if(!visitedEdges.containsKey(p) &&
						(!visitedEdges.containsKey(q)))
				{
					_joinGraph.setEdgeInfo(entry._name, s, 
							getRandomJoinInfo(entry._name, s));
					visitedEdges.put(p, p);
					visitedEdges.put(q, q);
				}
			}
		}
	}
	
	protected JoinInfo getRandomJoinInfo(String relation1, String relation2) throws Exception{
		double d = new Random().nextDouble();
		double selectivity = 0.0;
		if(d<0.1){
			Field field1 = getRandomNonKeyField(relation1);
			Field field2 = getRandomNonKeyField(relation2);
			selectivity= 1.0/Math.max(Domain.getDomainCardinality(field1.getType()), 
				Domain.getDomainCardinality(field2.getType()));
			return new JoinInfo(selectivity, new JoinCondition(field1.getName(), field2.getName()));
		}
		else{
			//Getting key-foreign key selectivity
			Field field2 = getRandomNonKeyField(relation2);
			selectivity= 1.0/(double)Catalog.getCardinality(relation1);
			return new JoinInfo(selectivity, new JoinCondition(Catalog.getKey(relation1),
					field2.getName()));
		}
	}
	
	protected Field getRandomNonKeyField(String s) throws Exception{
		Schema schema = Catalog.getSchema(s);

		//We don't include key field when randomly chosing field
		List<Field> fields = new LinkedList<Field>();
		for(Iterator<Field> iter = schema.fieldsIterator(); iter.hasNext();)
		{
			Field field = iter.next();
			if(!field.getName().equals(Catalog.getKey(s)))
				fields.add(field);
		}
	
		int r = 1+new Random().nextInt(fields.size());
		for(Iterator<Field> iter = fields.iterator(); iter.hasNext();)
		{
			Field f = iter.next();
			if(r==1)
				return f;
			else r--;
		}
		throw new Exception("Error trying to get random field");
	}
	
	private Set<String> obtainSubQueryRelations(int i){
		if(i>_availableRelations.size() || i<=0)
			return null;
		else{
			Set<String> relations = new TreeSet<String>();
			int count = 0;
			
			for(String s : _availableRelations)
			{
				if(count>=i)
					break;
				
				relations.add(s);
				_usedRelations.add(s);
				count++;
			}
			
			_availableRelations.removeAll(relations);
			return relations;
		}
	}
	
	private int getNextQuerySize(){
		return _random.nextInt(_availableRelations.size())+1;
	}
	
	private void setQueryModifier(){
		double d = _random.nextDouble();
		
		if (d>=0 && d <_starProbability){
			_queryModifier = _modifiers[0];
		}
		else if (d>=_starProbability && d<_starProbability+_cliqueProbability)
		{
			_queryModifier = _modifiers[1];
		}
		else if (d>=_starProbability+_cliqueProbability 
				&& d<_starProbability+_cliqueProbability+_chainProbability)
		{
			_queryModifier = _modifiers[2];
		}
		else if (d>=_starProbability+_cliqueProbability+_chainProbability 
				&& d < _starProbability+_cliqueProbability+_chainProbability+_cycleProbability){
			_queryModifier = _modifiers[3];
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		/*CatalogGenerator catalogGenerator = new CatalogGenerator(100, 3, 2);
		catalogGenerator.generate();
		String sites_filename = System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
		String relations_filename = System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		QueryGenerator gen = new QueryGenerator(10, 
				System.getenv().get("HOME") + "/assembla_svn/Experiments/file");
		gen.generateClique();*/
	}
}
