package thesis.multilevel_optimization;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;

import thesis.joingraph.JoinGraph;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.catalog.RelationEntry;

public class OptimizationTask implements Serializable{
	private static final long serialVersionUID = -6378095297926391883L;
	protected JoinGraph _joinGraph;
	protected List<RelationEntry> _stubEntries;
	protected String _identifier;
	protected boolean _isGreedy;	
	//Specifies if we should consider the query site when optimizing.
	//Don't consider query site until final optimization.
	
	public OptimizationTask(JoinGraph graph, List<RelationEntry> previousStubEntries, 
			String stubIdentifier, boolean isGreedy) throws RemoteException{
		_joinGraph = graph;
		_stubEntries = previousStubEntries;
		_identifier = stubIdentifier;
		_isGreedy = isGreedy;
	}
	
	public JoinGraph getJoinGraph(){
		return _joinGraph;
	}
	
	public String getIdentifier(){
		return _identifier;
	}
	
	/**
	 * In order to optimize a join graph that contains intermediate stub relations
	 * we need to know the number of tuples in temp table and size of each tuple in bytes.
	 * The catalog at the allocated server node needs to updates to be in sync with master.
	 * @throws CatalogException 
	 */
	public void updateCatalog() throws CatalogException{
		for(RelationEntry s : _stubEntries){
			if (!Catalog.containsRelation(s.getName())){
				Catalog.addRelation(s);
			}
		}
	}
	
	public boolean equals(Object that){
		if(this==that) return true;
		else if(getClass().equals(that.getClass()))
		{
			OptimizationTask task = (OptimizationTask)that;
			if(task._joinGraph.equals(_joinGraph) &&
					task._stubEntries.equals(_stubEntries))
				return true;
			else return false;
		}
		else return false;
	}
	
	public int hashCode(){
		return _joinGraph.hashCode()+_stubEntries.hashCode();
	}

	public boolean isGreedy() {
		return _isGreedy;
	}
}
