package thesis.catalog;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import thesis.query_plan_tree.Site;

public class RelationEntry implements Serializable{
	private static final long serialVersionUID = -5474416943741499225L;
	protected String _name;
	protected double _numberOfTuples;
	protected int _sizeOfTupleInBytes;
	protected Set<Site> _sites;
	protected Schema _schema;

	//public final static RelationEntry NULL = new RelationEntry("", 0.0, 0, new HashSet<Site>());

	public RelationEntry(String name, double numberOfTuples, int sizeOfTuplesInBytes, Set<Site> sites)
	{
		_name = name;
		_schema = new Schema();
		_numberOfTuples = numberOfTuples;
		_sizeOfTupleInBytes = sizeOfTuplesInBytes;
		_sites = sites;
	}

	public RelationEntry(String stubIdentifier, double intermediateResultOfS) {
		_schema = new Schema();
		_name=stubIdentifier;
		_numberOfTuples=intermediateResultOfS;
		_sizeOfTupleInBytes = 0;
		_sites = new HashSet<Site>();
	}

	/**
	 * Merge relation entries by adding tuple sizes, union of sites
	 * and adding all fields. Name and _number of tuples need to be set manually. 
	 * @param e
	 * @throws CatalogException 
	 */
	public void conjunct(RelationEntry  e) throws CatalogException{
		_sizeOfTupleInBytes+=e._sizeOfTupleInBytes;
		_sites.addAll(e._sites);

		for(Iterator<Field> iter = e._schema.fieldsIterator(); iter.hasNext();)
		{
			Field field = iter.next();
			_schema.addField(field.getName(), field);
		}
	}

	public void setNumberOfTuples(double numberOfTuples){
		_numberOfTuples = numberOfTuples;
	}

	public void setName(String s){
		_name=s;
	}
	public void addField(String name, Field field) throws CatalogException{
		_schema.addField(name, field);
	}

	public String getName(){
		return _name;
	}

	public Schema getSchema(){
		return _schema;
	}
	public String toString(){
		return "["+_numberOfTuples+","+_sizeOfTupleInBytes+","+_sites+" "+_schema+" ]";
	}
}
