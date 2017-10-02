package thesis.catalog;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import thesis.query_plan_tree.Site;

public class Catalog {
	protected static Map<String, RelationEntry> _relationEntries = new HashMap<String, RelationEntry>(); 
	protected static List<Site> _sitesInSystem = new LinkedList<Site>();
	protected static Site _querySite = null;

	public static final int _pageSize = 4096; 
	public static final int _bufferSizeInPages = 52;
	public static final double _ioTimeConstantForPage = 0.00006144;		//64.8 MB/s 
	public static final double _networkTimeToSendAByte = 0.000000021193;	//45MB/s
	protected static Map<String, String> _relationKeys = new HashMap<String, String>();
	
	public static void clear(){
		System.out.println("Clearing catalog...");
		_sitesInSystem = new LinkedList<Site>();
		_querySite = null;
		_relationKeys = new HashMap<String, String>();
		_relationEntries = new HashMap<String, RelationEntry>();
	}
	public static void addKey(String relation, String key) throws CatalogException
	{
		if(_relationKeys.containsKey(relation))
			throw new CatalogException("Relation already contains key.");
		else {
			_relationKeys.put(relation, key);
			//System.out.println("added key: "+relation);
		}
	}
	
	public static String getKey(String relation) throws CatalogException
	{
		if(!_relationKeys.containsKey(relation))
			throw new CatalogException("No relation exists so cannot get key: "+relation);
		else 
			return _relationKeys.get(relation);
	}
	//public static Map<Pair<String, String>, List<Pair<String, String>>> 
	//_foreignKeyConstraints = new HashMap<Pair<String, String>, List<Pair<String, String>>>();

	/*public static void putForeignKeyConstraint(String key_relation, String foreign_relation,
			String key, String foreign_key) throws CatalogException
	{
		if(_relationEntries.containsKey(key_relation) 
				&& _relationEntries.containsKey(foreign_relation))
		{
			Pair<String, String> p = new Pair<String, String>(key_relation, foreign_relation);
			Pair<String, String> k = new Pair<String, String>(key, foreign_key);
			
			if(_foreignKeyConstraints.containsKey(p))
				_foreignKeyConstraints.get(p).add(k);
			else{
				List<Pair<String, String>> l = new LinkedList<Pair<String, String>>();
				l.add(k);
				_foreignKeyConstraints.put(p, l);
			}
		}
		else throw new CatalogException("tried to add foreign key constaint " +
				"for relations that do not exist.");
	}*/
	
/*	public static boolean isForeignKeyRelationship(String key_relation, String foreign_relation,
			String key, String foreign_key) throws CatalogException
	{
		if(_relationEntries.containsKey(key_relation) 
				&& _relationEntries.containsKey(foreign_relation))
		{
			Pair<String, String> p = new Pair<String, String>(key_relation, foreign_relation);
			Pair<String, String> k = new Pair<String, String>(key, foreign_key);
			
			if(_foreignKeyConstraints.containsKey(p)){
				for(Pair<String, String> pair : _foreignKeyConstraints.get(p))
				{
					if(pair.getFirst().equals(key) && pair.getSecond().equals(foreign_key))
						return true;
				}
				return false;
			}
			else{
				return false;
			}
		}
		else throw new CatalogException("tried to add foreign key constaint " +
				"for relations that do not exist.");
	}
	*/
	public static RelationEntry getRelationEntry(String s) throws CatalogException{
		if (_relationEntries.containsKey(s))
		{
			return _relationEntries.get(s);
		}
		else throw new CatalogException("Relation \""+s+"\" is not present in Catalog");
	}
	
	public static void addField(String relation, String field, Domain.Type type) throws CatalogException
	{
		if (_relationEntries.containsKey(relation))
		{
			_relationEntries.get(relation).addField(field, new Field(field, type).internalize());
		}
		else throw new CatalogException("Relation \""+relation+"\" is not present in Catalog" +
		" so cannot add field "+field);
	}
	
	/*public static double getSelectivity(String relation1, String field1, 
			String relation2, String field2) throws CatalogException
	{
		if(_relationEntries.containsKey(relation1) 
				&& _relationEntries.containsKey(relation2))
		{
			Field f1 = _relationEntries.get(relation1)._schema.getField(field1);
			Field f2 = _relationEntries.get(relation2)._schema.getField(field2);
			return 1.0/Math.max(Domain.getDomainCardinality(f1.getType()), 
					Domain.getDomainCardinality(f2.getType()));
		}
		else throw new CatalogException("Relation is not present in Catalog");
	}*/
	
	public static Site getQuerySite(){
		return _querySite;
	}

	public static Iterator<Site> siteIterator(){
		return _sitesInSystem.iterator();
	}
	public static int getPageSize(){
		return _pageSize;
	}

	/**
	 * Get the sites at which a relation is present.
	 * @param name
	 * @return
	 * @throws CatalogException
	 */
	public static Iterator<Site> getAvailableSitesIterator(String name) throws CatalogException{
		if(_relationEntries.containsKey(name))
			return _relationEntries.get(name)._sites.iterator();
		else throw new CatalogException("Relation \""+name+"\" is not present in Catalog" +
		" so cannot get available sites.");
	}

	public static Schema getSchema(String name) throws CatalogException{
		if(_relationEntries.containsKey(name))
			return _relationEntries.get(name)._schema;
		else throw new CatalogException("Relation \""+name+"\" is not present in Catalog" +
		" so cannot get available sites.");
	}
	/**
	 * Adds relation to the catalog.
	 * @param name
	 * @param numberOfTuples
	 * @param sizeOfTuplesInBytes
	 * @param sites
	 * @throws CatalogException 
	 */
	public static void addRelation(String name, double numberOfTuples,
			int sizeOfTuplesInBytes, Set<Site> sites) throws CatalogException
	{
		if(numberOfTuples==0)
			throw new CatalogException("Cannot add relation with 0 tuples!");
		
		RelationEntry entry = new RelationEntry(name, numberOfTuples, sizeOfTuplesInBytes, sites);
		_relationEntries.put(name, entry);
		// System.out.println("added "+name+" "+sites);
		//System.out.println("added "+name+":"+entry+" to Catalog");
	}

	public static Site addSite(String ip)
	{
		Site site = new Site(ip).internalize();
		_sitesInSystem.add(site);
		return site;
	}

	public static void setQuerySite(String ip) {
		Site site = new Site(ip).internalize();
		_querySite = site;
	}

	public static int getNumberOfSitesInSystem(){
		return _sitesInSystem.size();
	}

	/**
	 * @param name The name of relation.
	 * @return The cardinality of a relation.
	 * @throws CatalogException 
	 */
	public static double getCardinality(String name) throws CatalogException{
		if(_relationEntries.containsKey(name))
			return _relationEntries.get(name)._numberOfTuples;
		else throw new CatalogException("Relation \""+name+"\" is not present in Catalog" +
		" so cannot get cardinality.");
	}

	/**
	 * @param name The name of relation.
	 * @return The size of tuple in relation name.
	 * @throws CatalogException
	 */
	public static int getTupleSize(String name) throws CatalogException{
		if(_relationEntries.containsKey(name))
			return _relationEntries.get(name)._sizeOfTupleInBytes;
		else throw new CatalogException("Relation \""+name+"\" is not present in Catalog" +
		" so cannot get the size of tuple.");
	}

	public static String catalogToString(){
		String s = "Sites: "+_sitesInSystem.toString()+"\n";

		for(Entry<String, RelationEntry> entry : _relationEntries.entrySet()){
			s+=entry.getKey()+":"+entry.getValue()+"\n";
		}
		return s;
	}

	public static boolean containsRelation(String relation){
		return _relationEntries.containsKey(relation);
	}

	protected static void populateRelationEntries(String string) 
	throws IOException, CatalogException, ParseException {
		BufferedReader br = new BufferedReader(new FileReader(string));
		String line = "";
		boolean fieldsLine = false;
		String relation = "";
		
		while((line=br.readLine())!=null)
		{
			if(line.startsWith("#"))
				continue;
			
			if(fieldsLine)
			{
				fieldsLine=false;
				String[] parts = line.split(" ");
				int i = 0; 
				
				while(i<parts.length-1)
				{
					if(parts[i+1].startsWith("_"))
						addKey(relation, parts[i+1]);
					addField(relation, parts[i+1], Domain.parseType(parts[i]));
					i+=2;
				}
			}
			else{
				String[] parts = line.split(" ");
				Set<Site> sites = new HashSet<Site>();
				for(int i = 3; i<parts.length; i++){
					String[] s = parts[i].split(":");
					sites.add(new Site(s[0]).internalize());
				}
				relation = parts[0];
				addRelation(parts[0], Double.parseDouble(parts[1]), Integer.parseInt(parts[2]), sites);
				fieldsLine=true;
			}
		}
	}

	protected static void populateSiteEntries(String filename) throws NumberFormatException, IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = "";

		while((line=br.readLine())!=null)
		{
			if(line.startsWith("#") || line.isEmpty())
				continue;
			else{
				String[] parts = line.split(" ");
				if(parts.length!=1) throw new IOException("Invalid Catalog-Site file format!");
				else
				{
					Site site = addSite(parts[0]);
					if(_querySite==null)
						_querySite=site;
				}
			}
		}
	}

	public static Set<Site> getRelationLocations(String s) {
		return _relationEntries.get(s)._sites;
	}

	public static Iterator<String> relationsIterator() {
		Set<String> relations = new TreeSet<String>();
		
		for(String s : _relationEntries.keySet())
			relations.add(s);
		
		
		return relations.iterator();
	}
	
	public static void populate(String sites_filename, String relations_filename) 
	throws IOException, CatalogException, ParseException
	{
		clear();
		populateSiteEntries(sites_filename);
		populateRelationEntries(relations_filename);
	}

	public static void addRelation(RelationEntry relationEntry) throws CatalogException {
		if(_relationEntries.containsKey(relationEntry.getName()))
			throw new CatalogException("Trying to add entry but relation entry already exists!");
		else 
			_relationEntries.put(relationEntry.getName(), relationEntry);
		
	}
}
