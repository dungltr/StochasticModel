package thesis.join_graph_generator_framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.catalog.Domain;
import thesis.catalog.Field;
import thesis.catalog.Domain.Type;
import thesis.query_plan_tree.Site;

public class CatalogGenerator {
	private int _numberOfRelations;
	private int _numberOfSites;
	private Random _random;
	private List<Site> _sites;
	private int _relationsResideAtNumberOfSites;
	private String _dirname;
	
	public CatalogGenerator(int sizeOfQuery, int numberOfSites, 
			int relationsResideAtNumberOfSites, String dirname) throws CatalogException{
		_random = new Random();
		_numberOfSites = numberOfSites;
		_numberOfRelations = sizeOfQuery;
		_sites = new ArrayList<Site>();
		_relationsResideAtNumberOfSites = relationsResideAtNumberOfSites;
		_dirname = dirname;
		if(numberOfSites<relationsResideAtNumberOfSites)
			throw new CatalogException("Cannot create catalog.");
	}
	
	public void generate() throws FileNotFoundException, CatalogException {
		System.out.println(_dirname+"SystemSites.conf");
		PrintWriter p = new PrintWriter(_dirname+"SystemSites.conf");
		for(int i = 0; i<_numberOfSites; i++)
		{
			Site site = new Site("163.1.88."+i).internalize();
			_sites.add(site);
			p.print(site._ipAddress);
			if(i<_numberOfSites-1)
				p.println();
		}
		p.close();
		
		p = new PrintWriter(_dirname+"Catalog.conf");
		
		for(int i = 65; i<65+_numberOfRelations; i++){
			String name = ""+(char)(65+((i-65)%26));
			name+=(int)Math.ceil(i-64);
			System.out.println((i-64)+": Name: "+name);
			List<Field> fields = getRandomFields(name);
			int tupleSize = 0;
			
			String fieldLine = "";
			Iterator<Field> iterator = fields.iterator();
			Field field = null;
			
			if(iterator.hasNext()){
				field = iterator.next();
				tupleSize+=Domain.getSizeInBytes(field.getType());
				fieldLine+=field.getType()+" _"+field.getName();
				if(iterator.hasNext())
					fieldLine+=" ";
				//System.out.println("Attempting to add "+name);
				//System.out.println(field.getName());
				//Catalog.addKey(name, field.getName());
			}
			else throw new CatalogException("Error generating random fields!");
			
			while(iterator.hasNext()){
				field = iterator.next();
				tupleSize+=Domain.getSizeInBytes(field.getType());
				fieldLine+=field.getType()+" "+field.getName();
				if(iterator.hasNext())
					fieldLine+=" ";
			}
			
			p.print(name+" "+(int)getRandomRelationSize()+" "+tupleSize+" ");
			
			for(Iterator<Site> iter = getRandomSites().iterator(); iter.hasNext();){
				Site s = iter.next();
				p.print(s._ipAddress);
				
				if(iter.hasNext())
					p.print(" ");
			}
			p.println();
			p.print(fieldLine);
				
			if(i<65+_numberOfRelations-1)
				p.println();
		}
		p.close();
	}
	
	private List<Field> getRandomFields(String relation_name)
	{
		List<Field> fields = new LinkedList<Field>();
		String fieldIdentifier = relation_name+".F";
		int fieldNumber = 1;
		int numberOfFields = 5+_random.nextInt(6);
		
		for(int i = 0; i<numberOfFields; i++)
			fields.add(new Field(fieldIdentifier+(fieldNumber++), getRandomFieldType()));
			
		return fields;
	}
	
	private Type getRandomFieldType()
	{
		double d = _random.nextDouble();
		
		if(d<0.05)
			return Type.A;
		else if(d<0.55)
			return Type.B;
		else if (d<0.85)
			return Type.C;
		else
			return Type.D;
	}
	private double getRandomRelationSize(){
		double d = _random.nextDouble();
		if(d<0.05)
			return 1000+_random.nextInt(9000);
		else if(d<45)
			return 10000+_random.nextInt(90000);
		else if (d<70)
			return 100000+_random.nextInt(900000);
		else
			return 1000000+_random.nextInt(9000000);
	}
	
	private Set<Site> getRandomSites()
	{
		
		List<Site> s = new ArrayList<Site>();
		
		for(Site si : _sites)
			s.add(si);
		
		Set<Site> sites = new HashSet<Site>();
		
		Random r = new Random();
		//We randomly choose the number of sites for each relation.
		while(sites.size()<r.nextInt(_numberOfSites)+1)
		{
			int idx = _random.nextInt(s.size());
			sites.add(s.get(idx));
			s.remove(idx);
		}
		
		return sites;
	}
	
	public static void main(String args[]) throws IOException, CatalogException, ParseException{
		CatalogGenerator gen = new CatalogGenerator(10, 3, 2, System.getenv().get("HOME") + "/Catalog.conf");
		gen.generate();
		//Catalog.populate(System.getenv().get("HOME") + "/SystemSites.conf", 
		//		System.getenv().get("HOME") + "/Catalog.conf");
		//System.out.println(Catalog.catalogToString());
	}
}
