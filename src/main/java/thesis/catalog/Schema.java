package thesis.catalog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Schema implements Serializable{
	private static final long serialVersionUID = -2572800283100812825L;
	protected Map<String, Field> _fields;
	
	public Schema(){
		_fields = new HashMap<String, Field>();
	}
	
	public void addField(String name, Field field) throws CatalogException{
		if(_fields.containsKey(name))
			throw new CatalogException("Trying to add a field that is already present " +
					"in relation entry in Catalog.");
		else{ 
			_fields.put(name, field);
		}
	}
	
	public Field getField(String name) throws CatalogException
	{
		if (_fields.containsKey(name))
			return _fields.get(name);
		else throw new CatalogException("Tried to get field "+name+" but it is not present in schema.");
	}
	public int getNumberOfFields(){
		return _fields.size();
	}
	
	public Iterator<Field> fieldsIterator(){
		return _fields.values().iterator();
	}
	public String toString(){
		String s = "<";
		for(Iterator<Field> iter = _fields.values().iterator(); iter.hasNext();){
			s+=iter.next().toString();
			if(iter.hasNext())
				s+=", ";
		}
		s+=">";
		return s;
	}
}
