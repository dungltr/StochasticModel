package thesis.catalog;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;

import thesis.catalog.Domain.Type;

public class Field implements Serializable{
	private static final long serialVersionUID = 7513951571415223968L;
	protected String _name;
	protected Domain.Type _type;
	protected static final Map<Field, WeakReference<Field>> _fields = 
		new WeakHashMap<Field, WeakReference<Field>>();
	
	public Field(String name, Domain.Type type)
	{
		_name = name;
		_type = type;
	}
	
	public Type getType(){
		return _type;
	}
	
	public String getName(){
		return _name;
	}
	public Field internalize()
	{
		Field s = null;
		WeakReference<Field> ref = _fields.get(this);
		if(ref!=null)
			s = ref.get();
		
		if(s==null)
		{
			s = this;
			_fields.put(s, new WeakReference<Field>(s));
		}
		return s;
	}
	
	public String toString(){
		return _type+" : "+_name;
	}
}
