package thesis.utilities;

public class Pair <F, S>{
	protected F _first;
	protected S _second;
	
	public Pair(F first, S second)
	{
		_first = first;
		_second = second;
	}
	
	public F getFirst()
	{
		return _first;
	}
	
	public S getSecond()
	{
		return _second;
	}
	
	public void setFirst(F f)
	{
		_first = f;
	}
	
	public void setSecond(S s)
	{
		_second = s;
	}
	
	public String toString()
	{
		return "<"+_first+","+_second+">";
	}
	
	public int hashCode(){
		return _first.hashCode()+_second.hashCode();
	}
	@SuppressWarnings("unchecked")
	public boolean equals(Object that)
	{
		if(this==that) return true;
		else if (that.getClass().equals(getClass()))
		{
			Pair pair = (Pair)that;
			if(pair._first.equals(_first) && pair._second.equals(_second))
				return true;
			else 
				return false;
		}
		else return false;
		
	}
}
