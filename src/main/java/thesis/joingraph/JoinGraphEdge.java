package thesis.joingraph;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;

public class JoinGraphEdge {
	protected static final Map<JoinGraphEdge, WeakReference<JoinGraphEdge>> _joinGraphEdges = 
		new WeakHashMap<JoinGraphEdge, WeakReference<JoinGraphEdge>>();
	public final String _vertex1;
	public final String _vertex2;
	public final JoinInfo _joinInfo;
	
	public JoinGraphEdge(String v1, String v2, JoinInfo joinInfo)
	{
		_vertex1 = v1;
		_vertex2 = v2;
		_joinInfo = joinInfo;
	}
	
	public JoinGraphEdge internalize()
	{
		JoinGraphEdge s = null;
		WeakReference<JoinGraphEdge> ref = _joinGraphEdges.get(this);
		if(ref!=null)
			s = ref.get();
		
		if(s==null)
		{
			s = this;
			_joinGraphEdges.put(s, new WeakReference<JoinGraphEdge>(s));
		}
		return s;
	}
	
	public boolean equals(Object that)
	{
		if(this==that) return true;
		else if(getClass().equals(that.getClass()))
		{
			JoinGraphEdge g = (JoinGraphEdge)that;
			//if(g._joinInfo.equals(_joinInfo))
			//{
				if(_vertex1.equals(g._vertex1) && _vertex2.equals(g._vertex2) ||
						_vertex1.equals(g._vertex2) && _vertex2.equals(g._vertex1))
				{
					return true;
				}
				else return false;
			//}
			//else return false;
		}
		else return false;
	}
	
	public int hashCode(){
		return _vertex1.hashCode()+_vertex2.hashCode();//+_joinInfo.hashCode();
	}
	
	public String toString(){
		return _vertex1+"->"+_vertex2+":"+_joinInfo;
	}
}
