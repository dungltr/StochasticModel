package thesis.query_plan_tree;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Site implements Serializable{
	private static final long serialVersionUID = -770652326006219572L;
	protected static final Map<Site, Site> sites = 
		new ConcurrentHashMap<Site, Site>();
	public final String _ipAddress;
	public final int _processID;	//Not being used for project
	
	public Site(){
		this("localhost");
	}
	
	public Site(String ipAddress){
		_ipAddress = ipAddress;
		_processID = 1;
	}
	
	public Site internalize()
	{
		//Site s = null;
		Site s = sites.get(this);
		//if(ref!=null){
		//	s = ref.get();
		//}
		
		if(s==null)
		{
			s = this;
			sites.put(s, s);
		}
		if(s==null){
			System.out.println("how is s still null?!");
		//	throw new NullPointerException("Returning null as site for internalise!");
		}
		return s;
	}
	
	public boolean equals(Object that)
	{
		if(this == that) return true;
		else if (getClass().equals(that.getClass()))
		{
			Site site = (Site)that;
			return 	   _ipAddress.equals(site._ipAddress) 
				    && _processID == site._processID;	
		}
		else return false;
	}
	
	public String toString()
	{
		return _ipAddress;//+":"+_processID;
	}
	
	public int hashCode()
	{
		return _ipAddress.hashCode()+_processID;
	}
}
