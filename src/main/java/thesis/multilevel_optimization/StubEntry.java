package thesis.multilevel_optimization;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Set;

import thesis.query_plan_tree.Site;

public class StubEntry implements Serializable{
	private static final long serialVersionUID = -7729906809240211371L;
	public final String _identifier;
	public  final double _numberOfTuples;
	public final int _sizeOfTupleInBytes;
	public final Set<Site> _sites;
	
	public StubEntry(String name, double numberOfTuples, int sizeOfTuplesInBytes, Set<Site> sites) throws RemoteException
	{
		_identifier = name;
		_numberOfTuples = numberOfTuples;
		_sizeOfTupleInBytes = sizeOfTuplesInBytes;
		_sites = sites;
	}
	
	public boolean equals(Object that){
		if(this == that) return true;
		else if (getClass().equals(that.getClass())){
			StubEntry e = (StubEntry)that;
			if(e._identifier.equals(_identifier) && e._numberOfTuples==_numberOfTuples
					&& e._sizeOfTupleInBytes==_sizeOfTupleInBytes)
				return true;
			else return false;
		}
		else return false;
	}
	
	public int hashCode(){
		return _sizeOfTupleInBytes+(int)_numberOfTuples+_identifier.hashCode();
	}
	
	public String toString(){
		return "["+_identifier+","+_numberOfTuples+","+_sizeOfTupleInBytes+"]";
	}
}
