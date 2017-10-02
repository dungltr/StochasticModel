package thesis.joingraph;

import java.io.Serializable;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;

public class JoinCondition implements Serializable, Cloneable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3758367327709684399L;
	protected List<String> _lhsFields;
	protected List<String> _rhsFields;
	
	public JoinCondition(){
		_lhsFields = new LinkedList<String>();
		_rhsFields = new LinkedList<String>();
	}
	
	public JoinCondition(String field1, String field2)
	{
		this();
		_lhsFields.add(field1);
		_rhsFields.add(field2);
	}
	
	public void addJoinCondition(JoinCondition jc){
		for(String s : jc._lhsFields){
			_lhsFields.add(s);
		}
		
		for(String s : jc._rhsFields){
			_rhsFields.add(s);
		}
	}
	
	public static JoinCondition flip(JoinCondition jc){
		JoinCondition j = new JoinCondition();
		List<String> temp = jc._lhsFields;
		j._lhsFields=jc._rhsFields;
		j._rhsFields=temp;
		return j;
	}
	
	public String toString(){
		String s = "";
		
		if(_lhsFields.size()!=_rhsFields.size())
			try {
				throw new JoinConditionException("Invalid join condition having " +
						"lhs: "+_lhsFields+" and rhs: "+_rhsFields);
			} catch (JoinConditionException e) {
				e.printStackTrace();
			}
		
		for(int i = 0; i<_lhsFields.size(); i++)
		{
			s+=_lhsFields.get(i)+"="+_rhsFields.get(i);
			if(i<_lhsFields.size()-1)
				s+=" AND ";
		}
		return s;
	}

	public static JoinCondition parse(String[] parts, int i, int j) throws ParseException {
		JoinCondition jc = new JoinCondition();

		for(int k = i; k<j; k++)
		{
			if(!parts[k].equals("AND")){
				String[] p = parts[k].split("=");
				if(p.length!=2)
					throw new ParseException("Invalid join condition, only equijoins supported!", 0);
				jc._lhsFields.add(p[0]);
				jc._rhsFields.add(p[1]);
			}
		}
		return jc;
	}
	
	public boolean equals(Object that)
	{
		if (this == that) return true;
		else if (getClass().equals(that.getClass())){
			JoinCondition joinCondition = (JoinCondition)that;
			if((_lhsFields.equals(joinCondition._lhsFields) &&
					_rhsFields.equals(joinCondition._rhsFields)) || 
					(_lhsFields.equals(joinCondition._rhsFields) && _rhsFields.equals(joinCondition._lhsFields)))
			{
				return true;
			}
			else return false;
		}
		else return false;
	}
	
	public int hashCode(){
		return _rhsFields.hashCode()+_lhsFields.hashCode();
	}
	
	public Object clone(){
		try {
			JoinCondition clone = (JoinCondition)super.clone();
			List<String> lhsFields = new LinkedList<String>();
			List<String> rhsFields = new LinkedList<String>();
			
			for(String s : _lhsFields)
				lhsFields.add(s);
			
			for(String s : _rhsFields)
				rhsFields.add(s);
			clone._lhsFields = lhsFields;
			clone._rhsFields = rhsFields;
			return clone;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			return null;
		}
	}
}
