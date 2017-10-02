package thesis.joingraph;

import java.io.Serializable;

public class JoinInfo implements Serializable, Cloneable{
	private static final long serialVersionUID = 5877540300561402737L;
	protected double _selectivity;
	protected JoinCondition _joinCondition;
	
	//private static final JoinInfo NULL = new JoinInfo(1.0);
	
	public JoinInfo(double selectivity, JoinCondition jc)
	{
		_selectivity = selectivity;
		_joinCondition = jc;
	}
	
	public JoinInfo(double sel)
	{
		_selectivity=sel;
		_joinCondition=new JoinCondition();
	}
	
	public double getSelectivity(){
		return _selectivity;
	}
	
	public void setSelectivity(double s){
		if(s>0 && s<=1)
			_selectivity=s;
		else throw new IndexOutOfBoundsException("JoinInfo: Selectivity must be between 0 and 1");
	}
	
	public Object clone(){
		try {
			JoinInfo clone = (JoinInfo)super.clone();
			clone._joinCondition = (JoinCondition)_joinCondition.clone();
			return clone;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			return null;
		}
	}
	public boolean equals(Object that)
	{
		if(this == that) return true;
		else if (getClass().equals(that.getClass()))
		{
			JoinInfo info = (JoinInfo) that;
			if(_selectivity==info._selectivity &&
					_joinCondition.equals(info._joinCondition))
				return true;
			else 
				return false;
		}
		else return false;
	}
	
	public void conjunct(JoinInfo b)
	{
		_selectivity = _selectivity*b._selectivity;
		_joinCondition.addJoinCondition(b._joinCondition);
	}

	public JoinCondition getJoinCondition() {
		return _joinCondition;
	}
	
	public int hashCode(){
		return (int)_selectivity+_joinCondition.hashCode();
	}
	public String toString(){
		return _joinCondition.toString()+" "+_selectivity;
	}
}
