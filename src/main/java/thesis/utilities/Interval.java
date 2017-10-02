package thesis.utilities;

import java.lang.Math;
public class Interval {
	public static final Interval NULL_INTERVAL = new Interval(-1,-1);
	Pair<Double, Double> _interval;
	
	public Interval(double f, double s)
	{
		_interval = new Pair<Double, Double>(f,s);
	}
	
	public double getStart()
	{
		return _interval.getFirst();
	}
	
	public double getEnd()
	{
		return _interval.getSecond();
	}
	
	public void setStart(double s)
	{
		_interval.setFirst(s);
	}
	
	public void setEnd(double e)
	{
		_interval.setSecond(e);
	}
	
	public double length()
	{
		return getEnd()-getStart();
	}
	public boolean equals(Object that)
	{
		if(this==that)
			return true;
		else if(that.getClass().equals(getClass()))
		{
			Interval interval = (Interval)that;
			if(getStart()==interval.getStart() && getEnd()==interval.getEnd())
				return true;
			else
				return false;
		}
		else return false;
	}
	
	/**
	 * Checks if the given interval overlaps with this interval by the 
	 * amount given by size.  If so then we place the overlapping interval in result.
	 * @param that
	 * @param size
	 * @param result
	 * @return
	 */
	public boolean overlaps(Interval that, double size, Interval result)
	{
		if(getStart()>that.getEnd() || that.getStart()>getEnd())
			return false;
		else
		{
			double maxStart = Math.max(getStart(), that.getStart());
			double minEnd = Math.min(getEnd(), that.getEnd());
			
			if(minEnd - maxStart >=size)
			{
				result.setStart(maxStart);
				result.setEnd(maxStart+size);
				return true;
			}
			else return false;
		}	
	}
}
