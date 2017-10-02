package thesis.iterators;

import java.util.Set;

public class CombinationIterator <T>{
	protected int[] _ptrs;
	protected int[] _ptrsEnd;
	protected T[] _source; 
	protected int _subsetSize;
	protected boolean _first;
	
	@SuppressWarnings("unchecked")
	public CombinationIterator(Set<T> source, int subsetSize)
	{
		_first = true;
		
		if(source.isEmpty())
			_source = null;
		else{
			_source = (T[])source.toArray();
			
			_subsetSize = subsetSize;
			_ptrs = new int[subsetSize];
			
			for(int i = 0; i<_subsetSize; ++i)
			{
				_ptrs[i] = i;	
			}

			_ptrsEnd = new int[_subsetSize];
			
			for(int i = 0; i<_subsetSize; ++i)
			{
				_ptrsEnd[i] = _source.length -(_subsetSize-i);	
			}
		}
	}
	
	public boolean next_combination(Set<T> destination)
	{
		if(_source==null)
			return false;
		
		destination.clear();
		
		if(_first)
		{
			_first=false;
			
			for(int i = 0; i<_subsetSize; ++i)
			{
				destination.add(_source[_ptrs[i]]);
			}

			return true;
		}

		int mover = findRightMostNonEndPtr();
			
		if(mover==-1)
			return false;
			
		++_ptrs[mover];
		adjustPtrs(mover);
		for(int i = 0; i<_subsetSize; ++i)
		{
			destination.add(_source[_ptrs[i]]);
		}

		return true;
	}
	
	public int findRightMostNonEndPtr()
	{
		for(int i = _subsetSize-1; i>=0; --i)
		{
			if(_ptrs[i] < _ptrsEnd[i])
				return i;
		}	
		return -1;
	}

	public void adjustPtrs(int ptrIndex)
	{
		if(ptrIndex == _subsetSize-1)
			return;

		for(int i = ptrIndex+1; i<_subsetSize; ++i)
		{
			if(_ptrs[ptrIndex]+(i-ptrIndex) < _ptrsEnd[i])
				_ptrs[i]=_ptrs[ptrIndex]+(i-ptrIndex); 
		}
	}
}
