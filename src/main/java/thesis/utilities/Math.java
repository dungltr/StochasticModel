package thesis.utilities;

public class Math {

	public static int factorial(int n) throws NumberFormatException
	{
		if(n>19 || n<0)
			throw new NumberFormatException("my factorial method can't be used for" +
					" integers greater than 19 as it overflows integers, sorry!");
		if(n==0)
			return 1;
		
		int k = 1;
		for(int i = 2; i<=n; i++)
		{
			k*=i;
		}
		return k;
	}
}
