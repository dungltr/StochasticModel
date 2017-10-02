package thesis.catalog;

import java.text.ParseException;

public class Domain {

	public enum Type {A, B, C, D};
	
	public static int getSizeInBytes(Type t)
	{
		switch(t){
			case A: return 2;
			case B: return 10;
			case C: return 15;
			default:return 8;
		}
	}
	
	public static int getDomainCardinality(Type t){
		switch(t){
			case A: return 9;
			case B: return 91;
			case C: return 401;
			default:return 501;
		}
	}
	public static Type parseType(String s) throws ParseException{
		if(s.length()!=1)
			throw new ParseException("Domain Type parse exception. No such type "+s, 0);
		if(s.equals("A"))
			return Type.A;
		else if (s.equals("B"))
			return Type.B;
		else if (s.equals("C"))
			return Type.C;
		else if (s.equals("D"))
			return Type.D;
		else 
			throw new ParseException("Domain Type parse exception. No such type "+s, 0);
	}
}
