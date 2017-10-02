package thesis.loggers;

import thesis.query_plan_tree.Site;

/**
 * A logger is associated with a given site.  Loggers print out information
 * by indicating where the print out is issued.
 * @author rob
 *
 */
public class Logger {

	protected Site _site;
	protected String _handle;
	public static boolean _loggingOn = true;
	
	public Logger(Site s, String handle){
		_site = s;
		_handle = handle;
	}
	
	public void print(String s)
	{
		if(_loggingOn)
			System.out.println("["+_site._ipAddress+" ("+_handle+")]: "+s);
	}
}
