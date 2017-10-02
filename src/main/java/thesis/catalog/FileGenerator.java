package thesis.catalog;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import thesis.query_plan_tree.Site;

public class FileGenerator {

	public static void generateSitesFile(int numberOfSites, String filename) 
	throws FileNotFoundException
	{
		PrintWriter p = new PrintWriter(filename);
		for(int i = 0; i<numberOfSites; i++)
		{
			p.print("163.1.88."+i);
			if(i<numberOfSites-1)
				p.println();
		}
		p.close();
	}
}
