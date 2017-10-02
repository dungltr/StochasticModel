package thesis.utilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class ScriptCreator {

	/**
	 * 
	 * @param outputFilename Full path of output file, with extension.
	 * Extension cannot be .dat or .png
	 * @param title
	 * @param xaxis
	 * @param yaxis
	 * @param legend
	 * @param dataFilename contains list of points with io/byte data.
	 * @throws IOException 
	 */
	public static void graphSingle(String dataFilename, String title, String xaxis, 
			String yaxis, String legend) throws IOException{
		String scriptFilename = "~/Desktop/assembla_svn/scripts/QualityTestWithK/graphSingle.sh";
		PrintWriter p = new PrintWriter(scriptFilename);
		p.println("gnuplot << EOF"); 
		p.println("set terminal png");
		p.println("set output \""+dataFilename+".png\"");
		p.println("set xlabel \""+xaxis+"\"");
		p.println("set ylabel \""+yaxis+"\"");
		p.println("set title \""+title+"\"");
		p.println("set xrange [0:]");
		p.println("set yrange [0:]");
		
		BufferedReader br = new BufferedReader(new FileReader(dataFilename));
		String line = "";

		String nameWithOutExtension = dataFilename.substring(0, dataFilename.lastIndexOf("."));
		String datFilename = nameWithOutExtension+".dat";
		PrintWriter datFile = new PrintWriter(datFilename);

		//a b(io,byte)
		while((line=br.readLine())!=null){
			String[] parts = line.split(" ");
			String a = parts[0];
			String[] parts2 = parts[1].split("(");
			String b = parts2[0];
			String[] parts3 = parts2[1].split(",");
			String io = parts3[0];
			String[] parts4 = parts3[1].split(")");
			String bytes = parts4[0];
			datFile.println(a+" "+b);
			p.println("set label \"("+io+","+bytes+") graph\"  at graph  "+a+", graph  "+b);
		}
		datFile.close();
		p.println("plot \""+dataFilename+".dat\" using 1:2 with linespoints ti \""+legend+"\"");
		p.println("EOF");
		p.close();
		
		Process process = Runtime.getRuntime().exec(scriptFilename+" "+datFilename);
	}
}
