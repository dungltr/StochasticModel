package thesis.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;

import thesis.code_generators.TaskTableGenerator;

import thesis.join_graph_generator_framework.CatalogGenerator;
import thesis.join_graph_generator_framework.QueryGenerator;
import thesis.joingraph.JoinGraph;
import thesis.joingraph.JoinGraphException;
import thesis.multilevel_optimization.SequentialAlgorithm;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;
import thesis.utilities.Pair;
import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;
import thesis.enumeration_algorithms.DPccpException;

public class SequentialExperiment extends MainExperiment{

	public static void main(String args[]) throws Exception{
		dirname = System.getenv().get("HOME") + "/assembla_svn/Experiments" +
		"/Experiment8.6_ComparisonML/2_Optimization_Sites/CompareML-9Sites/";




		//Run to generate catalog and queries
		//setUp();

		//Run to read in catalog and execute queries
		/*String sites_filename = dirname+"/SystemSites.conf";
		String relations_filename = dirname+"/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);

		for(int i : querySize)
			executeQueries(dirname+i+"-Queries/", dirname+"Sequential/"+i+"-Queries/");*/
		//produceAllAverageTimes();
		//produceTimeComparisonGraphFiles();

		/*String dest = System.getenv().get("HOME") + "/assembla_svn/Experiments/" +
				"Experiment8.6_ComparisonML/1ExecutionSite/";
		String dir = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment8.6_ComparisonML/";

		mergeDistributedTimes(dest, dir, 9);*/
		String dir = System.getenv().get("HOME") + "/assembla_svn/Experiments/" +
		"Experiment8.6_ComparisonML/10_Optimization_Sites/CompareML-5Sites/";
		//produceOptimalityGraphMLStacked(dir);
		pullAverageForGraph(dir);
		//printOutAllGraphs(dir);

	}

	private static void printOutAllGraphs(String dir) throws IOException{
		for(int j : querySize){


			for(int i = 0; i<types.length; i++){
				String type = types[i];
				Process process = Runtime.getRuntime().exec(System.getenv().get("HOME") + "/assembla_svn/scripts/OptimalityComparison.sh "+dir+"OptimalityComparisonGraph-Query"+j+"-"+type);
			}
		}
	}

	/**
	 * This goes through files named OptimalityComparisonGraph-Query[j]-type.dat
	 * and calculated average of seqML and distML.  Once all values are obtained
	 * we create graph file.
	 * @throws IOException 
	 */
	private static void pullAverageForGraph(String dir) throws IOException{
		for(int i = 0; i<types.length; i++){
			String type = types[i];
			//results[i] corresponds to query (i+1)*20 
			double[][] results = new double[5][2];
			for(int j : querySize){

				BufferedReader brSeq = 
					new BufferedReader(new FileReader(dir+"OptimalityComparisonGraph-Query"+j+"-"+type+".dat"));
				String line = "";
				double seqTotal = 0;
				double distTotal = 0;
				
				while((line=brSeq.readLine())!=null){
					String[] k = line.split(" ");
					seqTotal+=Double.parseDouble(k[1]);
					distTotal+=Double.parseDouble(k[2]);
				}
				brSeq.close();
				results[(j/20)-1][0]=seqTotal/5.0;
				results[(j/20)-1][1]=distTotal/5.0;
			}
			PrintWriter p = new PrintWriter(dir+"Comparison"+type+".dat");
			for(int z = 0; z<results.length; z++){
				p.println((z+1)*20+" "+results[z][0]+" "+results[z][1]);
			}
			p.close();
		}
	}
	private static void produceOptimalityGraphMLStacked(String dir) throws Exception{
		//String line = "";
		for(int j : querySize){

			for(int i = 0; i<types.length; i++){
				String type = types[i];
				PrintWriter p = new PrintWriter(dir+"OptimalityComparisonGraph-Query"+j+"-"+type+".perf");
				p.println("# clustered and stacked graph comparison of ML algorithms for optimality");
				p.println("=stackcluster;I/O;Network usage");
				p.println("=sortbmarks");
				p.println("=nogridy");
				p.println("=noupperright");
				p.println("legendx=right");
				p.println("legendy=center");
				p.println("yformat=%g");
				p.println("xlabel=Queries");
				p.println("ylabel=Scaled Optimal Plan Cost");
				p.println("=table");

				for(int k = 1; k<numberOfQueries; k++){
					//line="";
					p.println("multimulti=Query"+k);
					//	p.print("SeqML ");
					BufferedReader brSeq = 
						new BufferedReader(new FileReader(dir+"Sequential/"+j+"-Queries/"+
								type+"/"+getResultFileName(k, 
										dir+"Sequential/"+j+"-Queries/"+
										type+"/")));
					String s = brSeq.readLine();
					brSeq.close();
					String[] temp = s.split(" ");


					Pair<Double, Double> seqBreakdown = extractBreadkdown(temp[2]);
					double seqTime = seqBreakdown.getFirst()+seqBreakdown.getSecond();
					BufferedReader brDist = 
						new BufferedReader(new FileReader(dir+"Distributed/"+j+"-Queries/"+
								type+"/"+getResultFileName(k, 
										dir+"Distributed/"+j+"-Queries/"+
										type+"/")));
					s = brDist.readLine();
					brDist.close();
					temp = s.split(" ");


					Pair<Double, Double> distBreakdown = extractBreadkdown(temp[2]);
					double distTime = distBreakdown.getFirst()+distBreakdown.getSecond();

					double min = Math.min(seqTime, distTime);
					//p.println("SeqML-Schedule "+seqTime/min);
					p.println("SeqML "+(seqBreakdown.getFirst()/min)+" "+(seqBreakdown.getSecond()/min));
					//p.println("DistML-Schedule "+distTime/min);
					p.println("DistML "+(distBreakdown.getFirst()/min)+" "+(distBreakdown.getSecond()/min));

					//System.out.println("SeqTime: "+seqTime+" "+seqBreakdown.getFirst()+" "+seqBreakdown.getSecond());

					//line+=(seqBreakdown.getFirst()/min)+" "+(seqBreakdown.getSecond()/min);
					//p.println(line);
					//System.out.println(line);


					//line="DistML ";
					//
					//line+=(distBreakdown.getFirst()/min)+" "+(distBreakdown.getSecond()/min);
					//p.println(line);
				}
				p.close();
			}
		}
	}

	private static Pair<Double, Double> extractBreadkdown(String s) throws Exception{
		if(s.charAt(0)!='(')
			throw new Exception("invalid breakdown");

		s = s.substring(1, s.length());

		String[] b = s.split(",");

		String io = b[0];
		String network = b[1].substring(0, b[1].length()-1);
		System.out.println("IO: "+io+" network: "+network);
		return new Pair<Double, Double>(Double.parseDouble(io)*Catalog._ioTimeConstantForPage, 
				Double.parseDouble(network)*1024*1024*Catalog._networkTimeToSendAByte);
	}
	private static void produceOptimalityGraphML(String dir) throws IOException{
		String line = "";
		for(int j : querySize){


			for(int i = 0; i<types.length; i++){
				String type = types[i];
				PrintWriter p = new PrintWriter(dir+"OptimalityComparisonGraph-Query"+j+"-"+type+".dat");

				for(int k = 1; k<numberOfQueries; k++){
					p.print(k+" ");
					BufferedReader brSeq = 
						new BufferedReader(new FileReader(dir+"Sequential/"+j+"-Queries/"+
								type+"/"+getResultFileName(k, 
										dir+"Sequential/"+j+"-Queries/"+
										type+"/")));
					String s = brSeq.readLine();
					String[] temp = s.split(" ");

					double seq = Double.parseDouble(temp[0]);
					//line+=temp[0]+" ";

					brSeq.close();

					BufferedReader brDist = 
						new BufferedReader(new FileReader(dir+"Distributed/"+j+"-Queries/"+
								type+"/"+getResultFileName(k, 
										dir+"Distributed/"+j+"-Queries/"+
										type+"/")));
					s = brDist.readLine();
					temp = s.split(" ");

					double dist = Double.parseDouble(temp[0]);
					//line+=temp[0];
					double min = Math.min(seq, dist);
					line+=(seq/min)+" "+(dist/min);
					//System.out.println(line);
					brSeq.close();
					p.println(line);
					line="";
				}
				p.close();
			}

		}
	}

	private static String getResultFileName(int i, String dir) throws IOException{
		Process process = Runtime.getRuntime().exec("ls "+dir);

		BufferedReader br = new BufferedReader(new 
				InputStreamReader(process.getInputStream()));

		String line = "";
		while((line=br.readLine())!=null){
			if(line.contains("Result"+i)){
				br.close();
				return line;
			}
		}
		throw new IOException("Query result file for "+i+" not found!");
	}
	private static void mergeDistributedTimes(String dest, String dir, int numberOfExecutionSites) throws IOException{
		String line = "";
		int[] numberOfOptSites = {2, 5, 10, 15, 20};
		for(int i = 0; i<types.length; i++){
			String type = types[i];
			PrintWriter p = new PrintWriter(dest+type+".dat");
			for(int j : querySize){
				line+=j+" ";

				BufferedReader br = 
					new BufferedReader(new FileReader(dir+
							"10_Optimization_Sites/CompareML-"+
							numberOfExecutionSites+"Sites/Sequential/"+j+"-Queries/"+
							type+"/AveragedTime.txt"));
				line+=br.readLine()+" ";
				br.close();

				for(int os = 0; os < numberOfOptSites.length; os++){
					br = 
						new BufferedReader(new FileReader(dir+numberOfOptSites[os]+
								"_Optimization_Sites/CompareML-"+
								numberOfExecutionSites+"Sites/Distributed/"+j+"-Queries/"+
								type+"/AveragedTime.txt"));
					String av = br.readLine();
					br.close();
					line+=av;
					if(os<numberOfOptSites.length-1)
						line+=" ";
				}
				p.println(line);
				line="";
			}
			p.close();
		}
	}

	private static void produceTimeComparisonGraphFiles() throws IOException {
		String line = "";
		for(int i = 0; i<types.length; i++){
			String type = types[i];
			PrintWriter p = new PrintWriter(dirname+"TimeComparisonGraph-"+type+".dat");

			for(int j : querySize){
				line+=j+" ";

				BufferedReader brSeq = 
					new BufferedReader(new FileReader(dirname+"Sequential/"+j+"-Queries/"+
							type+"/AveragedTime.txt"));
				line+=brSeq.readLine()+" ";
				brSeq.close();

				BufferedReader brDist = 
					new BufferedReader(new FileReader(dirname+"Distributed/"+j+"-Queries/"+
							type+"/AveragedTime.txt"));
				line+=brDist.readLine();
				brDist.close();
				p.println(line);
				line="";
			}
			p.close();
		}
	}

	public static void produceAllAverageTimes() throws IOException{
		String[] alg = {
				//"Sequential", 
		"Distributed"};

		for(String s : alg){
			for(int j : querySize){
				String dir = dirname+s+"/"+j+"-Queries/";
				for(int i = 0; i<types.length; i++)
				{
					String type = types[i];
					produceAverageTime(dir+type+"/");
				}
			}
		}
	}

	/**
	 * Given a dir of chain, cycle etc look through 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	public static void produceAverageTime(String dir) throws IOException{
		System.out.println(dir);
		Process process = Runtime.getRuntime().exec("ls "+dir);

		BufferedReader br = new BufferedReader(new 
				InputStreamReader(process.getInputStream()));


		//Results should be first 5 entries.
		double totalTime = 0;

		String line = "";
		while((line=br.readLine())!=null){
			if(line.contains("Result")){
				BufferedReader b = new BufferedReader(new FileReader(dir+line));
				b.readLine();
				totalTime+= Double.parseDouble(b.readLine());
				b.close();
			}
		}
		br.close();

		PrintWriter p = new PrintWriter(dir+"AveragedTime.txt");
		p.println(totalTime/5.0);
		p.close();
		process = Runtime.getRuntime().exec("rm ~/Desktop/ZDir.txt");
	}
}
