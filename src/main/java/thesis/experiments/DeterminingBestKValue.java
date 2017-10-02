package thesis.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class DeterminingBestKValue {
	static int[] numberOfSites = {1, 3, 9};
	static int[] sizeOfQueries = {20, 40, 60, 80, 100};
	static String[] types = {"Chain", "Clique", "Cycle", "Star", "Mixed"};
	static int numberOfQueries = 20;
	static String[] algorithms = {"SeqML", "DistML"};
	static String _basedir = System.getenv().get("HOME") + "/assembla_svn/Experiments/QualityTestWithK/";

	protected static int getKValue(String algorithm, String type, int querySize, 
			int numberOfSites) throws Exception
			{
		if(numberOfSites!=1 && numberOfSites!=3 && numberOfSites!=9)
			throw new Exception("Unrecognised number of sites!");

		if(algorithm.equals("IDP1ccp") || algorithm.equals("DistML") || algorithm.equals("SeqML")){
			BufferedReader br = new BufferedReader(new FileReader(System.getenv().get("HOME") + "/"+algorithm+"KValues.txt"));
			String line;
			while((line=br.readLine())!=null){
				String[] parts = line.split(",");
				int lineQuerySize = Integer.parseInt(parts[0]);
				String lineType = parts[1];
				int kValue = 0;

				switch(numberOfSites){
				case 1: 
					if(!parts[2].equals("-")){ 
						kValue=Integer.parseInt(parts[2]);
					}
					else
						kValue=-1;
					break;
				case 3: 
					if(!parts[3].equals("-")){ 
						kValue=Integer.parseInt(parts[3]);
					}
					else
						kValue=-1;
					break;
				case 9: 
					if(!parts[4].equals("-")){ 
						kValue=Integer.parseInt(parts[4]);
					}
					else
						kValue=-1;
					break;
				}

				if(lineQuerySize==querySize && lineType.equals(type))
					return kValue;
			}
			throw new Exception("K value not found!");
		}
		else throw new Exception("unrecognised algorithm");
			}

	public static void main(String args[]) throws Exception{
		//printTallys();
		printCollectiveTally();
	}

	public static void printCollectiveTally() throws Exception{
		for(String alg : algorithms){
			for(int sites : numberOfSites){
				String dir = _basedir+"randomRelSites_"+sites+"Sites/"+alg+"/"; 
				
				for(int i = 0; i<types.length; i++)
				{
					String type = types[i];
					PrintWriter p = new PrintWriter(dir+type+".dat");
					for(int j : sizeOfQueries){
						String workingDir = dir+j+"-Queries/"+type+"/";
						int k = getAppropriateK(workingDir+"KTally.dat");
						
						p.print(j+" "+k);
						if(j<=80)
							p.println();
					}
					p.close();
				}
			}
		}
	}
	
	private static int getAppropriateK(String filename) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = "";
		int bestK = 0;
		int maxNum = 0;
		
		while((line=br.readLine())!=null){
			String[] l = line.split(" ");
			int tempK = Integer.parseInt(l[0]);
			int num = Integer.parseInt(l[1]);
			
			if(num>=maxNum){
				bestK = tempK;
				maxNum = num;
			}
		}
		br.close();
		if(maxNum==0)
			throw new Exception("best k has 0 tally...?");
		return bestK;
	}

	public static void printTallys() throws Exception{


		for(String alg : algorithms){
			for(int sites : numberOfSites){

				String dir = _basedir+"randomRelSites_"+sites+"Sites/"+alg+"/"; 
				for(int j : sizeOfQueries){
					for(int i = 0; i<types.length; i++)
					{
						String type = types[i];
						String workingDir = dir+j+"-Queries/"+type+"/";

						int maxK = getKValue(alg, type, j, sites);
						int[] kTally = new int[maxK+1];
						for(int q=1; q<21; q++)
						{
							int minK = 2;
							double optCost = Double.MAX_VALUE;

							for(int k = 2; k<=maxK; k++){
								String filename = "Result"+q+":k"+k+".txt";
								BufferedReader br = new BufferedReader(
										new FileReader(workingDir+filename));
								String line = br.readLine();
								if(line==null)
									throw new Exception("Could not read line from file: ");
								double tempCost = Double.parseDouble(line.split(" ")[0]);

								if(tempCost<optCost){
									minK = k;
									optCost = tempCost;
								}
							}
							if(optCost==Double.MAX_VALUE)
								throw new Exception("Could not find min opt value");
							kTally[minK]++;
						}
						PrintWriter p = new PrintWriter(workingDir+"KTally.dat");
						for(int z = 2; z<kTally.length-1; z++)
							p.println(z+" "+kTally[z]);
						p.print((kTally.length-1)+" "+kTally[(kTally.length-1)]);
						p.close();
					}
				}
			}
		}
	}
}
