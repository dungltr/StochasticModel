package thesis.utilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class CodeLineCounter {

	String _basedir;
	List<String> _dirs;
	List<String> files;
	int lineCount;

	public CodeLineCounter(String basedir){
		_basedir = basedir;
		_dirs = new LinkedList<String>();
		files = new LinkedList<String>();
		lineCount=0;
	}

	protected void obtainDirs() throws IOException{
		//Assumption that basedir only contains directories
		Process p = Runtime.getRuntime().exec("ls "+_basedir);

		BufferedReader stdInput = new BufferedReader(new 
				InputStreamReader(p.getInputStream()));
		System.out.println("Here is the standard output of the command:\n");
		String s = "";
		while ((s = stdInput.readLine()) != null) {
			_dirs.add(_basedir+s+"/");
			System.out.println("Obtained directory: "+_basedir+s+"/");
		}
		stdInput.close();
	}

	protected void obtainFiles() throws IOException{
		while(!_dirs.isEmpty()){
			String s = _dirs.remove(0);
			Process p = Runtime.getRuntime().exec("ls "+s);

			BufferedReader stdInput = new BufferedReader(new 
					InputStreamReader(p.getInputStream()));

			String file = "";
			while ((file = stdInput.readLine()) != null) {
				if(file.contains(".java")){
					files.add(s+file);
					System.out.println("Obtained file "+s+file);
				}
			}
			stdInput.close();
		}
	}

	public void count() throws IOException{
		obtainDirs();
		obtainFiles();
		obtainTotalCount();
		System.out.println("Project contains number of lines: "+lineCount);
	}

	private void obtainTotalCount() throws IOException {
		for(String s : files){
			BufferedReader br = new BufferedReader(new FileReader(s));
			String line = "";
			while((line=br.readLine())!=null){
				if(!line.equals("")){
					lineCount++;
				}
			}
			br.close();
		}
		
	}

	public static void main(String args[]) throws IOException{
		CodeLineCounter wc = new CodeLineCounter("/home/rob/workspace/CentralizedAlgorithm/src/");
		wc.count();
	}
}
