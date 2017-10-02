package thesis.multilevel_optimization;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import thesis.joingraph.JoinGraphException;

import thesis.loggers.Logger;

import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.Site;
import thesis.remote_interfaces.Optimizer;
import thesis.remote_interfaces.OptimizerResultCollector;

import thesis.catalog.Catalog;
import thesis.catalog.CatalogException;

import thesis.enumeration_algorithms.DPccp;

public class OptimizerServer extends UnicastRemoteObject
implements Optimizer {
	
	private static final long serialVersionUID = -2327704638086718350L;
	protected Map<String, List<Plan>> _stubAlternativePlans;
	protected final Logger _logger;
	//protected static String dirname = 
	//	System.getenv().get("HOME") + "/assembla_svn/Experiments" +
	//	"/Experiment8.6_ComparisonML/2_Optimization_Sites/CompareML-9Sites/";
	protected DPccpOptimizer optimizer;

	public OptimizerServer(Site s) throws RemoteException {
		super(1099);
		_logger = new Logger(s, "Server");
		_stubAlternativePlans = Collections.synchronizedMap(new HashMap<String, List<Plan>>());
	}
	
	public void optimize(OptimizationTask task, OptimizerResultCollector o) 
	throws RemoteException, JoinGraphException, CatalogException {
		optimizer = new DPccpOptimizer(task, o);
		optimizer.start();
	}
	
	public static void main(String args[]) {
	try {
		

		
	
		dirname = System.getenv().get("HOME") + "/";
		
		Registry localreg = LocateRegistry.createRegistry(1099);
		String sites_filename = dirname+"SystemSites.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
		String relations_filename = dirname+"Catalog.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		OptimizerServer obj = new OptimizerServer(
				new Site(InetAddress.getLocalHost().getHostAddress()).internalize());
		//Naming.rebind("//"+InetAddress.getLocalHost().getHostAddress()+"/OptimizerServer1", obj);
		localreg.bind("OptimizerServer", obj);
		obj._logger.print("rmi://"+InetAddress.getLocalHost().getHostAddress()+"/OptimizerServer bound in registry");
	}
	catch (Exception e) {
		System.out.println("OptimizerServer error: " + e.getMessage());
		e.printStackTrace();
	}
}

	static String dirname;
	
	/*public static void main(String args[]) {
		try {
			if(args.length!=1)
				throw new Exception("Incorrect number of arguments");
			if(!args[0].equals("1") && !args[0].equals("3") && !args[0].equals("9"))
				throw new Exception("Invalid number of sites: "+args[0]);
			
			

			//dirname = System.getenv().get("HOME") + "/assembla_svn/Experiments/Experiment8.9/CompareML-"+args[0]+"Sites/";
			assert 1==Integer.parseInt(args[0]);
			dirname = System.getenv().get("HOME") + "/assembla_svn/Experiments/QualityTestWithK/" +
			"randomRelSites_"+1+"Sites/";
			
			Registry localreg = LocateRegistry.createRegistry(1099);
			String sites_filename = dirname+"SystemSites.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
			String relations_filename = dirname+"Catalog.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
			Catalog.populate(sites_filename, relations_filename);
			OptimizerServer obj = new OptimizerServer(
					new Site(InetAddress.getLocalHost().getHostAddress()).internalize());
			//Naming.rebind("//"+InetAddress.getLocalHost().getHostAddress()+"/OptimizerServer1", obj);
			localreg.bind("OptimizerServer", obj);
			obj._logger.print("rmi://"+InetAddress.getLocalHost().getHostAddress()+"/OptimizerServer bound in registry");
		}
		catch (Exception e) {
			System.out.println("OptimizerServer error: " + e.getMessage());
			e.printStackTrace();
		}
	}*/
	
	public Optimizer getOptimizerInstance(){
		return this;
	}
	
	protected class DPccpOptimizer extends Thread{
		
		OptimizerResultCollector _collector;
		OptimizationTask _task;
		
		public DPccpOptimizer(OptimizationTask task, OptimizerResultCollector c) throws CatalogException{
			_collector = c;
			_task = task;
			task.updateCatalog();
		}
		
		@SuppressWarnings("deprecation")
		public void run(){
			long start = System.currentTimeMillis();
			_logger.print("Started optimizing: "+_task._identifier);
			DPccp dpccp;
			try {
				dpccp = new DPccp(_task.getJoinGraph());
				dpccp.enumerate(_task.isGreedy());
				_stubAlternativePlans.put(_task.getIdentifier(), dpccp.getFinalTreePossibilities());
				_collector.signalOptimizationComplete(_task.getIdentifier(), getOptimizerInstance());
				//_collector.free(getOptimizerInstance());
			} catch (Exception e) {
				e.printStackTrace();
			} 
			
			long end = System.currentTimeMillis();
			_logger.print("Completed optimizing: "+_task._identifier+". Time taken: "+(end-start)+"ms");
			this.stop();
		}
	}

	/**
	 * Return the optimal plan for returning result to the given site for
	 * the sub-graph named stub.
	 */
	public Plan pullOptimizedTree(String stub, Site site) 
	throws RemoteException, OptimizationTaskPoolException {
		if(!_stubAlternativePlans.containsKey(stub))
			throw new OptimizationTaskPoolException("No optimized results found for stub "+stub);
		else{
			List<Plan> plans = _stubAlternativePlans.get(stub);
			for(Plan tree : plans)
			{
				if(tree.getRoot().getSite().equals(site))
					return tree;
			}
			throw new OptimizationTaskPoolException("No query plan exists where result is " +
					"materialized at "+site);	
		} 
	}

	@Override
	public void reCatalog() throws IOException, CatalogException, ParseException {
		String sites_filename = dirname+"SystemSites.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/SystemSites.conf";
		String relations_filename = dirname+"Catalog.conf";//System.getProperty("user.home")+"/workspace/CentralizedAlgorithm/Experiments/Catalog/Catalog.conf";
		Catalog.populate(sites_filename, relations_filename);
		
	}
}
