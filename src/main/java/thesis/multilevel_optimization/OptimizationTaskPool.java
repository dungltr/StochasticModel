package thesis.multilevel_optimization;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import thesis.catalog.CatalogException;

import thesis.joingraph.JoinGraphException;
import thesis.loggers.Logger;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.Site;
import thesis.remote_interfaces.Optimizer;
import thesis.remote_interfaces.OptimizerResultCollector;

public class OptimizationTaskPool extends UnicastRemoteObject 
implements OptimizerResultCollector, Runnable{
	private static final long serialVersionUID = -4048985973734063175L;
	protected ConcurrentHashMap<String, Boolean> _optimizedSubplans; 
	protected Queue<OptimizationTask> _submitRequests;
	//protected Map<Site, Optimizer> _optimizers;
	protected Map<String, Optimizer> _stubMapping;
	protected Queue<Optimizer> _free;
	protected List<Optimizer> _optimizers;
	protected boolean _stop;
	protected Thread _optimizerPool;
	protected Logger _logger;
	
	public OptimizationTaskPool(ConcurrentHashMap<String, Boolean> optimizedSubplans, Site s) throws NumberFormatException, NotBoundException, IOException{
		super(1099);
		_stop=false;
		_logger = new Logger(s, "Master");
		_optimizers = new LinkedList<Optimizer>();
		_submitRequests = new LinkedList<OptimizationTask>();
		_free = new LinkedList<Optimizer>();
		_optimizedSubplans = optimizedSubplans;
		_stubMapping = new HashMap<String, Optimizer>();
		bindOptimizers();
		_optimizerPool = new Thread(this);
		_optimizerPool.start();
	}
	
	public void stop(){
		_stop = true;
	}
	int count = 0;
	
	@SuppressWarnings("deprecation")
	public void run() {
		while(true){
			if(_stop)
				break;
			try {
				if(!_submitRequests.isEmpty() && !_free.isEmpty()){
					OptimizationTask task = _submitRequests.poll();
					Optimizer optimizer = _free.poll();
					_stubMapping.put(task.getIdentifier(), optimizer);
					optimizer.optimize(task, this);
				}
				Thread.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			catch (RemoteException e) {
				e.printStackTrace();
			} catch (JoinGraphException e) {
				e.printStackTrace();
			} catch (CatalogException e) {
				e.printStackTrace();
			} 
		}
		_optimizerPool.stop();
	}
	
	/**
	 * Picks a free site, executes the optimization of the task at that site,
	 * places the result in _optimizedSubplans and then marks the site as available.
	 * @param task
	 * @throws RemoteException
	 * @throws JoinGraphException
	 */
	public void submit(OptimizationTask task) throws RemoteException, JoinGraphException{
		_optimizedSubplans.put(task.getIdentifier(), false);
		_submitRequests.add(task);
		_logger.print("submitted task "+task._identifier);
	}
	
	/**
	 * Reads in all sites from site file.
	 * Binds all available optimizers to objects
	 * @throws NotBoundException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public void bindOptimizers() throws NotBoundException, NumberFormatException, IOException{
		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.home")+
				"/workspace/CentralizedAlgorithm/ServerSites.conf"));
		String line = "";
		
		while((line=br.readLine())!=null)
		{
			if(line.startsWith("#") || line.equals(""))
				continue;
			else
			{
				//String[] parts = line.split(" ");
				Site s = null;
				
				s = new Site(line).internalize();
				_logger.print("looking up rmi://"+s._ipAddress+ "/OptimizerServer");
				Optimizer optimizer = (Optimizer)Naming.lookup("rmi://"+s._ipAddress
						+ "/OptimizerServer");
				if(_free.contains(optimizer))
					throw new IOException("Site has already been seen hence Site.conf not in correct format!");
				else{
					_free.add(optimizer);
					_optimizers.add(optimizer);
				}
			}
		}
	}

	/*public synchronized void addResult(String s, QueryPlanTree result)
			throws RemoteException {
		_optimizedSubplans.remove(s);
		_optimizedSubplans.put(s, result);
		result.outputToFile(System.getProperty("user.home")+
				"/workspace/CentralizedAlgorithm/multilevel_trees/"+s+".tex");
	}*/

	//public void free(Optimizer o) throws RemoteException {
	//	_free.add(o);
	//}

	@Override
	public Plan pullOptimizedTree(String stub, Site site)
			throws RemoteException, OptimizationTaskPoolException {
		if(!_stubMapping.containsKey(stub)) throw new OptimizationTaskPoolException("unrecognised stub!");
		else
		{
			return _stubMapping.get(stub).pullOptimizedTree(stub,site);
		}
	}

	@Override
	public synchronized void signalOptimizationComplete(String s, Optimizer o) 
	throws RemoteException, OptimizationTaskPoolException {
		if(!_optimizedSubplans.containsKey(s))
			throw new OptimizationTaskPoolException("unrecognised stub!");
		else {
			_optimizedSubplans.remove(s);
			_optimizedSubplans.put(s, true);
			_free.add(o);
		}	
	}

	public void reCatalog() throws IOException, CatalogException, ParseException {
		// TODO Auto-generated method stub
		for(Optimizer o : _optimizers)
			o.reCatalog();
		
	}
}
