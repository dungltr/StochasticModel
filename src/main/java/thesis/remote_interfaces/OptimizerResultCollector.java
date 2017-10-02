package thesis.remote_interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;

import thesis.multilevel_optimization.OptimizationTaskPoolException;


import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.Site;

public interface OptimizerResultCollector extends Remote{
	//void addResult(String s, QueryPlanTree result) throws RemoteException;
	Plan pullOptimizedTree(String stub, Site site) 
	throws RemoteException, OptimizationTaskPoolException;
	//void free(Optimizer o) throws RemoteException;
	void signalOptimizationComplete(String s, Optimizer optimizer) 
	throws RemoteException, OptimizationTaskPoolException;
}
