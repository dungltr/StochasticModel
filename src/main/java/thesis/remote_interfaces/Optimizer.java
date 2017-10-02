package thesis.remote_interfaces;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.text.ParseException;

import thesis.catalog.CatalogException;

import thesis.multilevel_optimization.OptimizationTask;
import thesis.multilevel_optimization.OptimizationTaskPoolException;

import thesis.joingraph.JoinGraphException;
import thesis.query_plan_tree.Plan;
import thesis.query_plan_tree.Site;

public interface Optimizer extends Remote {
	Plan pullOptimizedTree(String s, Site site)
	throws RemoteException, OptimizationTaskPoolException;
	void optimize(OptimizationTask task, OptimizerResultCollector o) 
	throws RemoteException, JoinGraphException, CatalogException;
	void reCatalog() throws IOException, CatalogException, ParseException;
}
