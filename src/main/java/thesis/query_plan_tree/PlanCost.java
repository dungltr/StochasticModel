package thesis.query_plan_tree;

import thesis.scheduler.Scheduler;
import thesis.scheduler.SchedulerException;
import thesis.task_tree.TaskTree;


public class PlanCost {
   
   public static final PlanCost MAX_VALUE = new PlanCost(Double.MAX_VALUE, Double.MAX_VALUE);
   //public final Scheduler _scheduler;
   protected final double _finishTime;
   protected final double _systemUtilization;
   protected double _totalIO;
   protected double _bytesTransferedOverNetwork;

   private PlanCost(double d, double c){
	   _finishTime=d;
	   _systemUtilization=c;
   }
   
   public PlanCost(Plan tree) throws SchedulerException{

	  
	  TaskTree taskTree = new TaskTree(tree);
	    Scheduler scheduler = new Scheduler(taskTree);
	    scheduler.schedule();
	    _finishTime=scheduler.getFinishTime();
	    _systemUtilization=scheduler.getSystemUtilizationFactor();
	    _totalIO=taskTree.getTotalIO();
	    _bytesTransferedOverNetwork=taskTree.getBytesSentOverNetwork();
   }
   
   public void setTotalIO(double totalIO){
	   _totalIO = totalIO;
   }
   
   public void setBytesSentOverNetwork(double b){
	   _bytesTransferedOverNetwork=b;
   }
   public boolean equals(Object that){
       if(this == that) return true;
       else if (getClass().equals(that.getClass()))
       {
           PlanCost c = (PlanCost)that;
           if(c._finishTime==_finishTime &&
				   c._systemUtilization==_systemUtilization)
		   {
			   return true;
		   }
		   else return false;
       }
       else return false;
   }

   public int compareTo(PlanCost c)
   {
       if(_finishTime<c._finishTime)
           return -1;
       else if (_finishTime>c._finishTime)
           return 1;
       else if(_systemUtilization<c._systemUtilization)
           return -1;
       else if (_systemUtilization>c._systemUtilization)
           return 1;
       else return 0;
   }
   
   public String toString(){
	   return _finishTime+" "+_systemUtilization+" ("+_totalIO+","+
	   (_bytesTransferedOverNetwork/(1024.0*1024.0))+")";
   }
}
