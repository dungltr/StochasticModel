package thesis.scheduler;

import thesis.iterators.PostOrderTaskTreeIterator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import thesis.catalog.Catalog;
import thesis.task_tree.Task;
import thesis.task_tree.TaskTree;
import thesis.utilities.Interval;
import thesis.utilities.Pair;
import thesis.query_plan_tree.ReceiveNode;
import thesis.query_plan_tree.SendNode;
import thesis.query_plan_tree.Site;

public class Scheduler {

	protected TaskTree _tree;
	protected double _finishTime;
	protected Map<Site, List<ScheduleEntry>> _schedule;
	protected Map<Task, List<ScheduleEntry>> _childrenEntries;
	protected double totalDurationOfAllTasks;
	
	public Scheduler(TaskTree tree)
	{
		_tree = tree;
		_finishTime = 0;
		_schedule = new HashMap<Site, List<ScheduleEntry>>();
		_childrenEntries = new HashMap<Task, List<ScheduleEntry>>();
		totalDurationOfAllTasks = 0;
		
		for(Iterator<Site> iter = Catalog.siteIterator(); iter.hasNext();){
			Site site = iter.next();
			_schedule.put(site, new ArrayList<ScheduleEntry>());
			//System.out.println("added site: "+site);
		}
		//for(int i = 0; i<Configuration._sites.size(); i++)
		//{
		//	_schedule.put(Configuration._sites.get(i), new ArrayList<ScheduleEntry>());
		//}
	}
	
	public double getFinishTime()
	{
		return _finishTime;
	}
	
	public double getSystemUtilizationFactor() throws SchedulerException
	{
		double completeSystem = getFinishTime()*Catalog.getNumberOfSitesInSystem();
		if(completeSystem==0)
			throw new SchedulerException("This schedule has a finish time of 0");
		return totalDurationOfAllTasks/completeSystem;
	}
	public void schedule() throws SchedulerException
	{
		PostOrderTaskTreeIterator iter = new PostOrderTaskTreeIterator(_tree);

		while(iter.hasNext())
		{
			Task task = iter.next();
			
			if(task.getTree().getRoot().getClass().equals(SendNode.class))
			{
				
			}
			else if (task.getTree().getRoot().getClass().equals(ReceiveNode.class))
			{
				Task sendTask = task.getChildAt(0);
				double lower_bound = findLowerBound(sendTask);
				double duration =  sendTask.getDuration();
				
				Pair<Integer, Interval> send_gap =  findNextGap(sendTask.getSite(),
						-1, duration,lower_bound);
				Pair<Integer, Interval> receive_gap = findNextGap(task.getSite(), 
						-1, duration, lower_bound);
				Interval slot = Interval.NULL_INTERVAL;
				
				while (slot.equals(Interval.NULL_INTERVAL))
				{
					//check if overlap, if not then advance lowest gap by finish point
					if(receive_gap.getSecond().overlaps(send_gap.getSecond(), duration, slot))
					{
						break;
					}
					else
					{
						//System.out.println(send_gap.getSecond().getEnd()+" : "+receive_gap.getSecond().getEnd());
						if(send_gap.getSecond().getEnd()<receive_gap.getSecond().getEnd())
						{
							//advance send gap
							send_gap =  findNextGap(task.getChildAt(0).getSite(),
									send_gap.getFirst(), duration,lower_bound);
						}
						else
						{
							//advance receive gap
							receive_gap =  findNextGap(task.getSite(),
									receive_gap.getFirst(), duration,lower_bound);
						}
					}
				}
				
				List<ScheduleEntry> send_entries = _schedule.get(sendTask.getSite());
				List<ScheduleEntry> receive_entries = _schedule.get(task.getSite());
				ScheduleEntry send_entry = new ScheduleEntry(sendTask, slot.getStart(), slot.getEnd());
				ScheduleEntry receive_entry = new ScheduleEntry(task, slot.getStart(), slot.getEnd());
				
				send_entries.add(send_gap.getFirst(), send_entry);
				totalDurationOfAllTasks+=send_entry._interval.length();
				
				receive_entries.add(receive_gap.getFirst(), receive_entry);
				_finishTime = Math.max(_finishTime, receive_entry._interval.getEnd());
				totalDurationOfAllTasks+=receive_entry._interval.length();
				saveSchedule(task, receive_entry);
				_childrenEntries.remove(sendTask);
			}
			else
			{
				addToSchedule(task, findLowerBound(task));
			}
		}
	}
	
	public double findLowerBound(Task task)
	{
		double lower_bound = 0;
		
		if(_childrenEntries.containsKey(task)){
			for(Iterator<ScheduleEntry> child_iter = _childrenEntries.get(task).iterator();
			child_iter.hasNext();)
			{
				ScheduleEntry child = child_iter.next();
				lower_bound = Math.max(lower_bound,  child._interval.getEnd());
			}
		}
		return lower_bound;
	}
	
	/**
	 * Returns the next gap in schedule of Site site after the 
	 * schedule entry index.  Returns the index of gap. Gap size >=size.
	 * gap must be above lower_bound.
	 * @param site
	 * @param index
	 * @return (position at which schedule entry should be added, start time)
	 * @throws SchedulerException 
	 */
	public Pair<Integer, Interval> findNextGap(Site site, int index, double size, double lowerBound) throws SchedulerException
	{
		List<ScheduleEntry> entries = _schedule.get(site);
		if(entries==null) throw new SchedulerException("Tried to get schedule for site "+site+" " +
				"but this site does not exist in system!");
		
		if(entries.isEmpty()){
			return new Pair<Integer, Interval>(entries.size(), new Interval(0,Double.MAX_VALUE));
		}
		
		//Check space at base
		if(index==-1 && entries.get(0)._interval.getStart()>=lowerBound+size)
		{
				return new Pair<Integer, Interval>(0, new Interval(0,  entries.get(0)._interval.getStart()));
		}
		else if(index==-1)
			index++;
		
		//Find a gap
		for(int i = index; i<entries.size()-1; i++)
		{
			ScheduleEntry first = entries.get(i);
			ScheduleEntry second = entries.get(i+1);
			if(second._interval.getStart()-size>=first._interval.getEnd() 
					&& first._interval.getEnd()>=lowerBound)
			{
				//We have a gap that we can fit the task in.
				return new Pair<Integer, Interval>(i+1, 
						new Interval(first._interval.getEnd(), second._interval.getStart()));
			}	
		}
		
		//Just return the final gap i.e. starts after latest schedule entry and ends at Integer.MAX
			return new Pair<Integer, Interval>(entries.size(), new Interval(
					entries.get(entries.size()-1)._interval.getEnd(),Double.MAX_VALUE));
	}
	
	public void addToSchedule(Task task, double lower_bound) throws SchedulerException
	{
		//System.out.println("trying to get "+task.getSite());
		List<ScheduleEntry> entries = _schedule.get(task.getSite());
		
		if(entries==null)
			throw new SchedulerException("Tried to get schedule of site "+task.getSite()+" but " +
					"there was no entry!");
		ScheduleEntry entry = null;
		
		//If the site has no tasks currently scheduled or the latest task is scheduled to 
		//finish before/equal to the lower bound then just schedule entry at lower bound
		if(entries.isEmpty() || 
				entries.get(entries.size()-1)._interval.getEnd()<=lower_bound){
			entry = new ScheduleEntry(task,
					lower_bound,
					lower_bound+task.getDuration());
			entries.add(entry);
		}
		else
		{
			Pair<Integer, Interval> idx_start = findNextGap(task.getSite(), -1, task.getDuration(), lower_bound);
			if(idx_start.getFirst()>=0){
				entry = new ScheduleEntry(task, idx_start.getSecond().getStart(),
						idx_start.getSecond().getStart()+task.getDuration());
				entries.add(idx_start.getFirst(), entry);
			}
		}
		totalDurationOfAllTasks+=entry._interval.length();
		_finishTime = Math.max(_finishTime, entry._interval.getEnd());
		saveSchedule(task, entry);
	}
	
	public void saveSchedule(Task task, ScheduleEntry entry)
	{
		//Add the current schedule entry to the children of parent task.
		//This allows the lower bound based on dependencies to be found quicker.
		if(task.getParent()!=null){
			if(_childrenEntries.containsKey(task.getParent()))
			{
				_childrenEntries.get(task.getParent()).add(entry);
			}
			else
			{
				List<ScheduleEntry> children = new ArrayList<ScheduleEntry>();
				children.add(entry);
				_childrenEntries.put(task.getParent(), children);
			}
		}
		
		if(task.getNumberOfChildren()>0)
			_childrenEntries.remove(task);
	}
	public String toString()
	{
		String s = "";
		for(Iterator<Site> iter = _schedule.keySet().iterator(); iter.hasNext();)
		{
			Site site = iter.next();

			for(int i = 0; i<_schedule.get(site).size(); i++)
			{
				s+=site+"\t";
				s+=_schedule.get(site).get(i)._interval.getStart()+"\t";
				s+=_schedule.get(site).get(i)._interval.getEnd()+"\t";
				s+=_schedule.get(site).get(i)._task.getID()+"\n";
			}
			
		}
		return s;
	}
	
	public void outputToFile(String name) throws FileNotFoundException{
		PrintWriter p = new PrintWriter(name);
		p.print(toString());
		p.close();
	}
	
	protected class ScheduleEntry 
	{
		Task _task;
		Interval _interval;
		
		public ScheduleEntry(Task task, double s, double e){
			_task = task;
			_interval = new Interval(s,e);
		}
		
		public void setLowerInterval(double l)
		{
			_interval.setStart(l);
		}
		
		public void setUpperInterval(double u)
		{
			_interval.setEnd(u);
		}
		public String toString()
		{
			return "["+_interval.getStart()+","+_interval.getEnd()+"] "+_task.getID();
		}
	}
}
