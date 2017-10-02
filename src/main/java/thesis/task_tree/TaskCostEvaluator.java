package thesis.task_tree;

import thesis.catalog.Catalog;
import thesis.iterators.PostOrderTaskIterator;
import thesis.query_plan_tree.JoinNode;
import thesis.query_plan_tree.QueryPlanTreeException;
import thesis.query_plan_tree.PlanNode;
import thesis.query_plan_tree.CostPlanVisitor;
import thesis.query_plan_tree.ReceiveNode;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.SendNode;
import thesis.utilities.Pair;

public class TaskCostEvaluator implements CostPlanVisitor{

	double _cost;
	double _totalIO;
	double _totalBytesSent;
	
	public TaskCostEvaluator(Task task)
	{
		_cost = 0;
		_totalBytesSent=0;
		_totalIO = 0;
		evaluateCost(task);
	}
	
	public void evaluateCost(Task task)
	{
		_cost = 0;
		_totalBytesSent=0;
		_totalIO = 0;
		PostOrderTaskIterator iter = new PostOrderTaskIterator(task);
	
		while(iter.hasNext())
		{
			PlanNode node = iter.next();
			Pair<Double, Double> costBreakdown = node.accept(this);
			_totalIO+=costBreakdown.getFirst();
			_totalBytesSent+=costBreakdown.getSecond();
		}
	}
	
	public double getTotalBytesSentOverNetwork(){
		return _totalBytesSent;
	}
	
	public double getTotalNumberOfIOs(){
		return _totalIO;
	}
	
	public double getCost()
	{
		return _cost;
	}
	
	public Pair<Double, Double> visit(JoinNode node) {
		Pair<Double, Double> costBreakdown = new Pair<Double, Double>(0.0, 0.0);
		try {
			double inputCost = 0;
			double outputCost = 0;
			
			PlanNode outer = node.getChildAt(0);
			PlanNode inner = node.getChildAt(1);
			
			if(outer.outputIsMaterialized()){
				inputCost+=outer.getNumberOfOutputPages()*Catalog._ioTimeConstantForPage;
				costBreakdown.setFirst(costBreakdown.getFirst()+outer.getNumberOfOutputPages());
			}
			
			if(inner.outputIsMaterialized()){
				inputCost+=Math.ceil(outer.getNumberOfOutputPages()/(Catalog._bufferSizeInPages-2))
						*inner.getNumberOfOutputPages()*Catalog._ioTimeConstantForPage;
				costBreakdown.setFirst(costBreakdown.getFirst()+
						Math.ceil(outer.getNumberOfOutputPages()/(Catalog._bufferSizeInPages-2))
						*inner.getNumberOfOutputPages());
			}
			
			if(node.outputIsMaterialized()){
				outputCost = node.getNumberOfOutputPages()*Catalog._ioTimeConstantForPage;
				costBreakdown.setFirst(costBreakdown.getFirst()+ node.getNumberOfOutputPages());
			}
			
			_cost+= inputCost+outputCost;
			return costBreakdown;
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Pair<Double, Double> visit(ReceiveNode node) {
		try {
			double inputCost = 0;
			double outputCost = 0;
			PlanNode child = node.getChildAt(0);
			inputCost =  Catalog._networkTimeToSendAByte*child.getTotalOutputBytes();
			outputCost = Catalog._ioTimeConstantForPage*child.getNumberOfOutputPages();
			_cost+= inputCost+outputCost;
			return new Pair<Double, Double>(child.getNumberOfOutputPages(), child.getTotalOutputBytes());
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Pair<Double, Double> visit(SendNode node) {
		Pair<Double, Double> costBreakdown = new Pair<Double, Double>(0.0, 0.0);
		
		try {
			double inputCost = 0;
			double outputCost = 0;
			PlanNode child = node.getChildAt(0);
			
			if(child.outputIsMaterialized()){
				inputCost = Catalog._ioTimeConstantForPage*child.getNumberOfOutputPages();
				costBreakdown.setFirst(child.getNumberOfOutputPages());
			}
			
			outputCost = Catalog._networkTimeToSendAByte*child.getTotalOutputBytes();
			costBreakdown.setSecond(child.getTotalOutputBytes());
			_cost+=inputCost+outputCost;
			return costBreakdown;
		} catch (QueryPlanTreeException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Pair<Double, Double> visit(RelationNode node) {
		double inputCost = 0;
		double outputCost = 0;
		outputCost =  Catalog._ioTimeConstantForPage*node.getNumberOfOutputPages();
		_cost += inputCost+outputCost;
		return new Pair<Double, Double>(node.getNumberOfOutputPages(), 0.0);
	}

	public Pair<Double, Double> visit(DelimiterNode node) {
		_cost+=0;
		return new Pair<Double, Double>(0.0, 0.0);
	}

}
