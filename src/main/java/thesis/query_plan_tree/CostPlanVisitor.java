package thesis.query_plan_tree;

import thesis.task_tree.DelimiterNode;
import thesis.utilities.Pair;

public interface CostPlanVisitor {
	Pair<Double, Double> visit(JoinNode node);
	Pair<Double, Double> visit(ReceiveNode node);
	Pair<Double, Double> visit(SendNode node);
	Pair<Double, Double> visit(RelationNode node);
	Pair<Double, Double> visit(DelimiterNode node);
}
