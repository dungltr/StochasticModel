package thesis.task_tree;

import thesis.query_plan_tree.JoinNode;
import thesis.query_plan_tree.ReceiveNode;
import thesis.query_plan_tree.RelationNode;
import thesis.query_plan_tree.SendNode;

public interface TreeVisitor {
	void visit(JoinNode node);
	void visit(ReceiveNode node);
	void visit(SendNode node);
	void visit(RelationNode node);
	void visit(DelimiterNode node);
}
