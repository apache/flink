package eu.stratosphere.simple.jaql;

import java.util.ArrayList;
import java.util.List;

import com.ibm.jaql.lang.expr.core.Expr;

import eu.stratosphere.dag.Navigator;

final class ExprNavigator implements Navigator<Expr> {

	static final ExprNavigator INSTANCE = new ExprNavigator();

	private List<Expr> getChildren(Expr node) {
		List<Expr> children = new ArrayList<Expr>();
		for (int index = 0; index < node.numChildren(); index++)
			children.add(node.child(index));
		return children;
	}

	@Override
	public Iterable<Expr> getConnectedNodes(Expr node) {
		return this.getChildren(node);
	}
}