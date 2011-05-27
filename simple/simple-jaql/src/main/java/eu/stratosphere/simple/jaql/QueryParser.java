package eu.stratosphere.simple.jaql;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.FixedRecordExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.path.PathReturn;

import eu.stratosphere.dag.Navigator;
import eu.stratosphere.simple.jaql.rewrite.RewriteEngine;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.PlanCreator;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public class QueryParser extends PlanCreator {

	static class Binding {
		private Expr expr;

		private Object transformed;

		public Binding(Expr expr, Object transformed) {
			this.expr = expr;
			this.transformed = transformed;
		}

		public Expr getExpr() {
			return this.expr;
		}

		public Object getTransformed() {
			return this.transformed;
		}

		public void setExpr(Expr expr) {
			if (expr == null)
				throw new NullPointerException("expr must not be null");

			this.expr = expr;
		}

		public void setTransformed(Object transformed) {
			if (transformed == null)
				throw new NullPointerException("transformed must not be null");

			this.transformed = transformed;
		}

		@Override
		public String toString() {
			if (this.transformed != null)
				return this.transformed.toString();
			return this.expr.toString();
		}
	}

	private static final class PartialExprNavigator implements Navigator<Expr> {
		private List<Expr> getChildren(Expr node) {
			List<Expr> children = new ArrayList<Expr>();
			for (int index = 0; index < node.numChildren(); index++) {
				Expr child = node.child(index);
				if (!(child instanceof ConstExpr) && !(child instanceof VarExpr) && !(child instanceof PathReturn)
					&& !(child instanceof NameValueBinding) && !(child instanceof PathExpr)
					&& !(child instanceof RecordExpr) && !(child instanceof CompareExpr)
					&& !(child instanceof CopyField) && !(child instanceof FixedRecordExpr)
					&& !(child instanceof CopyRecord)// && !(child
														// instanceof
														// BindingExpr)
					&& !(child instanceof PathFieldValue))
					children.add(node.child(index));
			}
			return children;
		}

		@Override
		public Iterable<Expr> getConnectedNodes(Expr node) {
			return this.getChildren(node);
		}
	}

	BindingManager bindings = new BindingManager();

	Map<Expr, Operator> expressionToOperators = new IdentityHashMap<Expr, Operator>();

	Deque<List<Operator>> operatorInputs = new LinkedList<List<Operator>>();

	private EvaluableExpressionParser pathParser = new EvaluableExpressionParser(this);

	private OperatorParser operatorParser = new OperatorParser(this);

	private ConditionParser conditionParser = new ConditionParser(this);

	private ObjectCreationParser objectCreationParser = new ObjectCreationParser(this);

	private SopremoPlan convert(Expr expr) {
		// System.out.println(new ExprPrinter(expr).toString(new DirectedAcyclicGraphPrinter.NodePrinter<Expr>() {
		// @Override
		// public String toString(Expr node) {
		// return node.getClass().getSimpleName();
		// // return node.getClass().getSimpleName() + " "
		// // + node.toString().replaceAll("system::", "").replaceAll("\n",
		// // " ");
		// }
		// }, 30));
		Operator operator = this.parseOperator(expr);

		return new SopremoPlan(Arrays.asList(operator));
	}

	int findInputIndex(Operator input) {
		if (this.operatorInputs.isEmpty())
			return -1;
		Iterator<List<Operator>> iterator = this.operatorInputs.descendingIterator();
		while (iterator.hasNext()) {
			List<Operator> inputs = iterator.next();

			for (int index = 0; index < inputs.size(); index++)
				if (inputs.get(index) == input)
					return index;
		}
		return -1;
	}

	@Override
	public SopremoPlan getPlan(InputStream stream) {
		Expr expr = this.parseScript(stream);
		return expr == null ? null : this.convert(expr);
	}

	Condition parseCondition(FilterExpr expr) {
		return this.conditionParser.parse(expr);
	}

	Operator parseOperator(Expr expr) {
		return this.operatorParser.parse(expr);
	}

	EvaluableExpression parsePath(Expr expr) {
		return this.pathParser.parse(expr);
	}

	Expr parseScript(InputStream stream) {
		Jaql jaql = new Jaql() {
			{
				this.rewriter = new RewriteEngine();
			}
		};
		// jaql.enableRewrite(false);
		jaql.setInput("test", stream);
		// jaql.setInput("test", new FileInputStream("scrub.jaql"));
		Expr expr = null;
		try {
			expr = jaql.expr();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return expr;
	}

	EvaluableExpression parseObjectCreation(Expr expr) {
		return this.objectCreationParser.parse(expr);
	}

}
