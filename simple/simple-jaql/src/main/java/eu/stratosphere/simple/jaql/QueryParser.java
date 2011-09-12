package eu.stratosphere.simple.jaql;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FixedRecordExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.path.PathReturn;

import eu.stratosphere.simple.PlanCreator;
import eu.stratosphere.simple.jaql.SJaqlParser.script_return;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.util.dag.Navigator;

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

	@Override
	public SopremoPlan getPlan(InputStream stream) {
		try {
			return tryParse(stream);
		} catch (Exception e) {
			return null;
		}
	}

	public SopremoPlan tryParse(InputStream stream) throws IOException, RecognitionException {
		SJaqlLexer lexer = new SJaqlLexer(new ANTLRInputStream(stream));
		CommonTokenStream tokens = new CommonTokenStream();
		tokens.setTokenSource(lexer);
		SJaqlParser parser = new SJaqlParser(tokens);
		parser.setTreeAdaptor(new SopremoTreeAdaptor());
		return parser.parse();
	}
}
