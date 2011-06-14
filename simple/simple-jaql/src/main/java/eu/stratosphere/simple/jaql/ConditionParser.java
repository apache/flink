package eu.stratosphere.simple.jaql;

import java.lang.reflect.Field;
import java.util.List;

import com.ibm.jaql.lang.expr.core.AndExpr;
import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.OrExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;

import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression.Combination;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.util.dag.converter.GraphConverter;
import eu.stratosphere.util.dag.converter.NodeConverter;

class ConditionParser implements JaqlToSopremoParser<ConditionalExpression> {

	private static final class AndConverter implements CondConverter<AndExpr> {
		@Override
		public BooleanExpression convertNode(AndExpr expr, List<BooleanExpression> childConditions) {
			return ConditionalExpression.valueOf(childConditions, Combination.AND);
		}
	}

	private final class ComparisonConverter implements CondConverter<CompareExpr> {
		private Field OpField;

		private BinaryOperator[] OperatorMapping = { BinaryOperator.EQUAL, BinaryOperator.NOT_EQUAL,
			BinaryOperator.LESS, BinaryOperator.LESS_EQUAL, BinaryOperator.GREATER, BinaryOperator.GREATER_EQUAL };
		{
			try {
				this.OpField = CompareExpr.class.getDeclaredField("op");
				this.OpField.setAccessible(true);
			} catch (Exception e) {
				throw new IllegalStateException("Cannot find op field", e);
			}
		}

		@Override
		public BooleanExpression convertNode(CompareExpr expr, List<BooleanExpression> childConditions) {
			try {
				int op = (Integer) this.OpField.get(expr);
				return new ComparativeExpression(ConditionParser.this.queryParser.parsePath(expr.child(0)),
					this.OperatorMapping[op], ConditionParser.this.queryParser.parsePath(expr.child(1)));
			} catch (Exception e) {
				throw new IllegalArgumentException("Cannot parse " + expr, e);
			}
		}
	}

	private static interface CondConverter<I extends Expr> extends NodeConverter<I, BooleanExpression> {
	}

	private static final class OrConverter implements CondConverter<OrExpr> {
		@Override
		public BooleanExpression convertNode(OrExpr expr, List<BooleanExpression> childConditions) {
			return ConditionalExpression.valueOf(childConditions, Combination.OR);
		}
	}

	private final class UnaryExpressionConverter implements CondConverter<PathExpr> {
		@Override
		public BooleanExpression convertNode(PathExpr expr, List<BooleanExpression> childConditions) {
			return new UnaryExpression(ConditionParser.this.queryParser.parsePath(expr));
		}
	}

	private QueryParser queryParser;

	private GraphConverter<Expr, BooleanExpression> condConverter = new GraphConverter<Expr, BooleanExpression>();

	@SuppressWarnings("unchecked")
	public ConditionParser(QueryParser queryParser) {
		this.queryParser = queryParser;
		this.condConverter.registerAll(new OrConverter(), new AndConverter(), new UnaryExpressionConverter(),
			new ComparisonConverter());
	}

	@Override
	public ConditionalExpression parse(Expr expr) {
		return ConditionalExpression.valueOf(this.condConverter.convertGraph(expr, ExprNavigator.INSTANCE));
	}
}
