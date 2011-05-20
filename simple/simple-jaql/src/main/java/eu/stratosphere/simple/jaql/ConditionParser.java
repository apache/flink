package eu.stratosphere.simple.jaql;

import java.lang.reflect.Field;
import java.util.List;

import com.ibm.jaql.lang.expr.core.AndExpr;
import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.OrExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;

import eu.stratosphere.reflect.TypeHandler;
import eu.stratosphere.reflect.TypeSpecificHandler;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.Condition.Combination;
import eu.stratosphere.sopremo.expressions.UnaryExpression;

class ConditionParser implements JaqlToSopremoParser<Condition> {

	private static final class AndConverter implements CondConverter<AndExpr> {
		@Override
		public BooleanExpression convert(AndExpr expr, List<BooleanExpression> childConditions) {
			return Condition.valueOf(childConditions, Combination.AND);
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
		public BooleanExpression convert(CompareExpr expr, List<BooleanExpression> childConditions) {
			try {
				int op = (Integer) this.OpField.get(expr);
				return new Comparison(ConditionParser.this.queryParser.parsePath(expr.child(0)),
					this.OperatorMapping[op], ConditionParser.this.queryParser.parsePath(expr.child(1)));
			} catch (Exception e) {
				throw new IllegalArgumentException("Cannot parse " + expr, e);
			}
		}
	}

	private static interface CondConverter<I extends Expr> extends TypeHandler<I, BooleanExpression> {
		public BooleanExpression convert(I expr, List<BooleanExpression> childConditions);
	}

	private static final class OrConverter implements CondConverter<OrExpr> {
		@Override
		public BooleanExpression convert(OrExpr expr, List<BooleanExpression> childConditions) {
			return Condition.valueOf(childConditions, Combination.OR);
		}
	}

	private final class UnaryExpressionConverter implements CondConverter<PathExpr> {
		@Override
		public BooleanExpression convert(PathExpr expr, List<BooleanExpression> childConditions) {
			return new UnaryExpression(ConditionParser.this.queryParser.parsePath(expr));
		}
	}

	private QueryParser queryParser;

	private TypeSpecificHandler<Expr, BooleanExpression, TypeHandler<Expr, BooleanExpression>> condConverter = new TypeSpecificHandler<Expr, BooleanExpression, TypeHandler<Expr, BooleanExpression>>();

	@SuppressWarnings("unchecked")
	public ConditionParser(QueryParser queryParser) {
		this.queryParser = queryParser;
		this.condConverter.registerAll(new OrConverter(), new AndConverter(), new UnaryExpressionConverter(),
			new ComparisonConverter());
	}

	@Override
	public Condition parse(Expr expr) {
		return Condition.valueOf(this.condConverter.handleRecursively(ExprNavigator.INSTANCE, expr));
	}
}
