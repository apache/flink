package eu.stratosphere.simple.jaql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.io.AbstractHandleFn;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;

import eu.stratosphere.dag.converter.GraphConversionListener;
import eu.stratosphere.dag.converter.GraphConverter;
import eu.stratosphere.dag.converter.NodeConverter;
import eu.stratosphere.simple.jaql.QueryParser.Binding;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.base.Aggregation;
import eu.stratosphere.sopremo.base.DataType;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.Condition.Combination;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.Path;

class OperatorParser implements JaqlToSopremoParser<Operator> {
	private final class BindingScoper implements GraphConversionListener<Expr, Operator> {
		@Override
		public void afterNodeConversion(Expr in, List<Operator> children, Operator out) {
			OperatorParser.this.queryParser.expressionToOperators.put(in, out);
		}

		@Override
		public void afterSubgraphConversion(Expr in, Operator out) {
			if (!(in instanceof BindingExpr))
				OperatorParser.this.queryParser.bindings.removeScope();
		}

		@Override
		public void beforeNodeConversion(Expr in, List<Operator> children) {
			OperatorParser.this.queryParser.operatorInputs.addLast(children);
		}

		@Override
		public void beforeSubgraphConversion(Expr in) {
			if (!(in instanceof BindingExpr))
				OperatorParser.this.queryParser.bindings.addScope();
		}
	}

	private final class AdhocSourceConverter implements OpConverter<ArrayExpr> {
		@Override
		public Operator convertNode(ArrayExpr expr, List<Operator> childOperators) {
			if (expr.parent() instanceof BindingExpr)
				return new Source(OperatorParser.this.queryParser.parsePath(expr));
			return null;
		}
	}

	private final class SourceConverter implements OpConverter<ReadFn> {
		@Override
		public Operator convertNode(ReadFn expr, List<Operator> childOperators) {
			return new Source(DataType.HDFS,
				((ConstExpr) ((AbstractHandleFn) expr.child(0)).location()).value.toString());
		}
	}

	private final class AggregationConverter implements OpConverter<GroupByExpr> {
		@Override
		public Operator convertNode(GroupByExpr expr, List<Operator> childOperators) {
			int n = expr.numInputs();
			List<EvaluableExpression> groupStatements = new ArrayList<EvaluableExpression>();
			for (int index = 0; index < n; index++) {
				OperatorParser.this.queryParser.bindings.set("$", new Binding(null, new Input(index)));
				EvaluableExpression groupStatement = OperatorParser.this.queryParser.parsePath(expr.byBinding().child(
					index));
				if (groupStatement != null) {
					OperatorParser.this.queryParser.bindings.set(expr.getAsVar(index).taggedName(), new Binding(null,
						new Input(index)));
					groupStatements.add(groupStatement);
				}
				if (index > 0)
					childOperators.add(OperatorParser.this.queryParser.parseOperator(expr.inBinding().child(index)));
			}
			EvaluableExpression collectTransformation = OperatorParser.this.queryParser
				.parseObjectCreation(((ArrayExpr) expr.collectExpr()).child(0));
			return new Aggregation(collectTransformation, groupStatements.isEmpty() ? Aggregation.NO_GROUPING
				: groupStatements, childOperators);
		}
	}

	private final class JoinConverter implements OpConverter<JoinExpr> {
		private List<String> inputAliases = new ArrayList<String>();

		@Override
		public Operator convertNode(JoinExpr expr, List<Operator> childOperators) {
			Condition condition = this.parseCondition(expr);

			if (this.inputAliases.size() < childOperators.size())
				this.inputAliases.addAll(Arrays.asList(new String[childOperators.size() - this.inputAliases.size()]));
			for (int index = 0; index < childOperators.size(); index++)
				childOperators.set(index, this.withoutNameBinding(childOperators.get(index), index));

			Join join = new Join(this.parseTransformation(expr, childOperators.size()), condition, childOperators);
			for (int index = 0; index < expr.numBindings(); index++)
				if (expr.binding(index).preserve)
					join.withOuterJoin(childOperators.get(index));
			return join;
		}

		private Condition parseCondition(JoinExpr expr) {
			List<List<Path>> onPaths = new ArrayList<List<Path>>();
			for (int index = 0; index < expr.numBindings(); index++) {
				Expr onExpr = expr.onExpr(index);
				ArrayList<Path> onPath = new ArrayList<Path>();
				if (onExpr instanceof ArrayExpr)
					for (int i = 0; i < onExpr.numChildren(); i++)
						onPath.add((Path) OperatorParser.this.queryParser.parsePath(onExpr.child(i)));
				else
					onPath.add((Path) OperatorParser.this.queryParser.parsePath(onExpr));

				for (Path path : onPath)
					// leave out intermediate variable
					path.getFragments().remove(1);
				onPaths.add(onPath);
			}

			List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
			for (int index = 0; index < onPaths.get(0).size(); index++)
				expressions.add(new Comparison(onPaths.get(0).get(index), BinaryOperator.EQUAL, onPaths.get(1).get(
					index)));
			return Condition.valueOf(expressions, Combination.AND);
		}

		private ObjectCreation parseTransformation(JoinExpr expr, int numInputs) {
			OperatorParser.this.queryParser.bindings.set("$", new Binding(null, new Input(0)));
			ObjectCreation transformation = (ObjectCreation) OperatorParser.this.queryParser
				.parseObjectCreation(((ForExpr) expr.parent().parent()).collectExpr());
			for (int inputIndex = 0; inputIndex < numInputs; inputIndex++) {
				Path alias = new Path(new Input(0), new FieldAccess(this.inputAliases.get(inputIndex)));
				transformation.replace(alias, new Path(new Input(inputIndex)));
			}
			return transformation;
		}

		private Operator withoutNameBinding(Operator operator, int inputIndex) {
			if (operator instanceof Projection && operator.getEvaluableExpression() instanceof ObjectCreation) {
				ObjectCreation objectCreation = (ObjectCreation) operator.getEvaluableExpression();
				if (objectCreation.getMappingSize() == 1
					&& objectCreation.getMapping(0).getExpression() instanceof Input) {
					Operator coreInput = operator.getInputOperators().get(
						((Input) objectCreation.getMapping(0).getExpression()).getIndex());
					Iterator<Entry<String, Binding>> iterator = OperatorParser.this.queryParser.bindings.getAll()
						.entrySet().iterator();

					// replace bindings to core input
					while (iterator.hasNext()) {
						Binding binding = iterator.next().getValue();
						if (binding.getTransformed() == operator)
							binding.setTransformed(coreInput);
					}
					this.inputAliases.set(inputIndex, objectCreation.getMapping(0).getTarget());
					return coreInput;
				}
			}
			return operator;
		}
	}

	private final class TransformationConverter implements OpConverter<TransformExpr> {
		@Override
		public Operator convertNode(TransformExpr expr, List<Operator> childOperators) {
			return new Projection(OperatorParser.this.queryParser.parseObjectCreation(expr), childOperators.get(0));
		}
	}

	private final class SelectionConverter implements OpConverter<FilterExpr> {
		@Override
		public Operator convertNode(FilterExpr expr, List<Operator> childOperators) {
			return new Selection(OperatorParser.this.queryParser.parseCondition(expr), childOperators.get(0));
		}
	}

	private final class SinkConverter implements OpConverter<WriteFn> {
		@Override
		public Operator convertNode(WriteFn expr, List<Operator> childOperators) {
			return new Sink(DataType.HDFS, ((AbstractHandleFn) expr.descriptor()).location().toString(),
				childOperators.get(0));
		}
	}

	final class BindingExtractor<Output> implements NodeConverter<BindingExpr, Output> {

		private BindingExtractor() {
		}

		@Override
		public Output convertNode(BindingExpr expr, List<Output> children) {
			// System.out.println(expr);
			if (children.isEmpty() && expr.numChildren() == 0)
				return null;

			if (expr.var == Var.UNUSED)
				return null;

			Expr valueExpr;
			switch (expr.type) {
			case IN:
				valueExpr = expr.inExpr();
				break;
			case EQ:
				valueExpr = expr.eqExpr();
				break;
			default:
				System.out.println("unhandled binding");
				return null;
			}

			Object transformedExpr = null;
			if (!children.isEmpty())
				transformedExpr = children.get(0);
			else if (valueExpr instanceof VarExpr)
				transformedExpr = OperatorParser.this.queryParser.bindings.get(valueExpr.toString()).getTransformed();
			else
				transformedExpr = OperatorParser.this.queryParser.parseObjectCreation(valueExpr);
			// if (transformedExpr == null)
			// transformedExpr = JaqlPlanCreator.this.parsePath(valueExpr);
			OperatorParser.this.queryParser.bindings
				.set(expr.var.taggedName(), new Binding(valueExpr, transformedExpr));

			return null;
		}
	}

	private static interface OpConverter<I extends Expr> extends NodeConverter<I, Operator> {
	}

	QueryParser queryParser;

	private GraphConverter<Expr, Operator> operatorConverter = new GraphConverter<Expr, Operator>();

	@SuppressWarnings("unchecked")
	public OperatorParser(QueryParser queryParser) {
		this.queryParser = queryParser;
		this.operatorConverter.addListener(new BindingScoper());
		this.operatorConverter.registerAll(new SinkConverter(), new SelectionConverter(),
			new TransformationConverter(), new JoinConverter(), new AggregationConverter(), new SourceConverter(),
			new AdhocSourceConverter(), new BindingExtractor<Operator>());
	}

	@Override
	public Operator parse(Expr expr) {
		return this.operatorConverter.convertGraph(expr, ExprNavigator.INSTANCE);
	}

}
