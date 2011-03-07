package eu.stratosphere.simple.jaql;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntStack;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.FixedRecordExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.MathExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunction;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.io.AbstractHandleFn;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.path.PathReturn;

import eu.stratosphere.dag.DirectedAcyclicGraphPrinter;
import eu.stratosphere.dag.Navigator;
import eu.stratosphere.reflect.TypeHandler;
import eu.stratosphere.reflect.TypeSpecificHandler;
import eu.stratosphere.simple.jaql.rewrite.RewriteEngine;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Condition.Combination;
import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.JsonPath.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.Mapping;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Plan;
import eu.stratosphere.sopremo.PlanCreator;
import eu.stratosphere.sopremo.Transformation;
import eu.stratosphere.sopremo.ValueTransformation;
import eu.stratosphere.sopremo.operator.Aggregation;
import eu.stratosphere.sopremo.operator.Join;
import eu.stratosphere.sopremo.operator.Projection;
import eu.stratosphere.sopremo.operator.Selection;
import eu.stratosphere.sopremo.operator.Sink;
import eu.stratosphere.sopremo.operator.Source;

public class JaqlPlanCreator extends PlanCreator {
	private static final ExprNavigator EXPR_NAVIGATOR = new ExprNavigator();

	@Override
	public Plan getPlan(InputStream stream) {
		Jaql jaql = new Jaql() {
			{
				this.rewriter = new RewriteEngine();
			}
		};
		// jaql.enableRewrite(false);
		jaql.setInput("test", stream);
		// jaql.setInput("test", new FileInputStream("scrub.jaql"));
		Expr expr;
		try {
			expr = jaql.expr();
			return this.convert(expr);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final class ExprNavigator implements Navigator<Expr> {
		@Override
		public Iterable<Expr> getConnectedNodes(Expr node) {
			return this.getChildren(node);
		}

		private List<Expr> getChildren(Expr node) {
			List<Expr> children = new ArrayList<Expr>();
			for (int index = 0; index < node.numChildren(); index++)
				children.add(node.child(index));
			return children;
		}
	}

	public static class ExprPrinter extends DirectedAcyclicGraphPrinter<Expr> {

		private static final class PartialExprNavigator implements Navigator<Expr> {
			@Override
			public Iterable<Expr> getConnectedNodes(Expr node) {
				return this.getChildren(node);
			}

			private List<Expr> getChildren(Expr node) {
				List<Expr> children = new ArrayList<Expr>();
				for (int index = 0; index < node.numChildren(); index++) {
					Expr child = node.child(index);
					if (!(child instanceof ConstExpr) && !(child instanceof VarExpr)
							&& !(child instanceof PathReturn) && !(child instanceof NameValueBinding)
						&& !(child instanceof PathExpr)
							&& !(child instanceof RecordExpr) && !(child instanceof CompareExpr)
						&& !(child instanceof CopyField)
							&& !(child instanceof FixedRecordExpr) && !(child instanceof CopyRecord)// && !(child
																									// instanceof
																									// BindingExpr)
						&& !(child instanceof PathFieldValue))
						children.add(node.child(index));
				}
				return children;
			}
		}

		public ExprPrinter(Expr expr) {
			super(new PartialExprNavigator(), expr);
		}
	}

	private Plan convert(Expr expr) {
		System.out.println(new ExprPrinter(expr).toString(new DirectedAcyclicGraphPrinter.NodePrinter<Expr>() {
			@Override
			public String toString(Expr node) {
				return node.getClass().getSimpleName() + " "
					+ node.toString().replaceAll("system::", "").replaceAll("\n",
						" ");
			}
		}, 30));
		List<Operator> operator = this.convertSubtree(expr);

		return new Plan(operator);
	}

	private static interface ExprConverter<I extends Expr> extends TypeHandler<I, List<Operator>> {
		public List<Operator> convertAll(I expr, List<Operator> childOperators);
	}

	private abstract static class SingleExprConverter<I extends Expr> implements ExprConverter<I> {
		public abstract Operator convert(I expr, List<Operator> childOperators);

		@Override
		public List<Operator> convertAll(I expr, List<Operator> childOperators) {
			return Arrays.asList(this.convert(expr, childOperators));
		}
	}

	private static interface CondConverter<I extends Expr> extends TypeHandler<I, Condition> {
		public Condition convert(I expr, List<Condition> childConditions);
	}

	private static interface PathConverter<I extends Expr> extends TypeHandler<I, JsonPath> {
		public JsonPath convert(I expr, List<JsonPath> childPaths);
	}

	private static interface TransformationConverter<I extends Expr> extends TypeHandler<I, Mapping> {
		public Mapping convert(I expr, List<Mapping> childTransformations);
	}

	private TypeSpecificHandler<Expr, List<Operator>, TypeHandler<Expr, List<Operator>>> operatorConverter = new TypeSpecificHandler<Expr, List<Operator>, TypeHandler<Expr, List<Operator>>>();

	private TypeSpecificHandler<Expr, Condition, TypeHandler<Expr, Condition>> condConverter = new TypeSpecificHandler<Expr, Condition, TypeHandler<Expr, Condition>>();

	private TypeSpecificHandler<Expr, JsonPath, TypeHandler<Expr, JsonPath>> pathConverter = new TypeSpecificHandler<Expr, JsonPath, TypeHandler<Expr, JsonPath>>();

	private TypeSpecificHandler<Expr, Mapping, TypeHandler<Expr, Mapping>> transformationConverter = new TypeSpecificHandler<Expr, Mapping, TypeHandler<Expr, Mapping>>();

	public JaqlPlanCreator() {
		this.registerConverters();
	}

	private void registerConverters() {
		this.initOperatorConverter();
		this.initCondConverter();
		this.initPathConverter();
		this.initTransformationConverter();
	}

	private void initTransformationConverter() {
		this.transformationConverter.register(RecordExpr.class, new TransformationConverter<RecordExpr>() {
			@Override
			public Transformation convert(RecordExpr expr, List<Mapping> childTransformations) {
				Transformation transformation = new Transformation();
				for (Mapping mapping : childTransformations)
					transformation.addMapping(mapping);
				return transformation;
			}
		});
		this.transformationConverter.register(NameValueBinding.class, new TransformationConverter<NameValueBinding>() {
			@Override
			public ValueTransformation convert(NameValueBinding expr, List<Mapping> childTransformations) {
				return new ValueTransformation(((ConstExpr) expr.nameExpr()).value.toString(), JaqlPlanCreator.this
					.parsePath(expr
						.valueExpr()));
			}
		});
		this.transformationConverter.register(CopyField.class, new TransformationConverter<CopyField>() {
			@Override
			public ValueTransformation convert(CopyField expr, List<Mapping> childTransformations) {
				String fieldName = ((ConstExpr) expr.nameExpr()).value.toString();
				JsonPath path = JaqlPlanCreator.this.parsePath(expr.recExpr());
				path.setSelector(new JsonPath.FieldAccess(fieldName));
				return new ValueTransformation(fieldName, path);
			}
		});
		this.transformationConverter.register(ArrayExpr.class, new TransformationConverter<ArrayExpr>() {
			@Override
			public Transformation convert(ArrayExpr expr, List<Mapping> childTransformations) {
				Transformation transformation = new Transformation();
				for (int index = 0; index < childTransformations.size(); index++) {
					transformation.addMapping(childTransformations.get(index));
					// Mapping mapping = childTransformations.get(index);
					// ((Transformation) mapping).setTarget(String.valueOf(index));
					// transformation.addMapping(new ValueTransformation(String.valueOf(index),
					// parsePath(expr.child(index))));
				}
				return transformation;
			}
		});
		this.transformationConverter.register(CopyRecord.class, new TransformationConverter<CopyRecord>() {
			@Override
			public ValueTransformation convert(CopyRecord expr, List<Mapping> childTransformations) {
				JsonPath path = JaqlPlanCreator.this.parsePath(expr.recExpr());
				return new ValueTransformation("", path);
			}
		});
	}

	private void initPathConverter() {
		this.pathConverter.register(PathExpr.class, new PathConverter<PathExpr>() {
			@Override
			public JsonPath convert(PathExpr expr, List<JsonPath> childPath) {
				for (int index = 0; index < childPath.size() - 1; index++)
					childPath.get(index).setSelector(childPath.get(index + 1));
				return childPath.get(0);
			}
		});
		this.pathConverter.register(VarExpr.class, new PathConverter<VarExpr>() {
			@Override
			public JsonPath convert(VarExpr expr, List<JsonPath> childPath) {
				return new JsonPath.IdentifierAccess(expr.var().taggedName());
			}
		});
		this.pathConverter.register(PathFieldValue.class, new PathConverter<PathFieldValue>() {
			@Override
			public JsonPath convert(PathFieldValue expr, List<JsonPath> childPath) {
				JsonPath.FieldAccess fieldAccess = new JsonPath.FieldAccess(((ConstExpr) expr.nameExpr()).value
					.toString());
				if (childPath.size() > 1)
					fieldAccess.setSelector(childPath.get(1));
				return fieldAccess;
			}
		});
		this.pathConverter.register(ConstExpr.class, new PathConverter<ConstExpr>() {
			@Override
			public JsonPath convert(ConstExpr expr, List<JsonPath> childPath) {
				return new JsonPath.Constant(expr.value);
			}
		});
		// function fall-back
		this.pathConverter.register(Expr.class, new PathConverter<Expr>() {
			@Override
			public JsonPath convert(Expr expr, List<JsonPath> childPaths) {
				if (!expr.getClass().getSimpleName().endsWith("Fn"))
					return null;
				BuiltInFunctionDescriptor d = BuiltInFunction.getDescriptor(expr.getClass());
				return new JsonPath.Function(d.getName(), childPaths.toArray(new JsonPath[childPaths.size()]));
			}
		});
		this.pathConverter.register(MathExpr.class, new PathConverter<MathExpr>() {
			private Field OpField;

			private ArithmeticOperator[] OperatorMapping = { ArithmeticOperator.PLUS, ArithmeticOperator.MINUS,
				ArithmeticOperator.MULTIPLY, ArithmeticOperator.DIVIDE };

			{
				try {
					this.OpField = MathExpr.class.getDeclaredField("op");
					this.OpField.setAccessible(true);
				} catch (Exception e) {
					throw new IllegalStateException("Cannot find op field", e);
				}
			}

			@Override
			public JsonPath convert(MathExpr expr, List<JsonPath> childConditions) {
				try {
					int op = (Integer) this.OpField.get(expr);
					return new JsonPath.Arithmetic(childConditions.get(0), this.OperatorMapping[op], childConditions
						.get(1));
				} catch (Exception e) {
					throw new IllegalArgumentException("Cannot parse " + expr, e);
				}
			}
		});
	}

	private void initCondConverter() {
		this.condConverter.register(CompareExpr.class, new CondConverter<CompareExpr>() {
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
			public Condition convert(CompareExpr expr, List<Condition> childConditions) {
				try {
					int op = (Integer) this.OpField.get(expr);
					return new Condition(new Comparison(JaqlPlanCreator.this.parsePath(expr.child(0)),
						this.OperatorMapping[op], JaqlPlanCreator.this.parsePath(expr
							.child(1))));
				} catch (Exception e) {
					throw new IllegalArgumentException("Cannot parse " + expr, e);
				}
			}
		});
	}

	private void initOperatorConverter() {
		this.operatorConverter.register(WriteFn.class, new SingleExprConverter<WriteFn>() {
			@Override
			public Operator convert(WriteFn expr, List<Operator> childOperators) {
				return new Sink("hdfs", ((AbstractHandleFn) expr.descriptor()).location().toString(), childOperators
					.get(0));
			}
		});
		this.operatorConverter.register(FilterExpr.class, new SingleExprConverter<FilterExpr>() {
			@Override
			public Operator convert(FilterExpr expr, List<Operator> childOperators) {
				return new Selection(JaqlPlanCreator.this.parseCondition(expr), childOperators.get(0));
			}
		});
		this.operatorConverter.register(TransformExpr.class, new SingleExprConverter<TransformExpr>() {
			@Override
			public Operator convert(TransformExpr expr, List<Operator> childOperators) {
				return new Projection(JaqlPlanCreator.this.parseTransformation(expr), childOperators.get(0));
			}
		});
		this.operatorConverter.register(JoinExpr.class, new SingleExprConverter<JoinExpr>() {
			@Override
			public Operator convert(JoinExpr expr, List<Operator> childOperators) {
				Expr optionsExpr = expr.optionsExpr();
				List<List<JsonPath>> onPaths = new ArrayList<List<JsonPath>>();
				for (int index = 0; index < expr.numBindings(); index++) {
					ArrayExpr onExpr = (ArrayExpr) expr.onExpr(index);
					ArrayList<JsonPath> onPath = new ArrayList<JsonPath>();
					for (int i = 0; i < onExpr.numChildren(); i++)
						onPath.add(parsePath(onExpr.child(i)));
					onPaths.add(onPath);
				}

				Condition condition = null;
				for (int index = 0; index < onPaths.get(0).size(); index++) {
					condition = new Condition(new Comparison(onPaths.get(0).get(index), BinaryOperator.EQUAL, onPaths
						.get(1).get(index)), Combination.AND, condition);
				}
				return new Join(parseTransformation(expr.collectExpr()), condition, childOperators);
			}
		});
		this.operatorConverter.register(GroupByExpr.class, new SingleExprConverter<GroupByExpr>() {
			@Override
			public Operator convert(GroupByExpr expr, List<Operator> childOperators) {
				if (childOperators.size() > 0)
					return new Aggregation(null, null, childOperators.get(0));
				return null;
			}
		});
		this.operatorConverter.register(ReadFn.class, new SingleExprConverter<ReadFn>() {
			@Override
			public Operator convert(ReadFn expr, List<Operator> childOperators) {
				return new Source("hdfs", ((AbstractHandleFn) expr.child(0)).location().toString());
			}
		});
		this.operatorConverter.register(BindingExpr.class, new ExprConverter<BindingExpr>() {
			@Override
			public List<Operator> convertAll(BindingExpr expr, List<Operator> childOperators) {
				if (childOperators.isEmpty())
					return childOperators;

				switch (expr.type) {
				case IN:
					JaqlPlanCreator.this.bindings.put(expr.var.taggedName(), childOperators.get(0));
					break;
				}
				return childOperators;
			}
		});
	}

	protected Transformation parseTransformation(Expr expr) {
		return (Transformation) this.transformationConverter.handleRecursively(EXPR_NAVIGATOR, expr);
	}

	protected Condition parseCondition(FilterExpr expr) {
		return this.condConverter.handleRecursively(EXPR_NAVIGATOR, expr);
	}

	private Map<String, Object> bindings = new HashMap<String, Object>();

	private List<Operator> convertSubtree(Expr expr) {
		return this.operatorConverter.handleRecursively(EXPR_NAVIGATOR, expr);
	}

	private JsonPath parsePath(Expr expr) {
		return this.pathConverter.handleRecursively(EXPR_NAVIGATOR, expr);
	}

}
