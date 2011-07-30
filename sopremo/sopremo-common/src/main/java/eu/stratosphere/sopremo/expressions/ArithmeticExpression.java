package eu.stratosphere.sopremo.expressions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.EnumMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.node.BigIntegerNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.NumberCoercer;

/**
 * Represents all basic arithmetic expressions covering the addition, subtraction, division, and multiplication for
 * various types of numbers.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.NUMBER, minNodes = 2, maxNodes = 2, transitive = true)
public class ArithmeticExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9103414139002479181L;

	private ArithmeticExpression.ArithmeticOperator operator;

	private EvaluationExpression op1, op2;

	/**
	 * Initializes Arithmetic with two {@link EvaluationExpression}s and an {@link ArithmeticOperator} in infix
	 * notation.
	 * 
	 * @param op1
	 *        the first operand
	 * @param operator
	 *        the operator
	 * @param op2
	 *        the second operand
	 */
	public ArithmeticExpression(EvaluationExpression op1, ArithmeticOperator operator, EvaluationExpression op2) {
		this.operator = operator;
		this.op1 = op1;
		this.op2 = op2;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.op1.equals(((ArithmeticExpression) obj).op1)
			&& this.operator.equals(((ArithmeticExpression) obj).operator)
			&& this.op2.equals(((ArithmeticExpression) obj).op2);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return this.operator.evaluate((NumericNode) this.op1.evaluate(node, context),
			(NumericNode) this.op2.evaluate(node, context));
	}

	@Override
	public int hashCode() {
		return ((59 + this.op1.hashCode()) * 59 + this.operator.hashCode()) * 59 + this.op2.hashCode();
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.op1);
		builder.append(' ');
		builder.append(this.operator);
		builder.append(' ');
		builder.append(this.op2);
	}

	/**
	 * Closed set of basic arithmetic operators.
	 * 
	 * @author Arvid Heise
	 */
	public static enum ArithmeticOperator {
		/**
		 * Addition
		 */
		ADDITION("+", new IntegerEvaluator() {
			@Override
			protected int evaluate(int left, int right) {
				return left + right;
			}
		}, new LongEvaluator() {
			@Override
			protected long evaluate(long left, long right) {
				return left + right;
			}
		}, new DoubleEvaluator() {
			@Override
			protected double evaluate(double left, double right) {
				return left + right;
			}
		}, new BigIntegerEvaluator() {
			@Override
			protected BigInteger evaluate(BigInteger left, BigInteger right) {
				return left.add(right);
			}
		}, new BigDecimalEvaluator() {
			@Override
			protected BigDecimal evaluate(BigDecimal left, BigDecimal right) {
				return left.add(right);
			}
		}),
		/**
		 * Subtraction
		 */
		SUBTRACTION("-", new IntegerEvaluator() {
			@Override
			protected int evaluate(int left, int right) {
				return left - right;
			}
		}, new LongEvaluator() {
			@Override
			protected long evaluate(long left, long right) {
				return left - right;
			}
		}, new DoubleEvaluator() {
			@Override
			protected double evaluate(double left, double right) {
				return left - right;
			}
		}, new BigIntegerEvaluator() {
			@Override
			protected BigInteger evaluate(BigInteger left, BigInteger right) {
				return left.subtract(right);
			}
		}, new BigDecimalEvaluator() {
			@Override
			protected BigDecimal evaluate(BigDecimal left, BigDecimal right) {
				return left.subtract(right);
			}
		}),
		/**
		 * Multiplication
		 */
		MULTIPLICATION("*", new IntegerEvaluator() {
			@Override
			protected int evaluate(int left, int right) {
				return left * right;
			}
		}, new LongEvaluator() {
			@Override
			protected long evaluate(long left, long right) {
				return left * right;
			}
		}, new DoubleEvaluator() {
			@Override
			protected double evaluate(double left, double right) {
				return left * right;
			}
		}, new BigIntegerEvaluator() {
			@Override
			protected BigInteger evaluate(BigInteger left, BigInteger right) {
				return left.multiply(right);
			}
		}, new BigDecimalEvaluator() {
			@Override
			protected BigDecimal evaluate(BigDecimal left, BigDecimal right) {
				return left.multiply(right);
			}
		}),
		/**
		 * Division
		 */
		DIVISION("/", DivisionEvaluator.INSTANCE, DivisionEvaluator.INSTANCE, new DoubleEvaluator() {
			@Override
			protected double evaluate(double left, double right) {
				return left / right;
			}
		}, DivisionEvaluator.INSTANCE, DivisionEvaluator.INSTANCE);

		private final String sign;

		private Map<NumberType, NumberEvaluator> typeEvaluators = new EnumMap<NumberType, NumberEvaluator>(
			NumberType.class);

		private ArithmeticOperator(String sign, NumberEvaluator integerEvaluator, NumberEvaluator longEvaluator,
				NumberEvaluator doubleEvaluator, NumberEvaluator bigIntegerEvaluator,
				NumberEvaluator bigDecimalEvaluator) {
			this.sign = sign;
			this.typeEvaluators.put(NumberType.INT, integerEvaluator);
			this.typeEvaluators.put(NumberType.LONG, longEvaluator);
			this.typeEvaluators.put(NumberType.FLOAT, doubleEvaluator);
			this.typeEvaluators.put(NumberType.DOUBLE, doubleEvaluator);
			this.typeEvaluators.put(NumberType.BIG_INTEGER, bigIntegerEvaluator);
			this.typeEvaluators.put(NumberType.BIG_DECIMAL, bigDecimalEvaluator);
		}

		/**
		 * Performs the binary operation on the two operands after coercing both values to a common number type.
		 * 
		 * @param left
		 *        the left operand
		 * @param right
		 *        the right operand
		 * @return the result of the operation
		 */
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			NumberType widerType = NumberCoercer.INSTANCE.getWiderType(left.getNumberType(), right.getNumberType());
			return this.typeEvaluators.get(widerType).evaluate(left, right);
		}

		@Override
		public String toString() {
			return this.sign;
		}
	}

	private abstract static class BigDecimalEvaluator implements NumberEvaluator {
		protected abstract BigDecimal evaluate(BigDecimal left, BigDecimal right);

		@Override
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			return DecimalNode.valueOf(this.evaluate(left.getDecimalValue(), right.getDecimalValue()));
		}
	}

	private abstract static class BigIntegerEvaluator implements NumberEvaluator {
		protected abstract BigInteger evaluate(BigInteger left, BigInteger right);

		@Override
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			return BigIntegerNode.valueOf(this.evaluate(left.getBigIntegerValue(), right.getBigIntegerValue()));
		}
	}

	/**
	 * Taken from Groovy's org.codehaus.groovy.runtime.typehandling.BigDecimalMath
	 * 
	 * @author Arvid Heise
	 */
	static class DivisionEvaluator implements NumberEvaluator {
		private static final DivisionEvaluator INSTANCE = new DivisionEvaluator();

		// This is an arbitrary value, picked as a reasonable choice for a precision
		// for typical user math when a non-terminating result would otherwise occur.
		public static final int DIVISION_EXTRA_PRECISION = 10;

		// This is an arbitrary value, picked as a reasonable choice for a rounding point
		// for typical user math.
		public static final int DIVISION_MIN_SCALE = 10;

		@Override
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			return DecimalNode.valueOf(divideImpl(left.getDecimalValue(), right.getDecimalValue()));
		}

		public static BigDecimal divideImpl(BigDecimal bigLeft, BigDecimal bigRight) {
			try {
				return bigLeft.divide(bigRight);
			} catch (ArithmeticException e) {
				// set a DEFAULT precision if otherwise non-terminating
				int precision = Math.max(bigLeft.precision(), bigRight.precision()) + DIVISION_EXTRA_PRECISION;
				BigDecimal result = bigLeft.divide(bigRight, new MathContext(precision));
				int scale = Math.max(Math.max(bigLeft.scale(), bigRight.scale()), DIVISION_MIN_SCALE);
				if (result.scale() > scale)
					result = result.setScale(scale, BigDecimal.ROUND_HALF_UP);
				return result;
			}
		}
	}

	private abstract static class DoubleEvaluator implements NumberEvaluator {
		protected abstract double evaluate(double left, double right);

		@Override
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			return DoubleNode.valueOf(this.evaluate(left.getDoubleValue(), right.getDoubleValue()));
		}
	}

	private abstract static class IntegerEvaluator implements NumberEvaluator {
		protected abstract int evaluate(int left, int right);

		@Override
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			return IntNode.valueOf(this.evaluate(left.getIntValue(), right.getIntValue()));
		}
	}

	private abstract static class LongEvaluator implements NumberEvaluator {
		protected abstract long evaluate(long left, long right);

		@Override
		public NumericNode evaluate(NumericNode left, NumericNode right) {
			return LongNode.valueOf(this.evaluate(left.getLongValue(), right.getLongValue()));
		}
	}

	private static interface NumberEvaluator {
		public NumericNode evaluate(NumericNode left, NumericNode right);
	}
}