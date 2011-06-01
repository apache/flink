package eu.stratosphere.sopremo.expressions;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.node.BigIntegerNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public class Arithmetic extends EvaluableExpression {
	private Arithmetic.ArithmeticOperator operator;

	private Evaluable op1, op2;

	public Arithmetic(Evaluable op1, Arithmetic.ArithmeticOperator operator, Evaluable op2) {
		this.operator = operator;
		this.op1 = op1;
		this.op2 = op2;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.op1.equals(((Arithmetic) obj).op1) && this.operator.equals(((Arithmetic) obj).operator)
			&& this.op2.equals(((Arithmetic) obj).op2);
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

	public static enum ArithmeticOperator {
		PLUS("+") {

			@Override
			BigDecimal evaluateBigDecimal(BigDecimal value1, BigDecimal value2) {
				return value1.add(value2);
			}

			@Override
			BigInteger evaluateBigInteger(BigInteger value1, BigInteger value2) {
				return value1.add(value2);
			}

			@Override
			double evaluateDouble(double value1, double value2) {
				return value1 + value2;
			}

			@Override
			int evaluateInt(int value1, int value2) {
				return value1 + value2;
			}

			@Override
			long evaluateLong(long value1, long value2) {
				return value1 + value2;
			}

		},
		MINUS("-") {
			@Override
			BigDecimal evaluateBigDecimal(BigDecimal value1, BigDecimal value2) {
				return value1.subtract(value2);
			}

			@Override
			BigInteger evaluateBigInteger(BigInteger value1, BigInteger value2) {
				return value1.subtract(value2);
			}

			@Override
			double evaluateDouble(double value1, double value2) {
				return value1 - value2;
			}

			@Override
			int evaluateInt(int value1, int value2) {
				return value1 - value2;
			}

			@Override
			long evaluateLong(long value1, long value2) {
				return value1 - value2;
			}

		},
		MULTIPLY("*") {
			@Override
			BigDecimal evaluateBigDecimal(BigDecimal value1, BigDecimal value2) {
				return value1.multiply(value2);
			}

			@Override
			BigInteger evaluateBigInteger(BigInteger value1, BigInteger value2) {
				return value1.multiply(value2);
			}

			@Override
			double evaluateDouble(double value1, double value2) {
				return value1 * value2;
			}

			@Override
			int evaluateInt(int value1, int value2) {
				return value1 * value2;
			}

			@Override
			long evaluateLong(long value1, long value2) {
				return value1 * value2;
			}

		},
		DIVIDE("/") {
			@Override
			BigDecimal evaluateBigDecimal(BigDecimal value1, BigDecimal value2) {
				return value1.divide(value2);
			}

			@Override
			BigInteger evaluateBigInteger(BigInteger value1, BigInteger value2) {
				return value1.divide(value2);
			}

			@Override
			double evaluateDouble(double value1, double value2) {
				return value1 / value2;
			}

			@Override
			int evaluateInt(int value1, int value2) {
				return value1 / value2;
			}

			@Override
			long evaluateLong(long value1, long value2) {
				return value1 / value2;
			}

		};

		private final String sign;

		ArithmeticOperator(String sign) {
			this.sign = sign;
		}

		public NumericNode evaluate(NumericNode a, NumericNode b) {
			switch (this.getWiderType(a.getNumberType(), b.getNumberType())) {
			case BIG_DECIMAL:
				return DecimalNode.valueOf(this.evaluateBigDecimal(a.getDecimalValue(), b.getDecimalValue()));
			case DOUBLE:
			case FLOAT:
				return DoubleNode.valueOf(this.evaluateDouble(a.getDoubleValue(), b.getDoubleValue()));
			case BIG_INTEGER:
				return BigIntegerNode.valueOf(this.evaluateBigInteger(a.getBigIntegerValue(), b.getBigIntegerValue()));
			case LONG:
				return LongNode.valueOf(this.evaluateLong(a.getLongValue(), b.getLongValue()));
			default:
				return IntNode.valueOf(this.evaluateInt(a.getIntValue(), b.getIntValue()));
			}
		}

		abstract BigDecimal evaluateBigDecimal(BigDecimal value1, BigDecimal value2);

		abstract BigInteger evaluateBigInteger(BigInteger value1, BigInteger value2);

		abstract double evaluateDouble(double value1, double value2);

		abstract int evaluateInt(int value1, int value2);

		abstract long evaluateLong(long value1, long value2);

		private NumberType getWiderType(NumberType numberType, NumberType numberType2) {
			// TODO:
			return NumberType.values()[Math.max(numberType.ordinal(), numberType2.ordinal())];
		}

		@Override
		public String toString() {
			return this.sign;
		}
	}
}