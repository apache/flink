package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.node.BigIntegerNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.NumericNode;

public final class NumberCoercer {
	private static final int NUMBER_TYPES_COUNT = NumberType.values().length;

	/**
	 * The default, stateless instance.
	 */
	public static final NumberCoercer INSTANCE = new NumberCoercer();

	private NumberType[][] typeCoerceMatrix = new NumberType[NUMBER_TYPES_COUNT][NUMBER_TYPES_COUNT];

	private static interface Coercer {
		public NumericNode coerce(NumericNode node);
	}

	private Map<NumberType, Coercer> coercers = new EnumMap<NumberType, Coercer>(NumberType.class);

	public NumberCoercer() {
		List<NumberType> widestTypes = Arrays.asList(NumberType.DOUBLE, NumberType.FLOAT, NumberType.BIG_DECIMAL,
			NumberType.BIG_INTEGER, NumberType.LONG, NumberType.INT);

		for (int leftIndex = 0; leftIndex < NUMBER_TYPES_COUNT; leftIndex++) {
			for (int rightIndex = 0; rightIndex < NUMBER_TYPES_COUNT; rightIndex++) {
				int coerceIndex = Math.min(widestTypes.indexOf(NumberType.values()[leftIndex]),
					widestTypes.indexOf(NumberType.values()[rightIndex]));
				typeCoerceMatrix[leftIndex][rightIndex] = widestTypes.get(coerceIndex);
			}
		}

		coercers.put(NumberType.INT, new Coercer() {
			@Override
			public NumericNode coerce(NumericNode node) {
				return IntNode.valueOf(node.getIntValue());
			}
		});
		coercers.put(NumberType.LONG, new Coercer() {
			@Override
			public NumericNode coerce(NumericNode node) {
				return LongNode.valueOf(node.getLongValue());
			}
		});
		coercers.put(NumberType.DOUBLE, new Coercer() {
			@Override
			public NumericNode coerce(NumericNode node) {
				return DoubleNode.valueOf(node.getDoubleValue());
			}
		});
		coercers.put(NumberType.FLOAT, new Coercer() {
			@Override
			public NumericNode coerce(NumericNode node) {
				return DoubleNode.valueOf(node.getDoubleValue());
			}
		});
		coercers.put(NumberType.BIG_INTEGER, new Coercer() {
			@Override
			public NumericNode coerce(NumericNode node) {
				return BigIntegerNode.valueOf(node.getBigIntegerValue());
			}
		});
		coercers.put(NumberType.BIG_DECIMAL, new Coercer() {
			@Override
			public NumericNode coerce(NumericNode node) {
				return DecimalNode.valueOf(node.getDecimalValue());
			}
		});
	}

	public NumberType getWiderType(NumberType leftType, NumberType rightType) {
		return typeCoerceMatrix[leftType.ordinal()][rightType.ordinal()];
	}

	public NumericNode coerce(NumericNode node, NumberType targetType) {
		return coercers.get(targetType).coerce(node);
	}
}
