package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
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
	 * The default instance.
	 */
	public static final NumberCoercer INSTANCE = new NumberCoercer();

	private NumberType[][] typeCoerceMatrix = new NumberType[NUMBER_TYPES_COUNT][NUMBER_TYPES_COUNT];

	private Map<NumberType, Coercer> coercers = new EnumMap<NumberType, Coercer>(NumberType.class);

	private Map<Class<? extends JsonNode>, Coercer> classCoercers = new IdentityHashMap<Class<? extends JsonNode>, Coercer>();

	private Map<NumberType, Class<? extends JsonNode>> implementationTypes = new EnumMap<NumberType, Class<? extends JsonNode>>(
		NumberType.class);

	private Map<Class<? extends JsonNode>, NumberType> numberTypes = new IdentityHashMap<Class<? extends JsonNode>, NumberType>();

	Map<Class<? extends JsonNode>, Coercer> getClassCoercers() {
		return classCoercers;
	}
	
	public NumberCoercer() {
		List<NumberType> widestTypes = Arrays.asList(NumberType.DOUBLE, NumberType.FLOAT, NumberType.BIG_DECIMAL,
			NumberType.BIG_INTEGER, NumberType.LONG, NumberType.INT);
		@SuppressWarnings("unchecked")
		Class<? extends JsonNode>[] types = (Class<? extends JsonNode>[]) new Class<?>[] { DoubleNode.class,
			DoubleNode.class, DecimalNode.class, BigIntegerNode.class, LongNode.class, IntNode.class };

		for (int index = 0; index < types.length; index++) {
			this.implementationTypes.put(widestTypes.get(index), types[index]);
			this.numberTypes.put(types[index], widestTypes.get(index));
		}

		for (int leftIndex = 0; leftIndex < NUMBER_TYPES_COUNT; leftIndex++)
			for (int rightIndex = 0; rightIndex < NUMBER_TYPES_COUNT; rightIndex++) {
				int coerceIndex = Math.min(widestTypes.indexOf(NumberType.values()[leftIndex]),
					widestTypes.indexOf(NumberType.values()[rightIndex]));
				this.typeCoerceMatrix[leftIndex][rightIndex] = widestTypes.get(coerceIndex);
			}

		this.coercers.put(NumberType.INT, new Coercer() {
			@Override
			public NumericNode coerce(JsonNode node) {
				return IntNode.valueOf(node.getIntValue());
			}
		});
		this.coercers.put(NumberType.LONG, new Coercer() {
			@Override
			public NumericNode coerce(JsonNode node) {
				return LongNode.valueOf(node.getLongValue());
			}
		});
		this.coercers.put(NumberType.DOUBLE, new Coercer() {
			@Override
			public NumericNode coerce(JsonNode node) {
				return DoubleNode.valueOf(node.getDoubleValue());
			}
		});
		this.coercers.put(NumberType.FLOAT, new Coercer() {
			@Override
			public NumericNode coerce(JsonNode node) {
				return DoubleNode.valueOf(node.getDoubleValue());
			}
		});
		this.coercers.put(NumberType.BIG_INTEGER, new Coercer() {
			@Override
			public NumericNode coerce(JsonNode node) {
				return BigIntegerNode.valueOf(node.getBigIntegerValue());
			}
		});
		this.coercers.put(NumberType.BIG_DECIMAL, new Coercer() {
			@Override
			public NumericNode coerce(JsonNode node) {
				return DecimalNode.valueOf(node.getDecimalValue());
			}
		});

		for (Entry<NumberType, Coercer> entry : this.coercers.entrySet())
			this.classCoercers.put(this.implementationTypes.get(entry.getKey()), entry.getValue());
	}

	public NumericNode coerce(NumericNode node, NumberType targetType) {
		return this.coercers.get(targetType).coerce(node);
	}

	public <T extends NumericNode> T coerce(NumericNode node, Class<T> targetType) {
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	JsonNode coerceGeneric(JsonNode node, NumberType targetType) {
		return this.coercers.get(targetType).coerce(node);
	}

	<T extends JsonNode> T coerceGeneric(JsonNode node, Class<T> targetType) {
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	public NumberType getWiderType(NumberType leftType, NumberType rightType) {
		return this.typeCoerceMatrix[leftType.ordinal()][rightType.ordinal()];
	}

	private static interface Coercer  extends TypeCoercer.Coercer {
		public NumericNode coerce(JsonNode node);
	}
}
