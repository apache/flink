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

	private final NumberType[][] typeCoerceMatrix = new NumberType[NUMBER_TYPES_COUNT][NUMBER_TYPES_COUNT];

	private final Map<NumberType, Coercer> coercers = new EnumMap<NumberType, Coercer>(NumberType.class);

	private final Map<Class<? extends JsonNode>, Coercer> classCoercers = new IdentityHashMap<Class<? extends JsonNode>, Coercer>();

	private final Map<NumberType, Class<? extends JsonNode>> implementationTypes = new EnumMap<NumberType, Class<? extends JsonNode>>(
		NumberType.class);

	private final Map<Class<? extends JsonNode>, NumberType> numberTypes = new IdentityHashMap<Class<? extends JsonNode>, NumberType>();

	public NumberCoercer() {
		final List<NumberType> widestTypes = Arrays.asList(NumberType.DOUBLE, NumberType.FLOAT, NumberType.BIG_DECIMAL,
			NumberType.BIG_INTEGER, NumberType.LONG, NumberType.INT);
		@SuppressWarnings("unchecked")
		final Class<? extends JsonNode>[] types = (Class<? extends JsonNode>[]) new Class<?>[] { DoubleNode.class,
			DoubleNode.class, DecimalNode.class, BigIntegerNode.class, LongNode.class, IntNode.class };

		for (int index = 0; index < types.length; index++) {
			this.implementationTypes.put(widestTypes.get(index), types[index]);
			this.numberTypes.put(types[index], widestTypes.get(index));
		}

		for (int leftIndex = 0; leftIndex < NUMBER_TYPES_COUNT; leftIndex++)
			for (int rightIndex = 0; rightIndex < NUMBER_TYPES_COUNT; rightIndex++) {
				final int coerceIndex = Math.min(widestTypes.indexOf(NumberType.values()[leftIndex]),
					widestTypes.indexOf(NumberType.values()[rightIndex]));
				this.typeCoerceMatrix[leftIndex][rightIndex] = widestTypes.get(coerceIndex);
			}

		this.coercers.put(NumberType.INT, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return IntNode.valueOf(node.getIntValue());
			}
		});
		this.coercers.put(NumberType.LONG, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return LongNode.valueOf(node.getLongValue());
			}
		});
		this.coercers.put(NumberType.DOUBLE, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return DoubleNode.valueOf(node.getDoubleValue());
			}
		});
		this.coercers.put(NumberType.FLOAT, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return DoubleNode.valueOf(node.getDoubleValue());
			}
		});
		this.coercers.put(NumberType.BIG_INTEGER, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return BigIntegerNode.valueOf(node.getBigIntegerValue());
			}
		});
		this.coercers.put(NumberType.BIG_DECIMAL, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return DecimalNode.valueOf(node.getDecimalValue());
			}
		});

		for (final Entry<NumberType, Coercer> entry : this.coercers.entrySet())
			this.classCoercers.put(this.implementationTypes.get(entry.getKey()), entry.getValue());
	}

	@SuppressWarnings("unchecked")
	public <T extends NumericNode> T coerce(final NumericNode node, final Class<T> targetType) {
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	public NumericNode coerce(final NumericNode node, final NumberType targetType) {
		return this.coercers.get(targetType).coerce(node);
	}

	@SuppressWarnings("unchecked")
	<T extends JsonNode> T coerceGeneric(final JsonNode node, final Class<T> targetType) {
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	JsonNode coerceGeneric(final JsonNode node, final NumberType targetType) {
		return this.coercers.get(targetType).coerce(node);
	}

	Map<Class<? extends JsonNode>, Coercer> getClassCoercers() {
		return this.classCoercers;
	}

	public NumberType getWiderType(final NumberType leftType, final NumberType rightType) {
		return this.typeCoerceMatrix[leftType.ordinal()][rightType.ordinal()];
	}

	private static interface Coercer extends TypeCoercer.Coercer {
		@Override
		public NumericNode coerce(JsonNode node);
	}
}
