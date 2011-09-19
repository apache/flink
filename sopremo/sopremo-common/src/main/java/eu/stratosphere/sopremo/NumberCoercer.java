package eu.stratosphere.sopremo;

import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.jsondatamodel.BigIntegerNode;
import eu.stratosphere.sopremo.jsondatamodel.DecimalNode;
import eu.stratosphere.sopremo.jsondatamodel.DoubleNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode.TYPES;
import eu.stratosphere.sopremo.jsondatamodel.LongNode;
import eu.stratosphere.sopremo.jsondatamodel.NumericNode;

public final class NumberCoercer {
	// private static final int NUMBER_TYPES_COUNT = JsonNode.TYPES.values().length;

	/**
	 * The default instance.
	 */
	public static final NumberCoercer INSTANCE = new NumberCoercer();

	// private final NumberType[][] typeCoerceMatrix = new NumberType[NUMBER_TYPES_COUNT][NUMBER_TYPES_COUNT];
	//
	private final Map<JsonNode.TYPES, Coercer> coercers = new EnumMap<JsonNode.TYPES, Coercer>(JsonNode.TYPES.class);

	//
	private final Map<Class<? extends JsonNode>, Coercer> classCoercers = new IdentityHashMap<Class<? extends JsonNode>, Coercer>();

	//
	// private final Map<NumberType, Class<? extends JsonNode>> implementationTypes = new EnumMap<NumberType, Class<?
	// extends JsonNode>>(
	// NumberType.class);
	//
	// private final Map<Class<? extends JsonNode>, NumberType> numberTypes = new IdentityHashMap<Class<? extends
	// JsonNode>, NumberType>();

	public NumberCoercer() {
		// final List<NumberType> widestTypes = Arrays.asList(NumberType.DOUBLE, NumberType.FLOAT,
		// NumberType.BIG_DECIMAL,
		// NumberType.BIG_INTEGER, NumberType.LONG, NumberType.INT);
		// @SuppressWarnings("unchecked")
		// final Class<? extends JsonNode>[] types = (Class<? extends JsonNode>[]) new Class<?>[] { DoubleNode.class,
		// DoubleNode.class, DecimalNode.class, BigIntegerNode.class, LongNode.class, IntNode.class };
		//
		// for (int index = 0; index < types.length; index++) {
		// this.implementationTypes.put(widestTypes.get(index), types[index]);
		// this.numberTypes.put(types[index], widestTypes.get(index));
		// }
		//
		// for (int leftIndex = 0; leftIndex < NUMBER_TYPES_COUNT; leftIndex++)
		// for (int rightIndex = 0; rightIndex < NUMBER_TYPES_COUNT; rightIndex++) {
		// final int coerceIndex = Math.min(widestTypes.indexOf(NumberType.values()[leftIndex]),
		// widestTypes.indexOf(NumberType.values()[rightIndex]));
		// this.typeCoerceMatrix[leftIndex][rightIndex] = widestTypes.get(coerceIndex);
		// }

		this.coercers.put(JsonNode.TYPES.IntNode, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return IntNode.valueOf(((NumericNode) node).getIntValue());
			}
		});
		this.coercers.put(JsonNode.TYPES.LongNode, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return LongNode.valueOf(((NumericNode) node).getLongValue());
			}
		});
		this.coercers.put(JsonNode.TYPES.DoubleNode, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return DoubleNode.valueOf(((NumericNode) node).getDoubleValue());
			}
		});
		this.coercers.put(JsonNode.TYPES.BigIntegerNode, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return BigIntegerNode.valueOf(((NumericNode) node).getBigIntegerValue());
			}
		});
		this.coercers.put(JsonNode.TYPES.DecimalNode, new Coercer() {
			@Override
			public NumericNode coerce(final JsonNode node) {
				return DecimalNode.valueOf(((NumericNode) node).getDecimalValue());
			}
		});

		for (final Entry<JsonNode.TYPES, Coercer> entry : this.coercers.entrySet())
			this.classCoercers.put(
				/* this.implementationTypes.get( */this.getImplementationType(entry.getKey())/* ) */, entry.getValue());
	}

	@SuppressWarnings("unchecked")
	public <T extends NumericNode> T coerce(final NumericNode node, final Class<T> targetType) {
		if (node.getClass() == targetType)
			return (T) node;
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	public NumericNode coerce(final NumericNode node, final JsonNode.TYPES targetType) {
		if (node.getTypePos() == targetType.ordinal())
			return node;
		return this.coercers.get(targetType).coerce(node);
	}

	@SuppressWarnings("unchecked")
	<T extends JsonNode> T coerceGeneric(final JsonNode node, final Class<T> targetType) {
		return (T) this.classCoercers.get(targetType).coerce(node);
	}

	JsonNode coerceGeneric(final JsonNode node, final int targetType) {
		return this.coercers.get(targetType).coerce(node);
	}

	Map<Class<? extends JsonNode>, Coercer> getClassCoercers() {
		return this.classCoercers;
	}

	// public NumberType getWiderType(final NumberType leftType, final NumberType rightType) {
	// return this.typeCoerceMatrix[leftType.ordinal()][rightType.ordinal()];
	// }

	public TYPES getWiderType(final JsonNode leftType, final JsonNode rightType) {
		return leftType.getTypePos() >= rightType.getTypePos() ? leftType.getType() : rightType.getType();
	}

	public Class<? extends JsonNode> getImplementationType(final JsonNode.TYPES type) {
		return type.getClazz();
	}

	private static interface Coercer extends TypeCoercer.Coercer {
		@Override
		public NumericNode coerce(JsonNode node);
	}
}
