package eu.stratosphere.sopremo.type;

import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.type.TypeCoercer.Coercer;

public final class NumberCoercer {
	// private static final int NUMBER_TYPES_COUNT = IJsonNode.TYPES.values().length;

	/**
	 * The default instance.
	 */
	public static final NumberCoercer INSTANCE = new NumberCoercer();

	private final Map<AbstractJsonNode.Type, Coercer<? extends INumericNode, ? extends INumericNode>> coercers =
		new EnumMap<AbstractJsonNode.Type, Coercer<? extends INumericNode, ? extends INumericNode>>(
			AbstractJsonNode.Type.class);

	private final Map<Class<? extends IJsonNode>, Coercer<? extends INumericNode, ? extends INumericNode>> classCoercers =
		new IdentityHashMap<Class<? extends IJsonNode>, Coercer<? extends INumericNode, ? extends INumericNode>>();

	public NumberCoercer() {
		// final List<NumberType> widestTypes = Arrays.asList(NumberType.DOUBLE, NumberType.FLOAT,
		// NumberType.BIG_DECIMAL,
		// NumberType.BIG_INTEGER, NumberType.LONG, NumberType.INT);
		// @SuppressWarnings("unchecked")
		// final Class<? extends IJsonNode>[] types = (Class<? extends IJsonNode>[]) new Class<?>[] { DoubleNode.class,
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

		this.coercers.put(AbstractJsonNode.Type.IntNode, new Coercer<INumericNode, IntNode>() {
			@Override
			public IntNode coerce(final INumericNode from, final IntNode target) {
				target.setValue(from.getIntValue());
				return target;
			}
		});
		this.coercers.put(AbstractJsonNode.Type.LongNode, new Coercer<INumericNode, LongNode>() {
			@Override
			public LongNode coerce(final INumericNode from, final LongNode target) {
				target.setValue(from.getLongValue());
				return target;
			}
		});
		this.coercers.put(AbstractJsonNode.Type.DoubleNode, new Coercer<INumericNode, DoubleNode>() {
			@Override
			public DoubleNode coerce(final INumericNode from, final DoubleNode target) {
				target.setValue(from.getDoubleValue());
				return target;
			}
		});
		this.coercers.put(AbstractJsonNode.Type.DecimalNode, new Coercer<INumericNode, DecimalNode>() {
			@Override
			public DecimalNode coerce(final INumericNode from, final DecimalNode target) {
				target.setValue(from.getDecimalValue());
				return target;
			}
		});
		this.coercers.put(AbstractJsonNode.Type.BigIntegerNode, new Coercer<INumericNode, BigIntegerNode>() {
			@Override
			public BigIntegerNode coerce(final INumericNode from, final BigIntegerNode target) {
				target.setValue(from.getBigIntegerValue());
				return target;
			}
		});

		for (final Entry<AbstractJsonNode.Type, Coercer<? extends INumericNode, ? extends INumericNode>> entry : this.coercers
			.entrySet())
			this.classCoercers.put(entry.getKey().getClazz(), entry.getValue());
	}

	@SuppressWarnings("unchecked")
	public <From extends INumericNode, To extends INumericNode> To coerce(final From node, final To target,
			final Class<To> targetType) {
		if (node.getClass() == targetType)
			return (To) node;
		final Coercer<From, To> coercer = (Coercer<From, To>) this.classCoercers.get(targetType);
		return coercer.coerce(node, target);
	}

	@SuppressWarnings("unchecked")
	public <From extends INumericNode, To extends INumericNode> To coerce(final From node, final To target,
			final AbstractJsonNode.Type targetType) {
		if (node.getType() == targetType)
			return (To) node;
		final Coercer<From, To> coercer = (Coercer<From, To>) this.coercers.get(targetType);
		return coercer.coerce(node, target);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	Map<Class<? extends IJsonNode>, Coercer<?, ?>> getClassCoercers() {
		return (Map) this.classCoercers;
	}

	// public NumberType getWiderType(final NumberType leftType, final NumberType rightType) {
	// return this.typeCoerceMatrix[leftType.ordinal()][rightType.ordinal()];
	// }

	public AbstractJsonNode.Type getWiderType(final IJsonNode leftType, final IJsonNode rightType) {
		return leftType.getType().ordinal() >= rightType.getType().ordinal() ? leftType.getType() : rightType.getType();
	}
}
