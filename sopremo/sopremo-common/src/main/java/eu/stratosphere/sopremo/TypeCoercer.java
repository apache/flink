package eu.stratosphere.sopremo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.NumericNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class TypeCoercer {
	@SuppressWarnings("unchecked")
	private static final Class<? extends IJsonNode>[] ARRAY_TYPES =
		(Class<? extends IJsonNode>[]) new Class<?>[] { ArrayNode.class };

	private final Map<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer>> coercers = new IdentityHashMap<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer>>();

	@SuppressWarnings("unchecked")
	public static final List<Class<? extends NumericNode>> NUMERIC_TYPES = Arrays.asList(
		IntNode.class, DoubleNode.class, LongNode.class, DecimalNode.class, BigIntegerNode.class);

	private static final Coercer NULL_COERCER = new Coercer() {
		@Override
		public IJsonNode coerce(final IJsonNode node) {
			return null;
		}
	};

	/**
	 * The default instance.
	 */
	public static final TypeCoercer INSTANCE = new TypeCoercer();

	public TypeCoercer() {
		this.coercers.put(BooleanNode.class, this.getToBooleanCoercers());
		this.coercers.put(TextNode.class, this.getToStringCoercers());
		this.coercers.put(ArrayNode.class, this.getToArrayCoercers());
		this.coercers.put(ObjectNode.class, new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>());
		this.addNumericCoercers(this.coercers);
		this.addSelfCoercers();
	}

	private void addNumericCoercers(
			final Map<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer>> coercers) {
		coercers.put(NumericNode.class, new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>());

		// init number to number
		for (final Class<? extends NumericNode> numericType : NUMERIC_TYPES) {
			final IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer> typeCoercers = new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>();
			coercers.put(numericType, typeCoercers);
			typeCoercers.put(NumericNode.class, NumberCoercer.INSTANCE.getClassCoercers().get(numericType));
		}

		// boolean to number
		coercers.get(IntNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return IntNode.valueOf(node == BooleanNode.TRUE ? 1 : 0);
			}
		});
		coercers.get(DoubleNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return DoubleNode.valueOf(node == BooleanNode.TRUE ? 1 : 0);
			}
		});
		coercers.get(LongNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return LongNode.valueOf(node == BooleanNode.TRUE ? 1 : 0);
			}
		});
		coercers.get(DecimalNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return DecimalNode.valueOf(BigDecimal.valueOf(node == BooleanNode.TRUE ? 1 : 0));
			}
		});
		coercers.get(BigIntegerNode.class).put(BooleanNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return BigIntegerNode.valueOf(BigInteger.valueOf(node == BooleanNode.TRUE ? 1 : 0));
			}
		});
		// default boolean to number conversion -> int
		coercers.get(NumericNode.class).put(BooleanNode.class,
			coercers.get(IntNode.class).get(BooleanNode.class));

		// string to number
		coercers.get(IntNode.class).put(TextNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				try {
					return IntNode.valueOf(Integer.parseInt(((TextNode) node).getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(DoubleNode.class).put(TextNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				try {
					return DoubleNode.valueOf(Double.parseDouble(((TextNode) node).getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(LongNode.class).put(TextNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				try {
					return LongNode.valueOf(Long.parseLong(((TextNode) node).getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(DecimalNode.class).put(TextNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				try {
					return DecimalNode.valueOf(new BigDecimal(((TextNode) node).getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(BigIntegerNode.class).put(TextNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				try {
					return BigIntegerNode.valueOf(new BigInteger(((TextNode) node).getTextValue()));
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		// default boolean to number conversion -> decimal
		coercers.get(NumericNode.class).put(TextNode.class,
			coercers.get(DecimalNode.class).get(TextNode.class));
	}

	private void addSelfCoercers() {
		final Coercer selfCoercer = new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return node;
			}
		};
		for (final Entry<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer>> toCoercers : this.coercers
			.entrySet())
			toCoercers.getValue().put(toCoercers.getKey(), selfCoercer);
	}

	public <T extends IJsonNode> T coerce(final IJsonNode node, final Class<T> targetType) {
		final T result = this.coerce(node, targetType, null);
		if (result == null)
			throw new CoercionException(String.format("Cannot coerce %s to %s", node, targetType));
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T extends IJsonNode> T coerce(final IJsonNode node, final Class<T> class1, final T defaultValue) {
		final Map<Class<? extends IJsonNode>, Coercer> toCoercer = this.coercers.get(class1);
		if (toCoercer == null)
			return defaultValue;
		Coercer fromCoercer = toCoercer.get(node.getClass());
		if (fromCoercer == null) {
			for (Class<?> superType = node.getClass(); superType != JsonNode.class.getSuperclass()
				&& fromCoercer == null; superType = superType
				.getSuperclass())
				fromCoercer = toCoercer.get(superType);
			if (fromCoercer == null)
				fromCoercer = NULL_COERCER;
			toCoercer.put(node.getClass(), fromCoercer);
		}

		final T result = (T) fromCoercer.coerce(node);
		if (result == null)
			return defaultValue;
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T extends IJsonNode> T coerce(final IJsonNode node, final T defaultValue) {
		return this.coerce(node, (Class<T>) defaultValue.getClass(), defaultValue);
	}

	private Map<Class<? extends IJsonNode>, Coercer> getToArrayCoercers() {
		final Map<Class<? extends IJsonNode>, Coercer> toArrayCoercers = new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>();
		toArrayCoercers.put(JsonNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				final ArrayNode arrayNode = new ArrayNode();
				arrayNode.add(node);
				return arrayNode;
			}
		});
		final Coercer containerToArray = new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				final ArrayNode arrayNode = new ArrayNode();
				final Iterator<IJsonNode> iterator = ((ArrayNode) node)
					.iterator();
				while (iterator.hasNext())
					arrayNode.add(iterator.next());
				return arrayNode;
			}
		};
		for (final Class<? extends IJsonNode> arrayType : ARRAY_TYPES)
			toArrayCoercers.put(arrayType, containerToArray);
		toArrayCoercers.put(ObjectNode.class, containerToArray);
		return toArrayCoercers;
	}

	private Map<Class<? extends IJsonNode>, Coercer> getToBooleanCoercers() {
		final Map<Class<? extends IJsonNode>, TypeCoercer.Coercer> toBooleanCoercers = new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>();
		toBooleanCoercers.put(NumericNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return BooleanNode.valueOf(((INumericNode) node).getDoubleValue() != 0);
			}
		});
		toBooleanCoercers.put(TextNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return BooleanNode.valueOf(((TextNode) node).getTextValue().length() > 0);
			}
		});
		toBooleanCoercers.put(NullNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return BooleanNode.FALSE;
			}
		});
		final Coercer containerToBoolean = new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return BooleanNode.valueOf(((IArrayNode) node).size() > 0);
			}
		};
		for (final Class<? extends IJsonNode> arrayType : ARRAY_TYPES)
			toBooleanCoercers.put(arrayType, containerToBoolean);
		toBooleanCoercers.put(ObjectNode.class, containerToBoolean);
		return toBooleanCoercers;
	}

	private Map<Class<? extends IJsonNode>, Coercer> getToStringCoercers() {
		final Map<Class<? extends IJsonNode>, TypeCoercer.Coercer> toStringCoercers = new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>();
		toStringCoercers.put(JsonNode.class, new Coercer() {
			@Override
			public IJsonNode coerce(final IJsonNode node) {
				return TextNode.valueOf(node.toString());
			}
		});
		return toStringCoercers;
	}

	public void setCoercer(final Class<? extends IJsonNode> from, final Class<? extends IJsonNode> to,
			final Coercer coercer) {
		Map<Class<? extends IJsonNode>, Coercer> toCoercers = this.coercers.get(to);
		if (toCoercers == null)
			this.coercers.put(to, toCoercers = new IdentityHashMap<Class<? extends IJsonNode>, TypeCoercer.Coercer>());
		toCoercers.put(from, coercer);
	}

	public static interface Coercer {
		public IJsonNode coerce(IJsonNode node);
	}
}
