package eu.stratosphere.sopremo.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.Reference;
import eu.stratosphere.util.reflect.ReflectUtil;
import eu.stratosphere.util.reflect.TypeHierarchyBrowser;
import eu.stratosphere.util.reflect.TypeHierarchyBrowser.Mode;
import eu.stratosphere.util.reflect.Visitor;

public class TypeCoercer {

	private final Map<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer<?, ?>>> coercers =
		new IdentityHashMap<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer<?, ?>>>();

	@SuppressWarnings("unchecked")
	public static final List<Class<? extends INumericNode>> NUMERIC_TYPES = Arrays.asList((Class<? extends INumericNode>)
		IntNode.class, DoubleNode.class, LongNode.class, DecimalNode.class, BigIntegerNode.class);

	private static final Coercer<IJsonNode, IJsonNode> NULL_COERCER = new Coercer<IJsonNode, IJsonNode>(null) {
		@Override
		public IJsonNode coerce(final IJsonNode node, final IJsonNode target) {
			return null;
		}
	};

	/**
	 * The default instance.
	 */
	public static final TypeCoercer INSTANCE = new TypeCoercer();

	public TypeCoercer() {
		this.addCoercers(MissingNode.class, new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, MissingNode>>());
		this.addCoercers(BooleanNode.class, this.getToBooleanCoercers());
		this.addCoercers(TextNode.class, this.getToStringCoercers());
		this.addCoercers(IArrayNode.class, this.getToArrayCoercers());
		this.addCoercers(IObjectNode.class, new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, IObjectNode>>());
		this.addNumericCoercers(this.coercers);
		this.addCoercers(IJsonNode.class, new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, IJsonNode>>());
		this.addSelfCoercers();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <To extends IJsonNode> void addCoercers(final Class<To> targetClass, final Map<?, Coercer<?, To>> coercers) {
		this.coercers.put(targetClass, (Map) coercers);
	}

	private void addNumericCoercers(
			final Map<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer<?, ?>>> coercers) {
		coercers.put(AbstractNumericNode.class, new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, ?>>());

		// init number to number
		for (final Class<? extends INumericNode> numericType : NUMERIC_TYPES) {
			final IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, ?>> typeCoercers =
				new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, ?>>();
			coercers.put(numericType, typeCoercers);
			typeCoercers.put(AbstractNumericNode.class, NumberCoercer.INSTANCE.getClassCoercers().get(numericType));
		}

		// boolean to number
		coercers.get(AbstractNumericNode.class).put(BooleanNode.class, new Coercer<BooleanNode, IntNode>(IntNode.class) {
			@Override
			public IntNode coerce(final BooleanNode from, final IntNode target) {
				target.setValue(from == BooleanNode.TRUE ? 1 : 0);
				return target;
			}
		});
		coercers.get(IntNode.class).put(BooleanNode.class, new Coercer<BooleanNode, IntNode>(IntNode.class) {
			@Override
			public IntNode coerce(final BooleanNode from, final IntNode target) {
				target.setValue(from == BooleanNode.TRUE ? 1 : 0);
				return target;
			}
		});
		coercers.get(DoubleNode.class).put(BooleanNode.class, new Coercer<BooleanNode, DoubleNode>(DoubleNode.class) {
			@Override
			public DoubleNode coerce(final BooleanNode from, final DoubleNode target) {
				target.setValue(from == BooleanNode.TRUE ? 1 : 0);
				return target;
			}
		});
		coercers.get(LongNode.class).put(BooleanNode.class, new Coercer<BooleanNode, LongNode>(LongNode.class) {
			@Override
			public LongNode coerce(final BooleanNode from, final LongNode target) {
				target.setValue(from == BooleanNode.TRUE ? 1 : 0);
				return target;
			}
		});
		coercers.get(DecimalNode.class).put(BooleanNode.class, new Coercer<BooleanNode, DecimalNode>(DecimalNode.class) {
			@Override
			public DecimalNode coerce(final BooleanNode from, final DecimalNode target) {
				target.setValue(from == BooleanNode.TRUE ? BigDecimal.ONE : BigDecimal.ZERO);
				return target;
			}
		});
		coercers.get(BigIntegerNode.class).put(BooleanNode.class, new Coercer<BooleanNode, BigIntegerNode>(BigIntegerNode.class) {
			@Override
			public BigIntegerNode coerce(final BooleanNode from, final BigIntegerNode target) {
				target.setValue(from == BooleanNode.TRUE ? BigInteger.ONE : BigInteger.ZERO);
				return target;
			}
		});
		// default boolean to number conversion -> int
		coercers.get(AbstractNumericNode.class).put(BooleanNode.class,
			coercers.get(IntNode.class).get(BooleanNode.class));

		// string to number
		coercers.get(IntNode.class).put(TextNode.class, new Coercer<TextNode, IntNode>(IntNode.class) {
			@Override
			public IntNode coerce(final TextNode from, final IntNode target) {
				try {
					// TODO: bottleneck
					target.setValue(Integer.parseInt(from.getTextValue().toString()));
					return target;
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(DoubleNode.class).put(TextNode.class, new Coercer<TextNode, DoubleNode>(DoubleNode.class) {
			@Override
			public DoubleNode coerce(final TextNode from, final DoubleNode target) {
				try {
					// TODO: bottleneck
					target.setValue(Double.parseDouble(from.getTextValue().toString()));
					return target;
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(LongNode.class).put(TextNode.class, new Coercer<TextNode, LongNode>(LongNode.class) {
			@Override
			public LongNode coerce(final TextNode from, final LongNode target) {
				try {
					// TODO: bottleneck
					target.setValue(Long.parseLong(from.getTextValue().toString()));
					return target;
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(DecimalNode.class).put(TextNode.class, new Coercer<TextNode, DecimalNode>(DecimalNode.class) {
			@Override
			public DecimalNode coerce(final TextNode from, final DecimalNode target) {
				try {
					// TODO: bottleneck
					target.setValue(new BigDecimal(from.getTextValue().toString()));
					return target;
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		coercers.get(BigIntegerNode.class).put(TextNode.class, new Coercer<TextNode, BigIntegerNode>(BigIntegerNode.class) {
			@Override
			public BigIntegerNode coerce(final TextNode from, final BigIntegerNode target) {
				try {
					// TODO: bottleneck
					target.setValue(new BigInteger(from.getTextValue().toString()));
					return target;
				} catch (final NumberFormatException e) {
					return null;
				}
			}
		});
		// default text to number conversion -> decimal
		coercers.get(AbstractNumericNode.class).put(TextNode.class,
			coercers.get(DecimalNode.class).get(TextNode.class));
	}

	private void addSelfCoercers() {		
		for (final Entry<Class<? extends IJsonNode>, Map<Class<? extends IJsonNode>, Coercer<?, ?>>> toCoercers : this.coercers.entrySet()) {
			final Class<? extends IJsonNode> targetClass = toCoercers.getKey();
			Map<Class<? extends IJsonNode>, Coercer<?, ?>> coercers = toCoercers.getValue();
			if(coercers.containsKey(targetClass))
				continue;
			if(targetClass.isInterface())
				coercers.put(targetClass, new CopyCoercer());
			else
				coercers.put(targetClass, new SelfCoercer(targetClass));
		}
	}

	public <From extends IJsonNode, To extends IJsonNode> To coerce(final From node, final To target,
			final Class<To> targetType) {
		final To result = this.coerce(node, target, targetType, null);
		if (result == null)
			throw new CoercionException(String.format("Cannot coerce %s to %s", node, targetType));
		return result;
	}

	@SuppressWarnings("unchecked")
	public <From extends IJsonNode, To extends IJsonNode> To coerce(final From node, final To target,
			final Class<To> targetClass, final To defaultValue) {
		Map<Class<? extends IJsonNode>, Coercer<?, ?>> toCoercer = this.coercers.get(targetClass);
		if (toCoercer == null) {
			final Map<Class<? extends IJsonNode>, Coercer<?, ?>> superclassCoercers = this
				.findSuperclassCoercers(targetClass);
			if (superclassCoercers == null)
				toCoercer = new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, ?>>();
			else
				toCoercer = new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, ?>>(superclassCoercers);

			this.coercers.put(targetClass, toCoercer);
		}
		Coercer<From, To> fromCoercer = (Coercer<From, To>) toCoercer.get(node.getClass());
		if (fromCoercer == null) {
			fromCoercer = this.findMatchingCoercer(node, toCoercer);
			if (fromCoercer == null)
				fromCoercer = (Coercer<From, To>) NULL_COERCER;
			toCoercer.put(node.getClass(), fromCoercer);
		}

		final Class<? extends To> defaultType = fromCoercer.getDefaultType();
		To result = defaultType != null ? SopremoUtil.ensureType(target, targetClass, defaultType) : null;
		result = fromCoercer.coerce(node, result);
		if (result == null)
			return defaultValue;
		return result;
	}

	protected <To, From> Map<Class<? extends IJsonNode>, Coercer<?, ?>> findSuperclassCoercers(final Class<?> toClass) {
		final Reference<Map<Class<? extends IJsonNode>, Coercer<?, ?>>> toCoercers =
			new Reference<Map<Class<? extends IJsonNode>, Coercer<?, ?>>>();

		TypeHierarchyBrowser.INSTANCE.visit(toClass, Mode.CLASS_FIRST, new Visitor<Class<?>>() {
			@Override
			public boolean visited(final Class<?> superClass, final int distance) {
				final Map<Class<? extends IJsonNode>, Coercer<?, ?>> froms = TypeCoercer.this.coercers.get(superClass);
				if (froms == null)
					return true;
				// found a matching coercer; terminate browsing
				toCoercers.setValue(froms);
				return false;
			}
		});

		return toCoercers.getValue();
	}

	@SuppressWarnings("unchecked")
	protected <To, From> Coercer<From, To> findMatchingCoercer(final From node,
			final Map<Class<? extends IJsonNode>, Coercer<?, ?>> toCoercer) {
		final Reference<Coercer<From, To>> fromCoercer = new Reference<TypeCoercer.Coercer<From, To>>();

		TypeHierarchyBrowser.INSTANCE.visit(node.getClass(), Mode.CLASS_FIRST, new Visitor<Class<?>>() {
			@Override
			public boolean visited(final Class<?> superClass, final int distance) {
				final Coercer<From, To> coercer = (Coercer<From, To>) toCoercer.get(superClass);
				if (coercer == null)
					return true;
				// found a matching coercer; terminate browsing
				fromCoercer.setValue(coercer);
				return false;
			}
		});

		return fromCoercer.getValue();
	}

	@SuppressWarnings("unchecked")
	public <From extends IJsonNode, To extends IJsonNode> To coerce(final From node, final To target,
			final To defaultValue) {
		return this.coerce(node, target, (Class<To>) defaultValue.getClass(), defaultValue);
	}

	private Map<Class<? extends IJsonNode>, Coercer<?, IArrayNode>> getToArrayCoercers() {
		final Map<Class<? extends IJsonNode>, Coercer<?, IArrayNode>> toArrayCoercers =
			new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, IArrayNode>>();
		toArrayCoercers.put(IJsonNode.class, new Coercer<IJsonNode, IArrayNode>(ArrayNode.class) {
			@Override
			public IArrayNode coerce(final IJsonNode from, final IArrayNode target) {
				target.clear();
				target.add(from);
				return target;
			}
		});
		toArrayCoercers.put(IArrayNode.class, new Coercer<IArrayNode, IArrayNode>(ArrayNode.class) {
			@Override
			public IArrayNode coerce(final IArrayNode from, final IArrayNode target) {
				target.clear();
				target.addAll(from);
				return target;
			}
		});
		toArrayCoercers.put(IObjectNode.class, new Coercer<IObjectNode, IArrayNode>(ArrayNode.class) {
			@Override
			public IArrayNode coerce(final IObjectNode from, final IArrayNode target) {
				target.clear();
				for (final Entry<String, IJsonNode> entry : from)
					target.add(entry.getValue());
				return target;
			}
		});
		return toArrayCoercers;
	}

	private Map<Class<? extends IJsonNode>, Coercer<?, BooleanNode>> getToBooleanCoercers() {
		final Map<Class<? extends IJsonNode>, Coercer<?, BooleanNode>> toBooleanCoercers =
			new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, BooleanNode>>();
		toBooleanCoercers.put(BooleanNode.class, new Coercer<BooleanNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final BooleanNode from, final BooleanNode target) {
				return from;
			}
		});
		toBooleanCoercers.put(INumericNode.class, new Coercer<INumericNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final INumericNode from, final BooleanNode target) {
				return BooleanNode.valueOf(from.getDoubleValue() != 0);
			}
		});
		toBooleanCoercers.put(TextNode.class, new Coercer<TextNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final TextNode from, final BooleanNode target) {
				return BooleanNode.valueOf(from.getTextValue().length() > 0);
			}
		});
		toBooleanCoercers.put(NullNode.class, new Coercer<NullNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final NullNode from, final BooleanNode target) {
				return BooleanNode.FALSE;
			}
		});
		toBooleanCoercers.put(MissingNode.class, new Coercer<MissingNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final MissingNode from, final BooleanNode target) {
				return BooleanNode.FALSE;
			}
		});
		toBooleanCoercers.put(IArrayNode.class, new Coercer<IArrayNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final IArrayNode from, final BooleanNode target) {
				return BooleanNode.valueOf(from.size() > 0);
			}
		});
		toBooleanCoercers.put(IObjectNode.class, new Coercer<IObjectNode, BooleanNode>(BooleanNode.class) {
			@Override
			public BooleanNode coerce(final IObjectNode from, final BooleanNode target) {
				return BooleanNode.valueOf(from.size() > 0);
			}
		});
		return toBooleanCoercers;
	}

	private Map<Class<? extends IJsonNode>, Coercer<?, TextNode>> getToStringCoercers() {
		final Map<Class<? extends IJsonNode>, Coercer<?, TextNode>> toStringCoercers =
			new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, TextNode>>();
		toStringCoercers.put(IJsonNode.class, new Coercer<IJsonNode, TextNode>(TextNode.class) {
			@Override
			public TextNode coerce(final IJsonNode from, final TextNode target) {
				target.setValue(from.toString());
				return target;
			}
		});
		toStringCoercers.put(NullNode.class, new Coercer<IJsonNode, TextNode>(TextNode.class) {
			@Override
			public TextNode coerce(final IJsonNode from, final TextNode target) {
				target.setValue("");
				return target;
			}
		});
		return toStringCoercers;
	}

	public <From extends IJsonNode, To extends IJsonNode> void setCoercer(final Class<From> from, final Class<To> to,
			final Coercer<From, To> coercer) {
		Map<Class<? extends IJsonNode>, Coercer<?, ?>> toCoercers = this.coercers.get(to);
		if (toCoercers == null)
			this.coercers.put(to, toCoercers = new IdentityHashMap<Class<? extends IJsonNode>, Coercer<?, ?>>());
		toCoercers.put(from, coercer);
	}

	/**
	 * @author Arvid Heise
	 */
	private static final class SelfCoercer extends Coercer<IJsonNode, IJsonNode> {
		/**
		 * Initializes SelfCoercer.
		 */
		public SelfCoercer(Class<? extends IJsonNode> defaultType) {
			super(defaultType);
		}

		@Override
		public IJsonNode coerce(final IJsonNode node, final IJsonNode target) {
			target.copyValueFrom(node);
			return target;
		}
	}
	
	/**
	 * @author Arvid Heise
	 */
	private static final class CopyCoercer extends Coercer<IJsonNode, IJsonNode> {
		/**
		 * Initializes SuperCoercer.
		 */
		public CopyCoercer() {
			super(null);
		}

		@Override
		public IJsonNode coerce(final IJsonNode node, IJsonNode target) {
			if(target == null)
				target = ReflectUtil.newInstance(node.getClass());
			target.copyValueFrom(node);
			return target;
		}
	}

	public static abstract class Coercer<From, To> {
		private final Class<? extends To> defaultType;

		public Coercer(final Class<? extends To> defaultType) {
			this.defaultType = defaultType;
		}

//		/**
//		 * Initializes TypeCoercer.Coercer.
//		 */
//		@SuppressWarnings("unchecked")
//		public Coercer() {
//			this.defaultType =
//				(Class<To>) BoundTypeUtil.getBindingOfSuperclass(this.getClass(), Coercer.class).getParameters()[1]
//					.getType();
//			try {
//				this.defaultType.newInstance();
//			} catch (final InstantiationException e) {
//				e.printStackTrace();
//			} catch (final IllegalAccessException e) {
//				e.printStackTrace();
//			}
//		}

		public abstract To coerce(From from, To target);

		/**
		 * Returns the resultType.
		 * 
		 * @return the resultType
		 */
		public Class<? extends To> getDefaultType() {
			return this.defaultType;
		}
	}
}
