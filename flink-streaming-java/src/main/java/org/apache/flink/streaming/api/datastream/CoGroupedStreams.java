/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *{@code CoGroupedStreams} represents two {@link DataStream DataStreams} that have been co-grouped.
 * A streaming co-group operation is evaluated over elements in a window.
 *
 * <p>To finalize co-group operation you also need to specify a {@link KeySelector} for
 * both the first and second input and a {@link WindowAssigner}.
 *
 * <p>Note: Right now, the groups are being built in memory so you need to ensure that they don't
 * get too big. Otherwise the JVM might crash.
 *
 * <p>Example:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> one = ...;
 * DataStream<Tuple2<String, Integer>> two = ...;
 *
 * DataStream<T> result = one.coGroup(two)
 *     .where(new MyFirstKeySelector())
 *     .equalTo(new MyFirstKeySelector())
 *     .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
 *     .apply(new MyCoGroupFunction());
 * } </pre>
 */
@Public
public class CoGroupedStreams<T1, T2> {

	/** The first input stream. */
	private final DataStream<T1> input1;

	/** The second input stream. */
	private final DataStream<T2> input2;

	/**
	 * Creates new CoGrouped data streams, which are the first step towards building a streaming
	 * co-group.
	 *
	 * @param input1 The first data stream.
	 * @param input2 The second data stream.
	 */
	public CoGroupedStreams(DataStream<T1> input1, DataStream<T2> input2) {
		this.input1 = requireNonNull(input1);
		this.input2 = requireNonNull(input2);
	}

	/**
	 * Specifies a {@link KeySelector} for elements from the first input.
	 *
	 * @param keySelector The KeySelector to be used for extracting the first input's key for partitioning.
	 */
	public <KEY> Where<KEY> where(KeySelector<T1, KEY> keySelector)  {
		Preconditions.checkNotNull(keySelector);
		final TypeInformation<KEY> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
		return where(keySelector, keyType);
	}

	/**
	 * Specifies a {@link KeySelector} for elements from the first input with explicit type information.
	 *
	 * @param keySelector The KeySelector to be used for extracting the first input's key for partitioning.
	 * @param keyType The type information describing the key type.
	 */
	public <KEY> Where<KEY> where(KeySelector<T1, KEY> keySelector, TypeInformation<KEY> keyType)  {
		Preconditions.checkNotNull(keySelector);
		Preconditions.checkNotNull(keyType);
		return new Where<>(input1.clean(keySelector), keyType);
	}

	// ------------------------------------------------------------------------

	/**
	 * CoGrouped streams that have the key for one side defined.
	 *
	 * @param <KEY> The type of the key.
	 */
	@Public
	public class Where<KEY> {

		private final KeySelector<T1, KEY> keySelector1;
		private final TypeInformation<KEY> keyType;

		Where(KeySelector<T1, KEY> keySelector1, TypeInformation<KEY> keyType) {
			this.keySelector1 = keySelector1;
			this.keyType = keyType;
		}

		/**
		 * Specifies a {@link KeySelector} for elements from the second input.
		 *
		 * @param keySelector The KeySelector to be used for extracting the second input's key for partitioning.
		 */
		public EqualTo equalTo(KeySelector<T2, KEY> keySelector)  {
			Preconditions.checkNotNull(keySelector);
			final TypeInformation<KEY> otherKey = TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
			return equalTo(keySelector, otherKey);
		}

		/**
		 * Specifies a {@link KeySelector} for elements from the second input with explicit type information for the key type.
		 *
		 * @param keySelector The KeySelector to be used for extracting the key for partitioning.
		 * @param keyType The type information describing the key type.
		 */
		public EqualTo equalTo(KeySelector<T2, KEY> keySelector, TypeInformation<KEY> keyType)  {
			Preconditions.checkNotNull(keySelector);
			Preconditions.checkNotNull(keyType);

			if (!keyType.equals(this.keyType)) {
				throw new IllegalArgumentException("The keys for the two inputs are not equal: " +
						"first key = " + this.keyType + " , second key = " + keyType);
			}

			return new EqualTo(input2.clean(keySelector));
		}

		// --------------------------------------------------------------------

		/**
		 * A co-group operation that has {@link KeySelector KeySelectors} defined for both inputs.
		 */
		@Public
		public class EqualTo {

			private final KeySelector<T2, KEY> keySelector2;

			EqualTo(KeySelector<T2, KEY> keySelector2) {
				this.keySelector2 = requireNonNull(keySelector2);
			}

			/**
			 * Specifies the window on which the co-group operation works.
			 */
			@PublicEvolving
			public <W extends Window> WithWindow<T1, T2, KEY, W> window(WindowAssigner<? super TaggedUnion<T1, T2>, W> assigner) {
				return new WithWindow<>(input1, input2, keySelector1, keySelector2, keyType, assigner, null, null);
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A co-group operation that has {@link KeySelector KeySelectors} defined for both inputs as
	 * well as a {@link WindowAssigner}.
	 *
	 * @param <T1> Type of the elements from the first input
	 * @param <T2> Type of the elements from the second input
	 * @param <KEY> Type of the key. This must be the same for both inputs
	 * @param <W> Type of {@link Window} on which the co-group operation works.
	 */
	@Public
	public static class WithWindow<T1, T2, KEY, W extends Window> {
		private final DataStream<T1> input1;
		private final DataStream<T2> input2;

		private final KeySelector<T1, KEY> keySelector1;
		private final KeySelector<T2, KEY> keySelector2;

		private final TypeInformation<KEY> keyType;

		private final WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner;

		private final Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger;

		private final Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor;

		protected WithWindow(DataStream<T1> input1,
				DataStream<T2> input2,
				KeySelector<T1, KEY> keySelector1,
				KeySelector<T2, KEY> keySelector2,
				TypeInformation<KEY> keyType,
				WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner,
				Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger,
				Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor) {
			this.input1 = input1;
			this.input2 = input2;

			this.keySelector1 = keySelector1;
			this.keySelector2 = keySelector2;
			this.keyType = keyType;

			this.windowAssigner = windowAssigner;
			this.trigger = trigger;
			this.evictor = evictor;
		}

		/**
		 * Sets the {@code Trigger} that should be used to trigger window emission.
		 */
		@PublicEvolving
		public WithWindow<T1, T2, KEY, W> trigger(Trigger<? super TaggedUnion<T1, T2>, ? super W> newTrigger) {
			return new WithWindow<>(input1, input2, keySelector1, keySelector2, keyType,
					windowAssigner, newTrigger, evictor);
		}

		/**
		 * Sets the {@code Evictor} that should be used to evict elements from a window before
		 * emission.
		 *
		 * <p>Note: When using an evictor window performance will degrade significantly, since
		 * pre-aggregation of window results cannot be used.
		 */
		@PublicEvolving
		public WithWindow<T1, T2, KEY, W> evictor(Evictor<? super TaggedUnion<T1, T2>, ? super W> newEvictor) {
			return new WithWindow<>(input1, input2, keySelector1, keySelector2, keyType,
					windowAssigner, trigger, newEvictor);
		}

		/**
		 * Completes the co-group operation with the user function that is executed
		 * for windowed groups.
		 *
		 * <p>Note: This method's return type does not support setting an operator-specific parallelism.
		 * Due to binary backwards compatibility, this cannot be altered. Use the {@link #with(CoGroupFunction)}
		 * method to set an operator-specific parallelism.
		 */
		public <T> DataStream<T> apply(CoGroupFunction<T1, T2, T> function) {

			TypeInformation<T> resultType = TypeExtractor.getCoGroupReturnTypes(
				function,
				input1.getType(),
				input2.getType(),
				"CoGroup",
				false);

			return apply(function, resultType);
		}

		/**
		 * Completes the co-group operation with the user function that is executed
		 * for windowed groups.
		 *
		 * <p><b>Note:</b> This is a temporary workaround while the {@link #apply(CoGroupFunction)}
		 * method has the wrong return type and hence does not allow one to set an operator-specific
		 * parallelism
		 *
		 * @deprecated This method will be removed once the {@link #apply(CoGroupFunction)} method is fixed
		 *             in the next major version of Flink (2.0).
		 */
		@PublicEvolving
		@Deprecated
		public <T> SingleOutputStreamOperator<T> with(CoGroupFunction<T1, T2, T> function) {
			return (SingleOutputStreamOperator<T>) apply(function);
		}

		/**
		 * Completes the co-group operation with the user function that is executed
		 * for windowed groups.
		 *
		 * <p>Note: This method's return type does not support setting an operator-specific parallelism.
		 * Due to binary backwards compatibility, this cannot be altered. Use the
		 * {@link #with(CoGroupFunction, TypeInformation)} method to set an operator-specific parallelism.
		 */
		public <T> DataStream<T> apply(CoGroupFunction<T1, T2, T> function, TypeInformation<T> resultType) {
			//clean the closure
			function = input1.getExecutionEnvironment().clean(function);

			UnionTypeInfo<T1, T2> unionType = new UnionTypeInfo<>(input1.getType(), input2.getType());
			UnionKeySelector<T1, T2, KEY> unionKeySelector = new UnionKeySelector<>(keySelector1, keySelector2);

			DataStream<TaggedUnion<T1, T2>> taggedInput1 = input1
					.map(new Input1Tagger<T1, T2>())
					.setParallelism(input1.getParallelism())
					.returns(unionType);
			DataStream<TaggedUnion<T1, T2>> taggedInput2 = input2
					.map(new Input2Tagger<T1, T2>())
					.setParallelism(input2.getParallelism())
					.returns(unionType);

			DataStream<TaggedUnion<T1, T2>> unionStream = taggedInput1.union(taggedInput2);

			// we explicitly create the keyed stream to manually pass the key type information in
			WindowedStream<TaggedUnion<T1, T2>, KEY, W> windowOp =
					new KeyedStream<TaggedUnion<T1, T2>, KEY>(unionStream, unionKeySelector, keyType)
					.window(windowAssigner);

			if (trigger != null) {
				windowOp.trigger(trigger);
			}
			if (evictor != null) {
				windowOp.evictor(evictor);
			}

			return windowOp.apply(new CoGroupWindowFunction<T1, T2, T, KEY, W>(function), resultType);
		}

		/**
		 * Completes the co-group operation with the user function that is executed
		 * for windowed groups.
		 *
		 * <p><b>Note:</b> This is a temporary workaround while the {@link #apply(CoGroupFunction, TypeInformation)}
		 * method has the wrong return type and hence does not allow one to set an operator-specific
		 * parallelism
		 *
		 * @deprecated This method will be removed once the {@link #apply(CoGroupFunction, TypeInformation)}
		 *             method is fixed in the next major version of Flink (2.0).
		 */
		@PublicEvolving
		@Deprecated
		public <T> SingleOutputStreamOperator<T> with(CoGroupFunction<T1, T2, T> function, TypeInformation<T> resultType) {
			return (SingleOutputStreamOperator<T>) apply(function, resultType);
		}
	}

	// ------------------------------------------------------------------------
	//  Data type and type information for Tagged Union
	// ------------------------------------------------------------------------

	/**
	 * Internal class for implementing tagged union co-group.
	 */
	@Internal
	public static class TaggedUnion<T1, T2> {
		private final T1 one;
		private final T2 two;

		private TaggedUnion(T1 one, T2 two) {
			this.one = one;
			this.two = two;
		}

		public boolean isOne() {
			return one != null;
		}

		public boolean isTwo() {
			return two != null;
		}

		public T1 getOne() {
			return one;
		}

		public T2 getTwo() {
			return two;
		}

		public static <T1, T2> TaggedUnion<T1, T2> one(T1 one) {
			return new TaggedUnion<>(one, null);
		}

		public static <T1, T2> TaggedUnion<T1, T2> two(T2 two) {
			return new TaggedUnion<>(null, two);
		}
	}

	private static class UnionTypeInfo<T1, T2> extends TypeInformation<TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		private final TypeInformation<T1> oneType;
		private final TypeInformation<T2> twoType;

		public UnionTypeInfo(TypeInformation<T1> oneType,
				TypeInformation<T2> twoType) {
			this.oneType = oneType;
			this.twoType = twoType;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 2;
		}

		@Override
		public int getTotalFields() {
			return 2;
		}

		@Override
		@SuppressWarnings("unchecked, rawtypes")
		public Class<TaggedUnion<T1, T2>> getTypeClass() {
			return (Class) TaggedUnion.class;
		}

		@Override
		public boolean isKeyType() {
			return true;
		}

		@Override
		public TypeSerializer<TaggedUnion<T1, T2>> createSerializer(ExecutionConfig config) {
			return new UnionSerializer<>(oneType.createSerializer(config), twoType.createSerializer(config));
		}

		@Override
		public String toString() {
			return "TaggedUnion<" + oneType + ", " + twoType + ">";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UnionTypeInfo) {
				@SuppressWarnings("unchecked")
				UnionTypeInfo<T1, T2> unionTypeInfo = (UnionTypeInfo<T1, T2>) obj;

				return unionTypeInfo.canEqual(this) && oneType.equals(unionTypeInfo.oneType) && twoType.equals(unionTypeInfo.twoType);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 *  oneType.hashCode() + twoType.hashCode();
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof UnionTypeInfo;
		}
	}

	private static class UnionSerializer<T1, T2> extends TypeSerializer<TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		private final TypeSerializer<T1> oneSerializer;
		private final TypeSerializer<T2> twoSerializer;

		public UnionSerializer(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
			this.oneSerializer = oneSerializer;
			this.twoSerializer = twoSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<TaggedUnion<T1, T2>> duplicate() {
			return this;
		}

		@Override
		public TaggedUnion<T1, T2> createInstance() {
			return null;
		}

		@Override
		public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from) {
			if (from.isOne()) {
				return TaggedUnion.one(oneSerializer.copy(from.getOne()));
			} else {
				return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
			}
		}

		@Override
		public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from, TaggedUnion<T1, T2> reuse) {
			if (from.isOne()) {
				return TaggedUnion.one(oneSerializer.copy(from.getOne()));
			} else {
				return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
			}		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(TaggedUnion<T1, T2> record, DataOutputView target) throws IOException {
			if (record.isOne()) {
				target.writeByte(1);
				oneSerializer.serialize(record.getOne(), target);
			} else {
				target.writeByte(2);
				twoSerializer.serialize(record.getTwo(), target);
			}
		}

		@Override
		public TaggedUnion<T1, T2> deserialize(DataInputView source) throws IOException {
			byte tag = source.readByte();
			if (tag == 1) {
				return TaggedUnion.one(oneSerializer.deserialize(source));
			} else {
				return TaggedUnion.two(twoSerializer.deserialize(source));
			}
		}

		@Override
		public TaggedUnion<T1, T2> deserialize(TaggedUnion<T1, T2> reuse,
				DataInputView source) throws IOException {
			byte tag = source.readByte();
			if (tag == 1) {
				return TaggedUnion.one(oneSerializer.deserialize(source));
			} else {
				return TaggedUnion.two(twoSerializer.deserialize(source));
			}
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			byte tag = source.readByte();
			target.writeByte(tag);
			if (tag == 1) {
				oneSerializer.copy(source, target);
			} else {
				twoSerializer.copy(source, target);
			}
		}

		@Override
		public int hashCode() {
			return 31 * oneSerializer.hashCode() + twoSerializer.hashCode();
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean equals(Object obj) {
			if (obj instanceof UnionSerializer) {
				UnionSerializer<T1, T2> other = (UnionSerializer<T1, T2>) obj;

				return other.canEqual(this) && oneSerializer.equals(other.oneSerializer) && twoSerializer.equals(other.twoSerializer);
			} else {
				return false;
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof UnionSerializer;
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new UnionSerializerConfigSnapshot<>(oneSerializer, twoSerializer);
		}

		@Override
		public CompatibilityResult<TaggedUnion<T1, T2>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof UnionSerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs =
					((UnionSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T1> oneSerializerCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousSerializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					previousSerializersAndConfigs.get(0).f1,
					oneSerializer);

				CompatibilityResult<T2> twoSerializerCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousSerializersAndConfigs.get(1).f0,
					UnloadableDummyTypeSerializer.class,
					previousSerializersAndConfigs.get(1).f1,
					twoSerializer);

				if (!oneSerializerCompatResult.isRequiresMigration() && !twoSerializerCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else if (oneSerializerCompatResult.getConvertDeserializer() != null && twoSerializerCompatResult.getConvertDeserializer() != null) {
					return CompatibilityResult.requiresMigration(
						new UnionSerializer<>(
							new TypeDeserializerAdapter<>(oneSerializerCompatResult.getConvertDeserializer()),
							new TypeDeserializerAdapter<>(twoSerializerCompatResult.getConvertDeserializer())));
				}
			}

			return CompatibilityResult.requiresMigration();
		}
	}

	/**
	 * The {@link TypeSerializerConfigSnapshot} for the {@link UnionSerializer}.
	 */
	public static class UnionSerializerConfigSnapshot<T1, T2> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public UnionSerializerConfigSnapshot() {}

		public UnionSerializerConfigSnapshot(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
			super(oneSerializer, twoSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	// ------------------------------------------------------------------------
	//  Utility functions that implement the CoGroup logic based on the tagged
	//  union window reduce
	// ------------------------------------------------------------------------

	private static class Input1Tagger<T1, T2> implements MapFunction<T1, TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		@Override
		public TaggedUnion<T1, T2> map(T1 value) throws Exception {
			return TaggedUnion.one(value);
		}
	}

	private static class Input2Tagger<T1, T2> implements MapFunction<T2, TaggedUnion<T1, T2>> {
		private static final long serialVersionUID = 1L;

		@Override
		public TaggedUnion<T1, T2> map(T2 value) throws Exception {
			return TaggedUnion.two(value);
		}
	}

	private static class UnionKeySelector<T1, T2, KEY> implements KeySelector<TaggedUnion<T1, T2>, KEY> {
		private static final long serialVersionUID = 1L;

		private final KeySelector<T1, KEY> keySelector1;
		private final KeySelector<T2, KEY> keySelector2;

		public UnionKeySelector(KeySelector<T1, KEY> keySelector1,
				KeySelector<T2, KEY> keySelector2) {
			this.keySelector1 = keySelector1;
			this.keySelector2 = keySelector2;
		}

		@Override
		public KEY getKey(TaggedUnion<T1, T2> value) throws Exception{
			if (value.isOne()) {
				return keySelector1.getKey(value.getOne());
			} else {
				return keySelector2.getKey(value.getTwo());
			}
		}
	}

	private static class CoGroupWindowFunction<T1, T2, T, KEY, W extends Window>
			extends WrappingFunction<CoGroupFunction<T1, T2, T>>
			implements WindowFunction<TaggedUnion<T1, T2>, T, KEY, W> {

		private static final long serialVersionUID = 1L;

		public CoGroupWindowFunction(CoGroupFunction<T1, T2, T> userFunction) {
			super(userFunction);
		}

		@Override
		public void apply(KEY key,
				W window,
				Iterable<TaggedUnion<T1, T2>> values,
				Collector<T> out) throws Exception {

			List<T1> oneValues = new ArrayList<>();
			List<T2> twoValues = new ArrayList<>();

			for (TaggedUnion<T1, T2> val: values) {
				if (val.isOne()) {
					oneValues.add(val.getOne());
				} else {
					twoValues.add(val.getTwo());
				}
			}
			wrappedFunction.coGroup(oneValues, twoValues, out);
		}
	}
}
