package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import scala.Product;

/**
 * This class wraps different HBase sink implementations to provide a common interface for all of them.
 *
 * @param <IN> input type
 */
public class HBaseSink<IN> {

	private DataStreamSink<IN> sink;

	private HBaseSink(DataStreamSink<IN> sink) {
		this.sink = sink;
	}

	private SinkTransformation<IN> getSinkTransformation() {
		return sink.getTransformation();
	}

	/**
	 * Sets the name of this sink. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named sink.
	 */
	public HBaseSink<IN> name(String name) {
		getSinkTransformation().setName(name);
		return this;
	}

	/**
	 * Sets an ID for this operator.
	 *
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 *
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 * @return The operator with the specified ID.
	 */
	@PublicEvolving
	public HBaseSink<IN> uid(String uid) {
		getSinkTransformation().setUid(uid);
		return this;
	}

	/**
	 * Sets an user provided hash for this operator. This will be used AS IS the create the JobVertexID.
	 *
	 * <p>The user provided hash is an alternative to the generated hashes, that is considered when identifying an
	 * operator through the default hash mechanics fails (e.g. because of changes between Flink versions).
	 *
	 * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting. The provided hash
	 * needs to be unique per transformation and job. Otherwise, job submission will fail. Furthermore, you cannot
	 * assign user-specified hash to intermediate nodes in an operator chain and trying so will let your job fail.
	 *
	 * <p>A use case for this is in migration between Flink versions or changing the jobs in a way that changes the
	 * automatically generated hashes. In this case, providing the previous hashes directly through this method (e.g.
	 * obtained from old logs) can help to reestablish a lost mapping from states to their target operator.
	 *
	 * @param uidHash The user provided hash for this operator. This will become the JobVertexID, which is shown in the
	 *                 logs and web ui.
	 * @return The operator with the user provided hash.
	 */
	@PublicEvolving
	public HBaseSink<IN> setUidHash(String uidHash) {
		getSinkTransformation().setUidHash(uidHash);
		return this;
	}

	/**
	 * Sets the parallelism for this sink. The degree must be higher than zero.
	 *
	 * @param parallelism The parallelism for this sink.
	 * @return The sink with set parallelism.
	 */
	public HBaseSink<IN> setParallelism(int parallelism) {
		getSinkTransformation().setParallelism(parallelism);
		return this;
	}

	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization.
	 * <p/>
	 * <p/>
	 * Chaining can be turned off for the whole
	 * job by {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 *
	 * @return The sink with chaining disabled
	 */
	public HBaseSink<IN> disableChaining() {
		getSinkTransformation().setChainingStrategy(ChainingStrategy.NEVER);
		return this;
	}

	/**
	 * Sets the slot sharing group of this operation. Parallel instances of
	 * operations that are in the same slot sharing group will be co-located in the same
	 * TaskManager slot, if possible.
	 *
	 * <p>Operations inherit the slot sharing group of input operations if all input operations
	 * are in the same slot sharing group and no slot sharing group was explicitly specified.
	 *
	 * <p>Initially an operation is in the default slot sharing group. An operation can be put into
	 * the default group explicitly by setting the slot sharing group to {@code "default"}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	public HBaseSink<IN> slotSharingGroup(String slotSharingGroup) {
		getSinkTransformation().setSlotSharingGroup(slotSharingGroup);
		return this;
	}

	/**
	 * Writes a DataStream into a HBase database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return HBaseSinkBuilder, to further configure the sink
	 */
	public static <IN> HBaseSinkBuilder<IN> addSink(org.apache.flink.streaming.api.scala.DataStream<IN> input) {
		return addSink(input.javaStream());
	}

	/**
	 * Writes a DataStream into a HBase database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return HBaseSinkBuilder, to further configure the sink
	 */
	public static <IN> HBaseSinkBuilder<IN> addSink(DataStream<IN> input) {
		TypeInformation<IN> typeInfo = input.getType();
		if (typeInfo instanceof TupleTypeInfo) {
			DataStream<Tuple> tupleInput = (DataStream<Tuple>) input;
			return (HBaseSinkBuilder<IN>) new HBaseTupleSinkBuilder<>(tupleInput, tupleInput.getType(), tupleInput.getType().createSerializer(tupleInput.getExecutionEnvironment().getConfig()));
		}
		if (typeInfo instanceof RowTypeInfo) {
			DataStream<Row> rowInput = (DataStream<Row>) input;
			return (HBaseSinkBuilder<IN>) new HBaseRowSinkBuilder(rowInput, rowInput.getType(), rowInput.getType().createSerializer(rowInput.getExecutionEnvironment().getConfig()));
		}
		if (typeInfo instanceof PojoTypeInfo) {
			return new HBasePojoSinkBuilder<>(input, input.getType(), input.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
		}
		if (typeInfo instanceof CaseClassTypeInfo) {
			DataStream<Product> productInput = (DataStream<Product>) input;
			return (HBaseSinkBuilder<IN>) new HBaseScalaProductSinkBuilder<>(productInput, productInput.getType(), productInput.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
		}
		throw new IllegalArgumentException("No support for the type of the given DataStream: " + input.getType());
	}

	/**
	 * Builder for a {@link HBaseSink}.
	 * @param <IN>
	 */
	public abstract static class HBaseSinkBuilder<IN> {
		protected final DataStream<IN> input;
		protected final TypeSerializer<IN> serializer;
		protected final TypeInformation<IN> typeInfo;
		protected HBaseTableBuilder tableBuilder;
		protected HBaseTableMapper tableMapper;

		public HBaseSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			this.input = input;
			this.serializer = serializer;
			this.typeInfo = typeInfo;
			tableBuilder = new HBaseTableBuilder();
		}

		/**
		 * Sets the cluster key of HBase to connect to.
		 * @param clusterKey
		 * @return this builder
		 * @throws IOException
		 */
		public HBaseSinkBuilder<IN> setClusterKey(String clusterKey) throws IOException {
			tableBuilder.withClusterKey(clusterKey);
			return this;
		}

		/**
		 * Enable the client buffer for HBase.
		 * Only flush when buffer is full or during checkpoint.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> enableBuffer() {
			tableBuilder.enableBuffer(true);
			return this;
		}

		/**
		 * Disable the client buffer for HBase.
		 * Flush to HBase on every operation. This might decrease the throughput and increase latency.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> disableBuffer() {
			tableBuilder.enableBuffer(false);
			return this;
		}

		/**
		 * Sets the name of table to be used.
		 * @param tableName
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setTableName(String tableName) {
			tableBuilder.withTableName(tableName);
			return this;
		}

		/**
		 * Sets additional property for hbase.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setProperty(String key, String value) {
			tableBuilder.addProperty(key, value);
			return this;
		}

		/**
		 * Sets the mapper for the sink.
		 * @param tableMapper {@link HBaseTableMapper}, records the mapping for a key to a HBase column family and qualifier.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setTableMapper(HBaseTableMapper tableMapper) {
			this.tableMapper = tableMapper;
			return this;
		}

		/**
		 * Finalizes the configuration of this sink.
		 *
		 * @return finalized sink
		 * @throws Exception
		 */
		public HBaseSink<IN> build() throws Exception {
			return createSink();
		}

		protected abstract HBaseSink<IN> createSink() throws Exception;

		protected void sanityCheck() {
			if (!tableBuilder.isClusterKeyConfigured()) {
				throw new IllegalArgumentException("HBase cluster key must be supplied using setClusterKey().");
			}
			if (tableMapper == null) {
				throw new IllegalArgumentException("HBaseTableMapper must be supplied using setTableMapper().");
			}
			if (tableMapper.getRowKey() == null) {
				throw new IllegalArgumentException("Rowkey must be supplied using setRowKey() of HBaseTableMapper.");
			}
			if (tableMapper.getKeyList() == null || tableMapper.getKeyList().length == 0) {
				throw new IllegalArgumentException(
					"At least one column should be supplied using either setMapping() or addMapping.");
			}
			if (this.tableBuilder.getTableName() == null) {
				throw new IllegalArgumentException("Table name must be supplied using setTableName() of HBaseTableMapper.");
			}
			for (String key : tableMapper.getKeyList()) {
				if (StringUtils.isEmpty(key)) {
					throw new IllegalArgumentException("The keys for sink cannot be empty.");
				}
			}
		}
	}

	/**
	 * Builder for a {@link HBaseTupleSink}.
	 * @param <IN>
	 */
	public static class HBaseTupleSinkBuilder<IN extends Tuple> extends HBaseSinkBuilder<IN> {

		public HBaseTupleSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override
		protected void sanityCheck() {
			super.sanityCheck();
			for (String key : tableMapper.getKeyList()) {
				if (StringUtils.isNumeric(key)) {
					throw new IllegalArgumentException("The key: " + key + " for tuple sink must be index of tuple.");
				}
			}
		}

		@Override
		protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBaseTupleSink<IN>(tableBuilder, tableMapper)));
		}
	}

	/**
	 * Builder for a {@link HBaseRowSink}.
	 */
	public static class HBaseRowSinkBuilder extends HBaseSinkBuilder<Row> {

		public HBaseRowSinkBuilder(DataStream<Row> input, TypeInformation<Row> typeInfo, TypeSerializer<Row> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override
		protected HBaseSink<Row> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBaseRowSink(tableBuilder, tableMapper, (RowTypeInfo) typeInfo)));
		}
	}

	/**
	 * Builder for a {@link HBasePojoSink}.
	 * @param <IN>
	 */
	public static class HBasePojoSinkBuilder<IN> extends HBaseSinkBuilder<IN> {

		public HBasePojoSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBasePojoSink<IN>(tableBuilder, tableMapper, typeInfo)));
		}
	}

	/**
	 * Builder for a {@link HBaseScalaProductSink}.
	 * @param <IN>
	 */
	public static class HBaseScalaProductSinkBuilder<IN extends Product> extends HBaseSinkBuilder<IN> {

		public HBaseScalaProductSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBaseScalaProductSink<IN>(tableBuilder, tableMapper)));
		}

		@Override
		protected void sanityCheck() {
			super.sanityCheck();
			for (String key : tableMapper.getKeyList()) {
				if (StringUtils.isNumeric(key)) {
					throw new IllegalArgumentException("The key: " + key + " for scala product sink must be index of product element.");
				}
			}
		}
	}
}
