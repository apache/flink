/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.gateway.utils.source.random;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link InputFormat} for {@link RandomSource}.
 */
public class RandomInputFormat implements InputFormat<Row, LimitableInputSplit> {
	private final List<LogicalType> types;
	private final List<RandomValueSetter> randomValueSetters;
	private final long limit;
	private final long interval;

	private LimitableInputSplit currentSplit;
	private long currentIndex;

	public RandomInputFormat(TableSchema schema, long limit, long interval) {
		this.types = new ArrayList<>();
		initTypes(schema);

		this.randomValueSetters = new ArrayList<>();
		createValueSetters(schema);

		this.limit = limit;
		this.interval = interval;

		this.currentIndex = 0;
	}

	@Override
	public void configure(Configuration parameters) {
		// do nothing
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public LimitableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		long[] limitPerSplit = createLimitForPerSplit(limit, minNumSplits);
		Preconditions.checkArgument(limitPerSplit.length == minNumSplits);
		LimitableInputSplit[] splits = new LimitableInputSplit[minNumSplits];
		for (int i = 0; i < limitPerSplit.length; ++i) {
			splits[i] = new LimitableInputSplit(limitPerSplit[i], i);
		}
		return splits;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(LimitableInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(LimitableInputSplit split) throws IOException {
		this.currentSplit = split;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return limitReached(currentIndex);
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		sleep(interval);
		Row row = new Row(types.size());
		for (int i = 0; i < randomValueSetters.size(); i++) {
			randomValueSetters.get(i).setField(row, i);
		}
		currentIndex++;
		return row;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

	private boolean limitReached(long currentIndex) {
		return currentSplit.getLimit() > 0 && currentIndex >= currentSplit.getLimit();
	}

	private void sleep(long interval) {
		if (interval > 0) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void initTypes(TableSchema schema) {
		for (TableColumn column : schema.getTableColumns()) {
			types.add(column.getType().getLogicalType());
		}
	}

	private void createValueSetters(TableSchema schema) {
		for (TableColumn column : schema.getTableColumns()) {
			LogicalType type = column.getType().getLogicalType();
			if (type instanceof BooleanType) {
				randomValueSetters.add(new RandomBooleanSetter());
			} else if (type instanceof TinyIntType) {
				randomValueSetters.add(new RandomTinyIntSetter());
			} else if (type instanceof SmallIntType) {
				randomValueSetters.add(new RandomSmallIntSetter());
			} else if (type instanceof IntType) {
				randomValueSetters.add(new RandomIntSetter());
			} else if (type instanceof BigIntType) {
				randomValueSetters.add(new RandomBigIntSetter());
			} else if (type instanceof FloatType) {
				randomValueSetters.add(new RandomFloatSetter());
			} else if (type instanceof DoubleType) {
				randomValueSetters.add(new RandomDoubleSetter());
			} else if (type instanceof CharType) {
				int length = ((CharType) type).getLength();
				randomValueSetters.add(new RandomStringSetter(length, length));
			} else if (type instanceof VarCharType) {
				int length = ((VarCharType) type).getLength();
				randomValueSetters.add(new RandomStringSetter(1, length));
			} else if (type instanceof DateType) {
				randomValueSetters.add(new RandomDateSetter());
			} else if (type instanceof TimeType) {
				randomValueSetters.add(new RandomTimeSetter());
			} else if (type instanceof TimestampType) {
				randomValueSetters.add(new RandomTimestampSetter());
			} else if (type instanceof DecimalType) {
				randomValueSetters.add(new RandomBigDecimalSetter());
			} else if (type instanceof BinaryType) {
				int length = ((BinaryType) type).getLength();
				randomValueSetters.add(new RandomBytesSetter(length, length));
			} else if (type instanceof VarBinaryType) {
				int length = ((VarBinaryType) type).getLength();
				randomValueSetters.add(new RandomBytesSetter(1, length));
			} else {
				throw new UnsupportedOperationException("RandomSourceFunction does not support type " + type);
			}
		}
	}

	/**
	 * An interface which sets a random value into the specified field of a row.
	 */
	private interface RandomValueSetter extends Serializable {
		void setField(Row row, int field);
	}

	/**
	 * A {@link RandomValueSetter} which sets a random boolean value into the specified field of a row.
	 */
	private static class RandomBooleanSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, ThreadLocalRandom.current().nextBoolean());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random byte value into the specified field of a row.
	 */
	private static class RandomTinyIntSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, (byte) ThreadLocalRandom.current().nextInt(-128, 128));
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random short value into the specified field of a row.
	 */
	private static class RandomSmallIntSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, (short) ThreadLocalRandom.current().nextInt(-32768, 32768));
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random int value into the specified field of a row.
	 */
	private static class RandomIntSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, ThreadLocalRandom.current().nextInt());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random long value into the specified field of a row.
	 */
	private static class RandomBigIntSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, ThreadLocalRandom.current().nextLong());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random float value into the specified field of a row.
	 */
	private static class RandomFloatSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, ThreadLocalRandom.current().nextFloat());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random double value into the specified field of a row.
	 */
	private static class RandomDoubleSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			row.setField(field, ThreadLocalRandom.current().nextDouble());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random string value into the specified field of a row.
	 */
	private static class RandomStringSetter implements RandomValueSetter {

		private final int minLen;
		private final int maxLen;

		public RandomStringSetter(int minLen, int maxLen) {
			this.minLen = minLen;
			this.maxLen = maxLen;
		}

		@Override
		public void setField(Row row, int field) {
			int length = ThreadLocalRandom.current().nextInt(minLen, maxLen + 1);
			StringBuilder builder = new StringBuilder(length);
			for (int i = 0; i < length; i++) {
				builder.append((char) ThreadLocalRandom.current().nextInt(33, 126));
			}
			row.setField(field, builder.toString());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random local date value into the specified field of a row.
	 */
	private static class RandomDateSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			Date date = new Date(ThreadLocalRandom.current().nextLong(0, System.currentTimeMillis() * 2));
			row.setField(field, date.toLocalDate());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random time value into the specified field of a row.
	 */
	private static class RandomTimeSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			Time time = new Time(ThreadLocalRandom.current().nextLong(0, System.currentTimeMillis() * 2));
			row.setField(field, time.toLocalTime());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random timestamp value into the specified field of a row.
	 */
	private static class RandomTimestampSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			Timestamp timestamp = new Timestamp(
					ThreadLocalRandom.current().nextLong(0, System.currentTimeMillis() * 2));
			row.setField(field, timestamp.toLocalDateTime());
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random big decimal value into the specified field of a row.
	 */
	private static class RandomBigDecimalSetter implements RandomValueSetter {

		@Override
		public void setField(Row row, int field) {
			BigDecimal divisor = new BigDecimal(ThreadLocalRandom.current().nextInt(1, 256));
			int scale = ThreadLocalRandom.current().nextInt(1, 30);
			BigDecimal bigDecimal = new BigDecimal(ThreadLocalRandom.current().nextLong())
					.divide(divisor, scale, BigDecimal.ROUND_HALF_EVEN);
			row.setField(field, bigDecimal);
		}
	}

	/**
	 * A {@link RandomValueSetter} which sets a random byte array into the specified field of a row.
	 */
	private static class RandomBytesSetter implements RandomValueSetter {

		private final int minLen;
		private final int maxLen;

		public RandomBytesSetter(int minLen, int maxLen) {
			this.minLen = minLen;
			this.maxLen = maxLen;
		}

		@Override
		public void setField(Row row, int field) {
			byte[] result = new byte[ThreadLocalRandom.current().nextInt(minLen, maxLen + 1)];
			ThreadLocalRandom.current().nextBytes(result);
			row.setField(field, result);
		}
	}

	private static long[] createLimitForPerSplit(long totalLimit, int minNumSplits) {
		long[] limitPerSplit = new long[minNumSplits];
		if (totalLimit < 0) {
			Arrays.fill(limitPerSplit, -1);
		} else {
			long avgLimit = totalLimit / minNumSplits;
			long remaining = totalLimit % minNumSplits;
			for (int i = 0; i < minNumSplits; ++i) {
				if (i < remaining) {
					limitPerSplit[i] = avgLimit + 1;
				} else {
					limitPerSplit[i] = avgLimit;
				}
			}
		}
		return limitPerSplit;
	}
}
