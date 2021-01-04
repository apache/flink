package org.apache.flink.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Objects;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@State(Scope.Benchmark)
public class RowBenchmark {

	private TypeSerializer<Row> rowSerializer;

	private TypeSerializer<MyPojo> pojoSerializer;


	@Setup(Level.Trial)
	public void setUp() {
		rowSerializer = Types
			.ROW(
				Types.STRING,
				Types.INT,
				Types.BOOLEAN,
				Types.STRING,
				Types.DOUBLE,
				Types.STRING,
				Types.STRING,
				Types.STRING,
				Types.STRING,
				Types.LONG)
			.createSerializer(new ExecutionConfig());
		pojoSerializer = Types.POJO(MyPojo.class).createSerializer(new ExecutionConfig());
	}

//	@Benchmark
//	@OutputTimeUnit(SECONDS)
//	@BenchmarkMode(Throughput)
//	@Fork(3)
//	@Warmup(iterations = 10)
//	@Measurement(iterations = 15)
//	public void testBefore(Blackhole blackhole) throws IOException {
//		for (int i = 0; i < 500_000; i++) {
//			// create
//			final Row row = new Row(10);
//			row.setField(0, "String");
//			row.setField(1, i);
//			row.setField(2, true);
//			row.setField(3, null);
//			row.setField(4, 1.0);
//			row.setField(5, "String 2");
//			row.setField(6, null);
//			row.setField(7, null);
//			row.setField(8, null);
//			row.setField(9, 300L);
//			// serialize/deserialize/copy/equals
//			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
//			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
//			final Row copied = rowSerializer.copy(deserialized);
//			if (!copied.equals(row)) {
//				throw new RuntimeException();
//			}
//			// access
//			blackhole.consume(copied.getField(0));
//			blackhole.consume(copied.getField(1));
//			blackhole.consume(copied.getField(2));
//			blackhole.consume(copied.getField(3));
//			blackhole.consume(copied.getField(4));
//			blackhole.consume(copied.getField(5));
//			blackhole.consume(copied.getField(6));
//			blackhole.consume(copied.getField(7));
//			blackhole.consume(copied.getField(8));
//			blackhole.consume(copied.getField(9));
//		}
//	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testPositioned(Blackhole blackhole) throws IOException {
		for (int i = 0; i < 500_000; i++) {
			// create
			final Row row = Row.withPositions(10);
			row.setField(0, "String");
			row.setField(1, i);
			row.setField(2, true);
			row.setField(3, null);
			row.setField(4, 1.0);
			row.setField(5, "String 2");
			row.setField(6, null);
			row.setField(7, null);
			row.setField(8, null);
			row.setField(9, 300L);
			// serialize/deserialize/copy/equals
			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
			final Row copied = rowSerializer.copy(deserialized);
			if (!copied.equals(row)) {
				throw new RuntimeException();
			}
			// access
			blackhole.consume(copied.getField(0));
			blackhole.consume(copied.getField(1));
			blackhole.consume(copied.getField(2));
			blackhole.consume(copied.getField(3));
			blackhole.consume(copied.getField(4));
			blackhole.consume(copied.getField(5));
			blackhole.consume(copied.getField(6));
			blackhole.consume(copied.getField(7));
			blackhole.consume(copied.getField(8));
			blackhole.consume(copied.getField(9));
		}
	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testNamed(Blackhole blackhole) throws IOException {
		for (int i = 0; i < 500_000; i++) {
			// create
			final Row row = Row.withNames();
			row.setField("f0", "String");
			row.setField("f1", i);
			row.setField("f2", true);
			row.setField("f3", null);
			row.setField("f4", 1.0);
			row.setField("f5", "String 2");
			row.setField("f6", null);
			row.setField("f7", null);
			row.setField("f8", null);
			row.setField("f9", 300L);
			// serialize/deserialize/copy/equals
			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
			final Row copied = rowSerializer.copy(deserialized);
			if (!copied.equals(row)) {
				throw new RuntimeException();
			}
			// access
			blackhole.consume(copied.getField(0));
			blackhole.consume(copied.getField(1));
			blackhole.consume(copied.getField(2));
			blackhole.consume(copied.getField(3));
			blackhole.consume(copied.getField(4));
			blackhole.consume(copied.getField(5));
			blackhole.consume(copied.getField(6));
			blackhole.consume(copied.getField(7));
			blackhole.consume(copied.getField(8));
			blackhole.consume(copied.getField(9));
		}
	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testNamedNotCopy(Blackhole blackhole) throws IOException {
		for (int i = 0; i < 500_000; i++) {
			// create
			final Row row = Row.withNames();
			row.setField("f0", "String");
			row.setField("f1", i);
			row.setField("f2", true);
			row.setField("f3", null);
			row.setField("f4", 1.0);
			row.setField("f5", "String 2");
			row.setField("f6", null);
			row.setField("f7", null);
			row.setField("f8", null);
			row.setField("f9", 300L);
			// serialize/deserialize
			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
			// access
			blackhole.consume(deserialized.getField(0));
			blackhole.consume(deserialized.getField(1));
			blackhole.consume(deserialized.getField(2));
			blackhole.consume(deserialized.getField(3));
			blackhole.consume(deserialized.getField(4));
			blackhole.consume(deserialized.getField(5));
			blackhole.consume(deserialized.getField(6));
			blackhole.consume(deserialized.getField(7));
			blackhole.consume(deserialized.getField(8));
			blackhole.consume(deserialized.getField(9));
		}
	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testPositionedNotCopy(Blackhole blackhole) throws IOException {
		for (int i = 0; i < 500_000; i++) {
			// create
			final Row row = Row.withPositions(10);
			row.setField(0, "String");
			row.setField(1, i);
			row.setField(2, true);
			row.setField(3, null);
			row.setField(4, 1.0);
			row.setField(5, "String 2");
			row.setField(6, null);
			row.setField(7, null);
			row.setField(8, null);
			row.setField(9, 300L);
			// serialize/deserialize
			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
			// access
			blackhole.consume(deserialized.getField(0));
			blackhole.consume(deserialized.getField(1));
			blackhole.consume(deserialized.getField(2));
			blackhole.consume(deserialized.getField(3));
			blackhole.consume(deserialized.getField(4));
			blackhole.consume(deserialized.getField(5));
			blackhole.consume(deserialized.getField(6));
			blackhole.consume(deserialized.getField(7));
			blackhole.consume(deserialized.getField(8));
			blackhole.consume(deserialized.getField(9));
		}
	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testPositionedSerialization(Blackhole blackhole) throws IOException {
		// create
		final Row row = Row.withPositions(10);
		row.setField(0, "String");
		row.setField(2, true);
		row.setField(3, null);
		row.setField(4, 1.0);
		row.setField(5, "String 2");
		row.setField(6, null);
		row.setField(7, null);
		row.setField(8, null);
		row.setField(9, 300L);
		for (int i = 0; i < 500_000; i++) {
			row.setField(1, i);
			// serialize/deserialize
			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
			// access
			blackhole.consume(deserialized.getField(0));
			blackhole.consume(deserialized.getField(1));
			blackhole.consume(deserialized.getField(2));
			blackhole.consume(deserialized.getField(3));
			blackhole.consume(deserialized.getField(4));
			blackhole.consume(deserialized.getField(5));
			blackhole.consume(deserialized.getField(6));
			blackhole.consume(deserialized.getField(7));
			blackhole.consume(deserialized.getField(8));
			blackhole.consume(deserialized.getField(9));
		}
	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testNamedSerialization(Blackhole blackhole) throws IOException {
		// create
		final Row row = Row.withNames();
		row.setField("f0", "String");
		row.setField("f2", true);
		row.setField("f3", null);
		row.setField("f4", 1.0);
		row.setField("f5", "String 2");
		row.setField("f6", null);
		row.setField("f7", null);
		row.setField("f8", null);
		row.setField("f9", 300L);
		for (int i = 0; i < 500_000; i++) {
			row.setField("f1", i);
			// serialize/deserialize
			final byte[] serialized = InstantiationUtil.serializeToByteArray(rowSerializer, row);
			final Row deserialized = InstantiationUtil.deserializeFromByteArray(rowSerializer, serialized);
			// access
			blackhole.consume(deserialized.getField(0));
			blackhole.consume(deserialized.getField(1));
			blackhole.consume(deserialized.getField(2));
			blackhole.consume(deserialized.getField(3));
			blackhole.consume(deserialized.getField(4));
			blackhole.consume(deserialized.getField(5));
			blackhole.consume(deserialized.getField(6));
			blackhole.consume(deserialized.getField(7));
			blackhole.consume(deserialized.getField(8));
			blackhole.consume(deserialized.getField(9));
		}
	}

	@Benchmark
	@OutputTimeUnit(SECONDS)
	@BenchmarkMode(Throughput)
	@Fork(3)
	@Warmup(iterations = 10)
	@Measurement(iterations = 15)
	public void testPojo(Blackhole blackhole) throws IOException {
		for (int i = 0; i < 500_000; i++) {
			// create
			final MyPojo pojo = new MyPojo();
			pojo.setF0("String");
			pojo.setF1(i);
			pojo.setF2(true);
			pojo.setF3(null);
			pojo.setF4(1.0);
			pojo.setF5("String 2");
			pojo.setF6(null);
			pojo.setF7(null);
			pojo.setF8(null);
			pojo.setF9(300L);
			// serialize/deserialize/copy/equals
			final byte[] serialized = InstantiationUtil.serializeToByteArray(pojoSerializer, pojo);
			final MyPojo deserialized = InstantiationUtil.deserializeFromByteArray(pojoSerializer, serialized);
			final MyPojo copied = pojoSerializer.copy(deserialized);
			if (!copied.equals(pojo)) {
				throw new RuntimeException();
			}
			// access
			blackhole.consume(copied.getF0());
			blackhole.consume(copied.getF1());
			blackhole.consume(copied.getF2());
			blackhole.consume(copied.getF3());
			blackhole.consume(copied.getF4());
			blackhole.consume(copied.getF5());
			blackhole.consume(copied.getF6());
			blackhole.consume(copied.getF7());
			blackhole.consume(copied.getF8());
			blackhole.consume(copied.getF9());
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class MyPojo {
		private String f0;
		private Integer f1;
		private Boolean f2;
		private String f3;
		private Double f4;
		private String f5;
		private String f6;
		private String f7;
		private String f8;
		private Long f9;

		public String getF0() {
			return f0;
		}

		public void setF0(String f0) {
			this.f0 = f0;
		}

		public Integer getF1() {
			return f1;
		}

		public void setF1(Integer f1) {
			this.f1 = f1;
		}

		public Boolean getF2() {
			return f2;
		}

		public void setF2(Boolean f2) {
			this.f2 = f2;
		}

		public String getF3() {
			return f3;
		}

		public void setF3(String f3) {
			this.f3 = f3;
		}

		public Double getF4() {
			return f4;
		}

		public void setF4(Double f4) {
			this.f4 = f4;
		}

		public String getF5() {
			return f5;
		}

		public void setF5(String f5) {
			this.f5 = f5;
		}

		public String getF6() {
			return f6;
		}

		public void setF6(String f6) {
			this.f6 = f6;
		}

		public String getF7() {
			return f7;
		}

		public void setF7(String f7) {
			this.f7 = f7;
		}

		public String getF8() {
			return f8;
		}

		public void setF8(String f8) {
			this.f8 = f8;
		}

		public Long getF9() {
			return f9;
		}

		public void setF9(Long f9) {
			this.f9 = f9;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			MyPojo myPojo = (MyPojo) o;
			return Objects.equals(f0, myPojo.f0) && Objects.equals(f1, myPojo.f1) && Objects.equals(
				f2,
				myPojo.f2) && Objects.equals(f3, myPojo.f3) && Objects.equals(f4, myPojo.f4)
				&& Objects.equals(f5, myPojo.f5) && Objects.equals(f6, myPojo.f6) && Objects.equals(
				f7,
				myPojo.f7) && Objects.equals(f8, myPojo.f8) && Objects.equals(f9, myPojo.f9);
		}

		@Override
		public int hashCode() {
			return Objects.hash(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9);
		}
	}
}
