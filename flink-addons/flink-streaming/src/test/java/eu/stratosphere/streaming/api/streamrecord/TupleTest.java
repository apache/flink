package eu.stratosphere.streaming.api.streamrecord;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.types.StringValue;

public class TupleTest {

	public Tuple readTuple(DataInput in) throws IOException {

		StringValue typeVal = new StringValue();
		typeVal.read(in);
		// TODO: use Tokenizer
		String[] types = typeVal.getValue().split(",");
		Class[] basicTypes = new Class[types.length];
		for (int i = 0; i < types.length; i++) {
			try {
				basicTypes[i] = Class.forName(types[i]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo
				.getBasicTupleTypeInfo(basicTypes);
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();

		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(
				tupleSerializer);
		dd.setInstance(tupleSerializer.createInstance());
		dd.read(in);
		return dd.getInstance();
	}

	private void writeTuple(Tuple tuple, DataOutput out) {
		Class[] basicTypes = new Class[tuple.getArity()];
		StringBuilder basicTypeNames = new StringBuilder();

		for (int i = 0; i < basicTypes.length; i++) {
			basicTypes[i] = tuple.getField(i).getClass();
			basicTypeNames.append(basicTypes[i].getName() + ",");
		}
		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo
				.getBasicTupleTypeInfo(basicTypes);

		StringValue typeVal = new StringValue(basicTypeNames.toString());

		@SuppressWarnings("unchecked")
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();
		SerializationDelegate<Tuple> serializationDelegate = new SerializationDelegate<Tuple>(
				tupleSerializer);
		serializationDelegate.setInstance(tuple);
		try {
			typeVal.write(out);
			serializationDelegate.write(out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void test() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);

		int num = 42;
		String str = "above clouds";

		Tuple2<Integer, String> tuple = new Tuple2<Integer, String>(num, str);

		try {

			writeTuple(tuple, out);
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(
					buff.toByteArray()));

			Tuple2<Integer, String> tupleOut = (Tuple2<Integer, String>) readTuple(
					in);
			assertEquals(tupleOut.getField(0), 42);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
	}

}
