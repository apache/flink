package eu.stratosphere.streaming.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

public class TypeExtractTest {

	public static class MySuperlass<T> implements Serializable{

	}

	public static class Myclass extends MySuperlass<Integer> {

		private static final long serialVersionUID = 1L;

	}

	@Test
	public void test() throws IOException, ClassNotFoundException {

		Myclass f = new Myclass();

		System.out.println(f.getClass().getGenericSuperclass());
		TypeInformation<?> ts = TypeExtractor.createTypeInfo(MySuperlass.class, f.getClass(), 0,
				null, null);

		System.out.println(ts);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;

		oos = new ObjectOutputStream(baos);
		oos.writeObject(f);

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		
		
		System.out.println(new TupleTypeInfo<Tuple>(TypeExtractor.getForObject(in.readObject())));
	}

}
