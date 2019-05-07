package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

/**
 *  Tests concerning type extraction of Parametrized Pojo and its superclasses.
 */
public class PojoParametrizedTypeExtractionTest {

	@Test
	public void testCreateTypeInfo(){
		/// Init
		final TypeInformation<ParametrizedParentImpl> directTypeInfo = TypeExtractor.createTypeInfo(ParametrizedParentImpl.class);

		/// Check
		Assert.assertTrue(directTypeInfo instanceof PojoTypeInfo);
	}

	@Test
	public void testFieldExtraction(){
		/// Init
		final TypeInformation<ParametrizedParentImpl> directTypeInfo = TypeExtractor.createTypeInfo(ParametrizedParentImpl.class);

		// Extract
		TypeInformation<?> directPojoFieldTypeInfo = ((PojoTypeInfo) directTypeInfo)
			.getPojoFieldAt(0)
			.getTypeInformation();

		// Check
		Assert.assertTrue(directPojoFieldTypeInfo instanceof PojoTypeInfo);
	}

	@Test
	public void testMapReturnTypeInfo(){
		/// Init
		final TypeInformation<ParametrizedParentImpl> directTypeInfo = TypeExtractor.createTypeInfo(ParametrizedParentImpl.class);

		// Extract
		TypeInformation<ParametrizedParentImpl> mapReturnTypeInfo = TypeExtractor
			.getMapReturnTypes(new ConcreteMapFunction(), directTypeInfo);

		// Check
		Assert.assertTrue(mapReturnTypeInfo instanceof PojoTypeInfo);
		Assert.assertEquals(directTypeInfo, mapReturnTypeInfo);
	}

	@Test
	public void testMapReturnFieldTypeInfo(){
		/// Init
		final TypeInformation<ParametrizedParentImpl> directTypeInfo = TypeExtractor.createTypeInfo(ParametrizedParentImpl.class);

		// Extract
		TypeInformation<ParametrizedParentImpl> mapReturnTypeInfo = TypeExtractor
			.getMapReturnTypes(new ConcreteMapFunction(), directTypeInfo);
		TypeInformation<?> mapReturnPojoFieldTypeInfo = ((PojoTypeInfo) mapReturnTypeInfo)
			.getPojoFieldAt(0)
			.getTypeInformation();

		// Check
		Assert.assertTrue(mapReturnPojoFieldTypeInfo instanceof PojoTypeInfo);

	}

	/**
	 * Representation of Pojo class with 2 fields.
	 */
	public static class Pojo implements Serializable {
		public int digits;
		public String letters;
	}

	/**
	 * Representation of class which is parametrized by some pojo.
	 */
	public static class ParameterizedParent<T extends Serializable> implements Serializable {
		public T pojoField;
	}

	/**
	 * Implementation of ParametrizedParent parametrized by Pojo.
	 */
	public static class ParametrizedParentImpl extends ParameterizedParent<Pojo> {
		public double precise;
	}

	/**
	 * Representation of map function for type extraction.
	 */
	public static class ConcreteMapFunction implements MapFunction<ParametrizedParentImpl, ParametrizedParentImpl> {
		@Override
		public ParametrizedParentImpl map(ParametrizedParentImpl value) throws Exception {
			return null;
		}
	}

}
