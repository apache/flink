package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;

public class TypeExtractorContractTest {

	@Test
	public void testMaterializeTypeVariableToActualType() {

		final List<ParameterizedType> parameterizedTypes =
			TypeExtractor.buildParameterizedTypeHierarchy(MyUdf.class, BasicInterface.class, true);

		final ParameterizedType baseInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 1);
		final TypeVariable firstTypeVariableOfBaseInterface = (TypeVariable) baseInterfaceType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfBaseInterface = (TypeVariable) baseInterfaceType.getActualTypeArguments()[1];

		final Type materializedFirstTypeVariable = TypeExtractor.materializeTypeVariable(parameterizedTypes, firstTypeVariableOfBaseInterface);
		final ParameterizedType materializedSecondTypeVariable = (ParameterizedType) TypeExtractor.materializeTypeVariable(parameterizedTypes, secondTypeVariableOfBaseInterface);

		Assert.assertEquals(Integer.class, materializedFirstTypeVariable);
		Assert.assertEquals(String.class, materializedSecondTypeVariable.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, materializedSecondTypeVariable.getActualTypeArguments()[1]);
		Assert.assertEquals(Tuple2.class, materializedSecondTypeVariable.getRawType());
	}

	@Test
	public void testMaterializeTypeVariableToBottomTypeVariable() {
		final UDF myUdf = new UDF<String, Integer, Long, String>();

		final List<ParameterizedType> parameterizedTypes =
			TypeExtractor.buildParameterizedTypeHierarchy(myUdf.getClass(), BasicInterface.class, true);

		final ParameterizedType baseInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 1);

		final TypeVariable firstTypeVariableOfBaseInterface = (TypeVariable) baseInterfaceType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfBaseInterface = (TypeVariable) baseInterfaceType.getActualTypeArguments()[1];

		final ParameterizedType richInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 2);

		final TypeVariable firstTypeVariableOfRichInterface = (TypeVariable) richInterfaceType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfRichInterface = (TypeVariable) richInterfaceType.getActualTypeArguments()[1];

		final Type materializedFirstTypeVariable = TypeExtractor.materializeTypeVariable(parameterizedTypes, firstTypeVariableOfBaseInterface);
		final Type materializedSecondTypeVariable = TypeExtractor.materializeTypeVariable(parameterizedTypes, secondTypeVariableOfBaseInterface);

		Assert.assertEquals(firstTypeVariableOfRichInterface, materializedFirstTypeVariable);
		Assert.assertEquals(secondTypeVariableOfRichInterface, materializedSecondTypeVariable);

	}

	@Test
	public void testBuildParameterizedTypeHierarchyFromBothSuperClassAndInterface() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyUdf.class, BasicInterface.class, true);

		Assert.assertEquals(3, parameterizedTypeHierarchy.size());
		Assert.assertEquals(UDF.class, parameterizedTypeHierarchy.get(0).getRawType());
		Assert.assertEquals(RichInterface.class, parameterizedTypeHierarchy.get(1).getRawType());
		Assert.assertEquals(BasicInterface.class, parameterizedTypeHierarchy.get(2).getRawType());

	}

	@Test
	public void testBuildParameterizedTypeHierarchyOnlyFromSuperClass() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyUdf.class, Object.class, false);

		Assert.assertEquals(1, parameterizedTypeHierarchy.size());
		Assert.assertEquals(UDF.class, parameterizedTypeHierarchy.get(0).getRawType());
	}

	@Test
	public void testBuildParameterizedTypeHierarchyWithoutInheritance() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyUdf.class, TypeExtractorContractTest.class, true);

		Assert.assertEquals(Collections.emptyList(), parameterizedTypeHierarchy);
	}

	interface BasicInterface<X, Y> {
		X doSomething(Y y);
	}

	interface RichInterface<X, Y, Z> extends BasicInterface<X, Y> {
		Z doSomething(X x, Y y);
	}

	class UDF<X, Y, Z, K> implements RichInterface<X, Y, Z> {

		K k;

		@Override
		public X doSomething(Y y) {
			return null;
		}

		@Override
		public Z doSomething(X x, Y y) {
			return null;
		}
	}

	class MyUdf extends UDF<Integer, Tuple2<String, Integer>, Tuple2<Integer, Double>, Long> {

	}
}
