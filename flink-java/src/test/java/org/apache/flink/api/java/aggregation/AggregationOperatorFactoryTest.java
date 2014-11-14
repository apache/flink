package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction.ResultTypeBehavior;
import org.apache.flink.api.java.aggregation.AggregationOperatorFactory.ResultTupleFactory;
import org.apache.flink.api.java.aggregation.AggregationOperatorFactory.ResultTypeFactory;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AggregationOperatorFactory.class)
public class AggregationOperatorFactoryTest {

	private AggregationOperatorFactory factory = new AggregationOperatorFactory();
	private ResultTypeFactory typeFactory = new ResultTypeFactory();
	private ResultTupleFactory tupleFactory = new ResultTupleFactory();
	
	@SuppressWarnings({ "rawtypes" })
	@Test
	public void shouldCreateTupleWithSingleElementTypeGivenByAggregationFunction() {
		// given
		BasicTypeInfo inputBasicTypeInfo = mock(BasicTypeInfo.class);
		TupleTypeInfoBase<Tuple> tupleTypeInfo = createInputType(uniqueInt(), inputBasicTypeInfo);
		BasicTypeInfo outputInputType = mock(BasicTypeInfo.class);
		AggregationFunction function = createAggregationFunctionWithResultType(
				uniqueInt(), ResultTypeBehavior.FIXED, outputInputType);
		
		// when
		TypeInformation<?> resultType = typeFactory.createAggregationResultType(tupleTypeInfo, function);

		// then
		assertThat(resultType, tupleWithTypes(outputInputType));
	}

	@SuppressWarnings({ "rawtypes" })
	@Test
	public void shouldCreateTupleWithSingleElementGivenByInput() {
		// given
		int pos = uniqueInt();
		BasicTypeInfo basicTypeInfo = mock(BasicTypeInfo.class);
		TupleTypeInfoBase<Tuple> tupleTypeInfo = createInputType(pos, basicTypeInfo);
		AggregationFunction function = createAggregationFunctionWithResultType(
				pos, ResultTypeBehavior.INPUT, basicTypeInfo);
		
		// when
		TypeInformation<?> resultType = typeFactory.createAggregationResultType(tupleTypeInfo, function);

		// then
		assertThat(resultType, tupleWithTypes(basicTypeInfo));
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void shouldCreateTupleWithMultipleElements() {
		// given
		int elements = 3;
		TupleTypeInfoBase inputType = mock(TupleTypeInfoBase.class);
		BasicTypeInfo[] types = new BasicTypeInfo[elements];
		AggregationFunction[] functions = new AggregationFunction[elements];
		for (int i = 0; i < elements; ++i) {
			types[i] = mock(BasicTypeInfo.class);
			functions[i] = mock(AggregationFunction.class);
			setAggregationResultTypeAtPosition(i, functions[i], types[i], ResultTypeBehavior.FIXED);
			given(inputType.getTypeAt(i)).willReturn(types[i]);
		}

		// when
		TypeInformation<?> resultType = typeFactory.createAggregationResultType(inputType, functions);

		// then
		assertThat(resultType, tupleWithTypes(types));
	}

	@SuppressWarnings("rawtypes")
	@Test(expected=IllegalArgumentException.class)
	public void errorIf0ElementsInOutputTuple() {
		// given
		int elements = 0;
		AggregationFunction[] functions = new AggregationFunction[elements];
		
		// when
		typeFactory.createAggregationResultType(null, functions);
	}
	
	@SuppressWarnings("rawtypes")
	@Test(expected=IllegalArgumentException.class)
	public void errorIfMoreThanTupleArityElementsInOutputTuple() {
		// given
		int elements = uniqueInt(Tuple.MAX_ARITY + 1, Tuple.MAX_ARITY + 10); // upper bound to reduce heap size  
		AggregationFunction[] functions = new AggregationFunction[elements];
		
		// when
		typeFactory.createAggregationResultType(null, functions);
	}
	
	@Test
	public void shouldCreateResultTuple() {
		// given
		int arity = uniqueInt(1, Tuple.MAX_ARITY + 1);

		// when
		Tuple result = tupleFactory.createResultTuple(arity);

		// then
		assertThat(result.getArity(), is(arity));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void errorIfResultTupleTooLarge() {
		// given
		int arity = uniqueInt(Tuple.MAX_ARITY + 2, Tuple.MAX_ARITY + 10);

		// when
		tupleFactory.createResultTuple(arity);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void shouldCreateAgregationOperatorWithResultTypeAndTuple() throws Exception {
		// given
		// setup DataSet with type
		DataSet input = mock(DataSet.class);
		TypeInformation inputType = mock(TypeInformation.class);
		given(input.getType()).willReturn(inputType);

		// setup aggregation functions
		AggregationFunction[] functions = { mock(AggregationFunction.class) };

		// setup creation of result type
		ResultTypeFactory typeFactory = mock(ResultTypeFactory.class);
		factory.setResultTypeFactory(typeFactory);
		TypeInformation resultType = mock(TypeInformation.class);
		int arity = uniqueInt(1, Tuple.MAX_ARITY);
		given(resultType.getArity()).willReturn(arity);
		given(typeFactory.createAggregationResultType(inputType, functions)).willReturn(resultType);

		// setup creation of result tuple
		ResultTupleFactory tupleFactory = mock(ResultTupleFactory.class);
		factory.setResultTupleFactory(tupleFactory);
		Tuple resultTuple = mock(Tuple.class);
		given(tupleFactory.createResultTuple(arity)).willReturn(resultTuple);

		// setup creation of aggregation operator
		AggregationOperator expected = mock(AggregationOperator.class);
		whenNew(AggregationOperator.class).withArguments(input, resultType, resultTuple, functions).thenReturn(expected);
		
		// when
		AggregationOperator actual = factory.aggregate(input, functions);

		// then
		assertThat(actual, is(expected));
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TupleTypeInfoBase<Tuple> createInputType(int pos, BasicTypeInfo inputBasicTypeInfo) {
		TupleTypeInfoBase<Tuple> tupleTypeInfo = mock(TupleTypeInfoBase.class);
		given(tupleTypeInfo.getTypeAt(pos)).willReturn(inputBasicTypeInfo);
		return tupleTypeInfo;
	}
	
	@SuppressWarnings("rawtypes")
	private AggregationFunction createAggregationFunctionWithResultType(int pos,
			ResultTypeBehavior behavior, BasicTypeInfo outputInputType) {
		AggregationFunction function = mock(AggregationFunction.class);
		setAggregationResultTypeAtPosition(pos, function, outputInputType,
				behavior);
		return function;
	}

	@SuppressWarnings("rawtypes")
	private void setAggregationResultTypeAtPosition(int i,
			AggregationFunction function, BasicTypeInfo type,
			ResultTypeBehavior bevavior) {
		given(function.getResultTypeBehavior()).willReturn(bevavior);
		given(function.getResultType()).willReturn(type);
		given(function.getFieldPosition()).willReturn(i);
	}
	
	private Matcher<TypeInformation<?>> tupleWithTypes(final TypeInformation<?>... types) {
		return new TypeSafeMatcher<TypeInformation<?>>() {

			@Override
			public void describeTo(Description description) {
				description.appendText("Java Tuple");
				description.appendText(String.valueOf(types.length));
				description.appendText("<");
				description.appendText(StringUtils.join(types, ", "));
				description.appendText(">");
			}

			@Override
			protected void describeMismatchSafely(TypeInformation<?> item,
					Description mismatchDescription) {
				mismatchDescription.appendText("was: ");
				mismatchDescription.appendText(String.valueOf(item));
			}
			
			@Override
			protected boolean matchesSafely(TypeInformation<?> item) {
				if ( ! item.isTupleType() ) {
					return false;
				}
				TupleTypeInfo<?> itemAsTuple = (TupleTypeInfo<?>) item;
				int arity = itemAsTuple.getArity();
				if ( arity != types.length ) {
					return false;
				}
				for (int i = 0; i < arity; ++i) {
					TypeInformation<?> fieldType = itemAsTuple.getTypeAt(i);
					if ( ! fieldType.equals(types[i]) ) {
						return false;
					}
				}
				return true;
			}
		};
	}
}
