package org.apache.flink.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * convert result list to an array T[]
 *
 * @author leochu
 * @date 2024/8/14
 */
public class ArrayAggr<T> extends AggregateFunction<T[], ArrayAccumulator<T>> {

    private DataType elementType;

    @Override
    public T[] getValue(ArrayAccumulator<T> tArrayAccumulator) {
        if (tArrayAccumulator.values.getList().isEmpty()) {
            return null;
        }
        List<T> values = new ArrayList<T>(tArrayAccumulator.values.getList());
        return values.toArray((T[]) Array.newInstance(
                elementType.getConversionClass(),
                values.size()));
    }

    @Override
    public ArrayAccumulator<T> createAccumulator() {
        return new ArrayAccumulator<>();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(InputTypeStrategies.ANY))
                .accumulatorTypeStrategy(new TypeStrategy() {
                    @Override
                    public Optional<DataType> inferType(CallContext callContext) {
                        return Optional.of(DataTypes.STRUCTURED(
                                ArrayAccumulator.class,
                                DataTypes.FIELD(
                                        "values",
                                        ListView.newListViewDataType(callContext
                                                .getArgumentDataTypes()
                                                .get(0)))));
                    }
                })
                .outputTypeStrategy(new TypeStrategy() {
                    @Override
                    public Optional<DataType> inferType(CallContext callContext) {
                        elementType = callContext.getArgumentDataTypes().get(0);
                        return Optional.of(DataTypes.ARRAY(elementType));
                    }
                })
                .build();
    }

    public void accumulate(ArrayAccumulator<T> tArrayAccumulator, T t) throws Exception {
        if (t != null) {
            tArrayAccumulator.values.add(t);
        }
    }

    public void retract(ArrayAccumulator<T> tArrayAccumulator, T t) throws Exception {
        if (t != null) {
            tArrayAccumulator.values.remove(t);
        }
    }
}
