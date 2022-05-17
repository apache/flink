package org.apache.flink.streaming.api.transformations.python;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** . */
public class PythonBroadcastStateTransformation<IN1, IN2, OUT>
        extends AbstractBroadcastStateTransformation<IN1, IN2, OUT> {

    private final Configuration configuration;

    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    public PythonBroadcastStateTransformation(
            String name,
            Configuration configuration,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            Transformation<IN1> regularInput,
            Transformation<IN2> broadcastInput,
            List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
            TypeInformation<OUT> outTypeInfo,
            int parallelism) {
        super(
                name,
                regularInput,
                broadcastInput,
                broadcastStateDescriptors,
                outTypeInfo,
                parallelism);
        this.configuration = configuration;
        this.pythonFunctionInfo = pythonFunctionInfo;
        updateManagedMemoryStateBackendUseCase(false);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
        return pythonFunctionInfo;
    }

    public static List<MapStateDescriptor<ByteArrayWrapper, byte[]>>
            convertStateNamesToStateDescriptors(Collection<String> names) {
        List<MapStateDescriptor<ByteArrayWrapper, byte[]>> descriptors =
                new ArrayList<>(names.size());
        TypeSerializer<byte[]> byteArraySerializer =
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.createSerializer(
                        new ExecutionConfig());
        for (String name : names) {
            descriptors.add(
                    new MapStateDescriptor<>(
                            name, ByteArrayWrapperSerializer.INSTANCE, byteArraySerializer));
        }
        return descriptors;
    }
}
