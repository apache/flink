package org.apache.flink.runtime.checkpoint;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

import java.util.ArrayList;

// Kryo will default to using the CollectionSerializer for StateObjectCollection.
// The problem is that will instantiate new StateObjectCollection instances with null values
// which are invalid. This overrides the create method so that StateObjectCollection are
// created with normal constructors and an empty ArrayList of the right size.
public class StateObjectCollectionSerializer extends CollectionSerializer<StateObjectCollection<?>> {
    @Override
    protected StateObjectCollection<?> create(
            Kryo kryo,
            Input input,
            Class<? extends StateObjectCollection<?>> type,
            int size) {
        return new StateObjectCollection<>(new ArrayList<>(size));
    }
}
