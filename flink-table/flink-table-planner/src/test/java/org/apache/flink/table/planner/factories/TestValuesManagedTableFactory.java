package org.apache.flink.table.planner.factories;

import org.apache.flink.table.factories.ManagedTableFactory;

import java.util.HashMap;
import java.util.Map;

/** Test implementation of {@link ManagedTableFactory}. */
public class TestValuesManagedTableFactory extends TestValuesTableFactory
        implements ManagedTableFactory {

    @Override
    public String factoryIdentifier() {
        return ManagedTableFactory.super.factoryIdentifier();
    }

    @Override
    public Map<String, String> enrichOptions(Context context) {
        return new HashMap<>(context.getCatalogTable().getOptions());
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {}

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {}
}
