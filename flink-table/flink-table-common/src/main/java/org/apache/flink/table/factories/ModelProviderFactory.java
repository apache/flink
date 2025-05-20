package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ml.ModelProvider;

public interface ModelProviderFactory {

    /** Create ModelRuntime based on connector. */
    ModelProvider createModelProvider(Context context);

    /** Provides catalog and session information describing the model to be accessed. */
    @PublicEvolving
    interface Context {
        /**
         * Returns the identifier of the model in the {@link Catalog}.
         *
         * <p>This identifier describes the relationship between the model instance and the
         * associated {@link Catalog} (if any).
         */
        ObjectIdentifier getObjectIdentifier();

        /**
         * Returns the resolved model information received from the {@link Catalog} or persisted
         * plan.
         *
         * <p>The {@link ResolvedCatalogModel} forwards the metadata from the catalog but offers a
         * validated {@link ResolvedSchema}. The original metadata object is available via {@link
         * ResolvedCatalogModel#getOrigin()}.
         *
         * <p>In most cases, a factory is interested in the following characteristics:
         *
         * <pre>{@code
         * // get the physical input and output data type to initialize the connector
         * context.getCatalogModel().getResolvedInputSchema().toPhysicalRowDataType()
         * context.getCatalogModel().getResolvedInputSchema().toPhysicalRowDataType()
         *
         * // get configuration options
         * context.getCatalogModel().getOptions()
         * }</pre>
         *
         * <p>During a plan restore, usually the model information persisted in the plan is used to
         * reconstruct the catalog model.
         */
        ResolvedCatalogModel getCatalogModel();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();

        /** Whether the model is temporary. */
        boolean isTemporary();
    }
}
