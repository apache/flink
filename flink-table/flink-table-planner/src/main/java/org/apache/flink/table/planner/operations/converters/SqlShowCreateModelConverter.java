package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.dql.SqlShowCreateModel;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowCreateModelOperation;

import java.util.Optional;

/** A converter for {@link org.apache.flink.sql.parser.dql.SqlShowCreateModel}. */
public class SqlShowCreateModelConverter implements SqlNodeConverter<SqlShowCreateModel> {

    @Override
    public Operation convertSqlNode(SqlShowCreateModel showCreateModel, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(showCreateModel.getFullModelName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedModel> model = context.getCatalogManager().getModel(identifier);

        if (model.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not execute SHOW CREATE MODEL. Model with identifier %s does not exist.",
                            identifier.asSerializableString()));
        }

        return new ShowCreateModelOperation(
                identifier, model.get().getResolvedModel(), model.get().isTemporary());
    }
}
