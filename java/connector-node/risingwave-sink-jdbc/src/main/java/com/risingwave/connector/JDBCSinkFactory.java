package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import io.grpc.Status;
import java.util.Map;

public class JDBCSinkFactory implements SinkFactory {
    public static final String JDBC_URL_PROP = "jdbc.url";
    public static final String TABLE_NAME_PROP = "table.name";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        if (!tableProperties.containsKey(JDBC_URL_PROP)
                || !tableProperties.containsKey(TABLE_NAME_PROP)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "%s or %s is not specified", JDBC_URL_PROP, TABLE_NAME_PROP))
                    .asRuntimeException();
        }

        String tableName = tableProperties.get(TABLE_NAME_PROP);
        String jdbcUrl = tableProperties.get(JDBC_URL_PROP);
        return new JDBCSink(tableName, jdbcUrl, tableSchema);
    }
}
