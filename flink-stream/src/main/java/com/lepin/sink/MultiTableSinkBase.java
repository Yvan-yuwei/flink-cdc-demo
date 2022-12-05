package com.lepin.sink;

import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class MultiTableSinkBase {

    private static final String HIVE_CATALOG = "hive_catalog";

    public abstract void doSink(ParameterTool parameterTool, DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataStreamSource);

    /**
     * sink iceberg
     *
     * @param parameterTool
     * @param apply
     * @param tableName
     */
    protected void sinkIceberg(ParameterTool parameterTool, DataStream<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> apply,
                               String tableName) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("catalog-type", "hive");
        properties.put("property-version", "1");
        properties.put("warehouse", parameterTool.get("warehouse"));
        properties.put("uri", parameterTool.get("uri"));

        CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, new Configuration(), properties);
        Catalog catalog = catalogLoader.loadCatalog();

        TableIdentifier identifier = TableIdentifier.of(Namespace.of("dwd"), tableName);

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        Table table = catalog.loadTable(identifier);

        final RowType rowType = FlinkSchemaUtil.convert(table.schema());
        final JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                        false, false, TimestampFormat.ISO_8601);

        final SingleOutputStreamOperator<RowData> ds = apply
                .map(value -> jsonRowDataDeserializationSchema.deserialize(value.getJson().getBytes(StandardCharsets.UTF_8)), jsonRowDataDeserializationSchema.getProducedType());

        FlinkSink.forRowData(ds)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                .writeParallelism(1)
                .upsert(Boolean.TRUE)
                .append();
    }
}
