package com.lepin.sink;

import com.google.common.collect.Maps;
import com.lepin.catalog.MysqlCatalog;
import com.lepin.enums.TypesToSchemaEnums;
import com.lepin.options.DbSourceOptions;
import com.lepin.options.IcebergSinkOptions;
import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class MysqlSinkIceberg {

    /**
     * hive catalog
     */
    private static final String HIVE_CATALOG = "hive_catalog";

    /**
     * mysql table info map
     */
    public static final Map<String, RowTypeInfo> TABLE_TYPE_INFORMATION_MAP = Maps.newConcurrentMap();
//    private static final Map<String, DataType[]> TABLE_DATA_TYPES_MAP = Maps.newConcurrentMap();

    public static final Map<String, OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>> OUTPUT_TAG_MAP
            = Maps.newConcurrentMap();

    /**
     * sink iceberg
     *
     * @param parameterTool
     * @param streamSource
     */
    public void sinkIceberg(ParameterTool parameterTool, DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> streamSource) {
        //side output
        final SingleOutputStreamOperator<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> mainDataStream = sideOutputStream(streamSource);

        //do sink iceberg
        TABLE_TYPE_INFORMATION_MAP.entrySet().forEach(v -> doHandle(parameterTool, mainDataStream, v));
    }

    /**
     * sink table 处理逻辑
     *
     * @param parameterTool
     * @param mainDataStream
     * @param entry
     */
    private void doHandle(ParameterTool parameterTool, SingleOutputStreamOperator<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> mainDataStream, Map.Entry<String, RowTypeInfo> entry) {
        String tableName = entry.getKey();
        RowTypeInfo rowTypeInfo = entry.getValue();
        String icebergTableName = String.format("ods_sync_%s", tableName);

        String[] fieldNames = rowTypeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

        List<Types.NestedField> nestedField = Lists.newArrayList();
        for (int i = 0; i < fieldNames.length; i++) {
            TypeInformation<?> fieldType = fieldTypes[i];
            Type type = Optional.ofNullable(TypesToSchemaEnums.of(String.valueOf(fieldType))).orElse(Types.StringType.get());
            if (Objects.equals(fieldNames[i], "id")) {
                nestedField.add(Types.NestedField.required(i + 1, fieldNames[i], type));
            } else {
                nestedField.add(Types.NestedField.optional(i + 1, fieldNames[i], type));
            }
        }

        Schema schema = new Schema(nestedField);

        OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> outputTag = OUTPUT_TAG_MAP.get(tableName);
        if(Objects.isNull(outputTag)){
            log.warn("out put tag is empty, table name is:{}", tableName);
            return;
        }
        DataStream<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> tableDataStream = mainDataStream.getSideOutput(outputTag);

        RowType rowType = FlinkSchemaUtil.convert(schema);
        JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                        false, false, TimestampFormat.ISO_8601);

        SingleOutputStreamOperator<RowData> sinkDataStream = tableDataStream
                .map(value -> {
                    RowData rowData =
                            jsonRowDataDeserializationSchema.deserialize(value.getJson().getBytes(StandardCharsets.UTF_8));
                    rowData.setRowKind(value.getRowKind());
                    return rowData;
                }, jsonRowDataDeserializationSchema.getProducedType());

        icebergSinkHive(sinkDataStream, parameterTool, icebergTableName, schema);
    }

    /**
     * hive catalog loader
     *
     * @param streamOperator
     * @param tool
     * @param icebergTableName
     * @param schema
     */
    private static void icebergSinkHive(SingleOutputStreamOperator<RowData> streamOperator, ParameterTool tool,
                                        String icebergTableName, Schema schema) {
        //配置 catalog 和 table
        CatalogLoader catalogLoader = catalogConfiguration(tool);
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(tool.get(IcebergSinkOptions.HIVE_DB.key())), icebergTableName);

        Table table = hiveTableFromCatalog(schema, catalog, identifier);

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        icebergSink(streamOperator, table, tableLoader);
    }

    /**
     * sink iceberg
     *
     * @param streamOperator
     * @param table
     * @param tableLoader
     */
    private static void icebergSink(SingleOutputStreamOperator<RowData> streamOperator, Table table, TableLoader tableLoader) {
        FlinkSink.forRowData(streamOperator)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                .writeParallelism(1)
                .upsert(Boolean.TRUE)
                .append();
    }

    /**
     * flink stream 旁路输出构建map
     *
     * @param src
     */
    private SingleOutputStreamOperator<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> sideOutputStream(DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> src) {
        return src.process(new ProcessFunction<RowKindJsonDeserializationSchemaBase.TableIRowKindJson, RowKindJsonDeserializationSchemaBase.TableIRowKindJson>() {
            @Override
            public void processElement(RowKindJsonDeserializationSchemaBase.TableIRowKindJson tableIRowKindJson, ProcessFunction<RowKindJsonDeserializationSchemaBase.TableIRowKindJson, RowKindJsonDeserializationSchemaBase.TableIRowKindJson>.Context context, Collector<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> collector) throws Exception {
                final String table = tableIRowKindJson.getTable();
                final OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> outputTag = OUTPUT_TAG_MAP.get(table);
//                log.warn("out put tag is:{}, table name is:{}", outputTag, table);
                if(Objects.nonNull(outputTag)) {
                    context.output(outputTag, tableIRowKindJson);
                }
            }
        });
    }

    /**
     * 初始化 mysql table
     *
     * @param tool
     * @throws Exception
     */
    public void initTable(ParameterTool tool) throws Exception {
        MysqlCatalog mysqlCatalog = new MysqlCatalog(DbSourceOptions.MYSQL_CATALOG.key(),
                tool.get(DbSourceOptions.DATABASE_NAME.key()),
                tool.get(DbSourceOptions.USERNAME.key()),
                tool.get(DbSourceOptions.PASSWORD.key()),
                String.format("jdbc" + ":mysql://%s:%d", tool.get(DbSourceOptions.HOSTNAME.key()),
                        Optional.ofNullable(tool.get(DbSourceOptions.PORT.key())).map(Integer::parseInt).orElse(3306)));
        List<String> tables = mysqlCatalog.listTables(tool.get(DbSourceOptions.DATABASE_NAME.key()));

        if (Objects.nonNull(tool.get(DbSourceOptions.TABLE_NAME.key()))) {
            String[] tableList = tool.get(DbSourceOptions.TABLE_NAME.key()).split(",");
            tables = Arrays.asList(tableList);
        }

        //测试用的不需要
//        String nonSkipTable = tool.get("non_skip_table");

        for (String table : tables) {
//            if (Objects.nonNull(nonSkipTable) && !table.contains(tool.get("non_skip_table"))) {
//                continue;
//            }
            // 获取mysql catalog中注册的表
            ObjectPath objectPath = new ObjectPath(tool.get(DbSourceOptions.DATABASE_NAME.key()), table);
            CatalogBaseTable catalogBaseTable = mysqlCatalog.getTable(objectPath);
            // 获取表的Schema
            TableSchema schema = catalogBaseTable.getSchema();
            // 获取表中字段名列表
            String[] fieldNames = schema.getFieldNames();
            // 获取表字段类型
            TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
            // 组装sink表ddl sql
            String tableName = table.split("\\.")[1];
//            DataType[] fieldDataTypes = schema.getFieldDataTypes();
//            TABLE_DATA_TYPES_MAP.put(tableName, fieldDataTypes);
            TABLE_TYPE_INFORMATION_MAP.put(tableName, new RowTypeInfo(fieldTypes, fieldNames));
        }
    }

    /**
     * 后续会有创建表的任务，这层之后不需要
     *
     * @param schema
     * @param catalog
     * @param identifier
     * @return
     */
    @NotNull
    private static Table hiveTableFromCatalog(Schema schema, Catalog catalog, TableIdentifier identifier) {
        Table table;
        if (catalog.tableExists(identifier)) {
            table = catalog.loadTable(identifier);
        } else {
            /**
             * 'format-version'='2',
             *     'iceberg.mr.catalog'='hive',
             *     'engine.hive.enabled'='true',
             *     'write.upsert.enabled'='true',
             *     'write.metadata.previous-versions-max'='3',
             *     'hive.vectorized.execution.enabled'='false',
             *     'write.metadata.delete-after-commit.enabled'='true',
             *     'warehouse'='hdfs://nameservice1/user/hive/warehouse/ods/ods_mysql_lepin_instalments_credit_order'
             */
            Map<String, String> prop = new HashMap<>();
            prop.put("format-version", "2");
            prop.put("iceberg.mr.catalog", "hive");
            prop.put("engine.hive.enabled", "true");
            prop.put("write.upsert.enabled", "true");
            prop.put("write.metadata.previous-versions-max", "3");
            prop.put("hive.vectorized.execution.enabled", "false");
            prop.put("write.metadata.delete-after-commit.enabled", "true");
            table = catalog.buildTable(identifier, schema)
                    .withPartitionSpec(PartitionSpec.unpartitioned())
                    .withProperties(prop)
                    .create();

        }
        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'
        // table.schema().identifierFieldIds();
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));
        return table;
    }

    @NotNull
    private static CatalogLoader catalogConfiguration(ParameterTool tool) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("catalog-type", "hive");
        properties.put("property-version", "1");
        properties.put("warehouse", tool.get(IcebergSinkOptions.WAREHOUSE.key()));
        properties.put("uri", tool.get(IcebergSinkOptions.URI.key()));

        CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, new Configuration(), properties);
        return catalogLoader;
    }
}
