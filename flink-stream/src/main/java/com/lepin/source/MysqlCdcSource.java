package com.lepin.source;

import com.lepin.options.DbSourceOptions;
import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import com.lepin.schema.RowKindJsonDeserializationSchemaV2;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Objects;
import java.util.Properties;

public class MysqlCdcSource {

    /**
     * mysql source cdc
     *
     * @param tool
     * @return
     */
    public static MySqlSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> getMysqlCdcSource(ParameterTool tool) {
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("skipped.operations", "d");
        String tableList = String.format("%s.*", tool.get(DbSourceOptions.DATABASE_NAME.key()));
        if (Objects.nonNull(tool.get(DbSourceOptions.TABLE_NAME.key()))) {
            tableList = tool.get(DbSourceOptions.TABLE_NAME.key());
        }
        MySqlSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> mySqlSource = MySqlSource.<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>builder()
                .debeziumProperties(dbzProperties)
//                .serverId("5400-5408")
                .hostname(tool.get(DbSourceOptions.HOSTNAME.key()))
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList(tool.get(DbSourceOptions.DATABASE_NAME.key()))
                .tableList(tableList)
//                .tableList(tool.get("table"))
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .username(tool.get(DbSourceOptions.USERNAME.key()))
                .password(tool.get(DbSourceOptions.PASSWORD.key()))
                .deserializer(new RowKindJsonDeserializationSchemaV2())
                .serverTimeZone("Asia/Shanghai")
                .build();
        return mySqlSource;
    }

    /**
     * mysql source cdc
     *
     * @param tool
     * @return
     */
    public static MySqlSource<String> getMysqlSource(ParameterTool tool) {
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("skipped.operations", "d");
        String tableList = String.format("%s.*", tool.get("db"));
        if (Objects.nonNull(tool.get("table_list"))) {
            tableList = tool.get("table_list");
        }
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .debeziumProperties(dbzProperties)
//                .serverId("5400-5408")
                .hostname(tool.get("host"))
                .port(3306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList(tool.get("db"))
                .tableList(tableList)
//                .tableList(tool.get("table"))
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(true)
                .username(tool.get("user"))
                .password(tool.get("password"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .build();
        return mySqlSource;
    }

}
