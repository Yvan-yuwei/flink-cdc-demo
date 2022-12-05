package com.lepin.stream;

import com.lepin.env.DataStreamEnv;
import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import com.lepin.sink.MysqlSinkIceberg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

@Slf4j
public class Mysql2Iceberg {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //--username root --password 123456 --hostname 192.168.15.117 --database_name xxx --warehouse hdfs://nn-1:8020/user/hive/warehouse/ --uri
        // thrift://nn-1:9083 --hive_db xxx --table_name xxx,xxx

        final MysqlSinkIceberg mysqlSinkIceberg = new MysqlSinkIceberg();
        try {
            //init table
            mysqlSinkIceberg.initTable(parameterTool);
        } catch (Exception e) {
            log.error("init mysql table error", e);
        }

        if (MysqlSinkIceberg.TABLE_TYPE_INFORMATION_MAP.isEmpty()) {
            log.warn("table map is empty, return");
            return;
        }

        //init side output Map
        MysqlSinkIceberg.TABLE_TYPE_INFORMATION_MAP.forEach((k, v) -> {
            OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> outputTag =
                    new OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>(k) {
                    };
            MysqlSinkIceberg.OUTPUT_TAG_MAP.putIfAbsent(k, outputTag);
        });


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> src = DataStreamEnv.getTableIRowKindJsonDataStreamSource(parameterTool, env);

//        src.setParallelism(1).print("get data from binlog ===>");

        // mysql sink iceberg
        mysqlSinkIceberg.sinkIceberg(parameterTool, src);

        log.warn("start mysql iceberg end");

        env.execute("sync mysql table to iceberg");
    }
}
