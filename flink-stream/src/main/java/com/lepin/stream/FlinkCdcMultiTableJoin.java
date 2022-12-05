package com.lepin.stream;

import com.lepin.env.DataStreamEnv;
import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import com.lepin.sink.ContactBillMappingDataSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkCdcMultiTableJoin {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataStreamSource = DataStreamEnv.getTableIRowKindJsonDataStreamSource(parameterTool, env);

        log.warn("start flink cdc multi table");

        // contract_bill_mapping_data
        new ContactBillMappingDataSink().doSink(parameterTool, dataStreamSource);

        log.warn("start flink cdc multi table end");

        env.execute("sync mysql table to iceberg");
    }
}
