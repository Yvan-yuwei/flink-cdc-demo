package com.lepin.env;

import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import com.lepin.source.MysqlCdcSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class DataStreamEnv {

    public static DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> getTableIRowKindJsonDataStreamSource(ParameterTool parameterTool, StreamExecutionEnvironment env) {
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60 * 1000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        checkpointConfig.setCheckpointTimeout(120 * 1000L);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        log.warn("start mysql iceberg ");

        return env.fromSource(MysqlCdcSource.getMysqlCdcSource(parameterTool), WatermarkStrategy.noWatermarks(), "mysql cdc");
    }
}
