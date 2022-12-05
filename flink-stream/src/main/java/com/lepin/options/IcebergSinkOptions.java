package com.lepin.options;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergSinkOptions {

    public static final ConfigOption<String> WAREHOUSE =
            ConfigOptions.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hdfs warehouse. eg: hdfs://nn-1:8020/user/hive/warehouse/");

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("thrift. eg: thrift://nn-1:9083");

    public static final ConfigOption<String> HIVE_DB =
            ConfigOptions.key("hive_db")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hive db name of the db to sink");

}
