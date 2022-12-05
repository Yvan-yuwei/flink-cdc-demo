package com.lepin.sink;

import com.alibaba.fastjson2.JSONObject;
import com.lepin.schema.RowKindJsonDeserializationSchemaBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

@Slf4j
public class ContactBillMappingDataSink extends MultiTableSinkBase {

    @Override
    public void doSink(ParameterTool parameterTool, DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataStreamSource) {
        final OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataOutputTag =
                new OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>("data") {
                };
        final OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataDetailOutputTag =
                new OutputTag<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>("data_detail") {
                };

        final SingleOutputStreamOperator<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> process = dataStreamSource.process(new ProcessFunction<RowKindJsonDeserializationSchemaBase.TableIRowKindJson, RowKindJsonDeserializationSchemaBase.TableIRowKindJson>() {
            @Override
            public void processElement(RowKindJsonDeserializationSchemaBase.TableIRowKindJson tableIRowKindJson,
                                       ProcessFunction<RowKindJsonDeserializationSchemaBase.TableIRowKindJson, RowKindJsonDeserializationSchemaBase.TableIRowKindJson>.Context context,
                                       Collector<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> out) throws Exception {


                final String table = tableIRowKindJson.getTable();
                // 发送数据到旁路输出
                if (Objects.equals(table, "data")) {
                    context.output(dataOutputTag, tableIRowKindJson);
                }

                if (Objects.equals(table, "data_detail")) {
                    JSONObject accountDetail = JSONObject.parseObject(tableIRowKindJson.getJson());
                    String dataId = accountDetail.getString("data_id");
                    if (Objects.nonNull(dataId)) {
                        context.output(dataDetailOutputTag, tableIRowKindJson);
                    }
                }
            }
        });

        final DataStream<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataStream = process.getSideOutput(dataOutputTag);
        final DataStream<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> dataDetailStream = process.getSideOutput(dataDetailOutputTag);

        final DataStream<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> apply = dataStream.join(dataDetailStream)
                .where(t -> {
                    final JSONObject account = JSONObject.parseObject(t.getJson());
                    return account.get("id");
                }).equalTo(t -> {
                    final JSONObject accountDetail = JSONObject.parseObject(t.getJson());
                    return accountDetail.get("data_id");
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((JoinFunction<RowKindJsonDeserializationSchemaBase.TableIRowKindJson, RowKindJsonDeserializationSchemaBase.TableIRowKindJson, RowKindJsonDeserializationSchemaBase.TableIRowKindJson>) (tableIRowKindJson, tableIRowKindJson2) -> {
                    final JSONObject jsonObject = new JSONObject();
                    final JSONObject account = JSONObject.parseObject(tableIRowKindJson.getJson());
                    final JSONObject account2 = JSONObject.parseObject(tableIRowKindJson2.getJson());

                    jsonObject.putAll(account);
                    jsonObject.putAll(account2);
                    final RowKindJsonDeserializationSchemaBase.TableIRowKindJson tableIRowKindJson1 = new RowKindJsonDeserializationSchemaBase.TableIRowKindJson();
                    tableIRowKindJson1.setJson(jsonObject.toString());
                    return tableIRowKindJson1;
                });


        apply.print("account join result ===> ");

        sinkIceberg(parameterTool, apply, "dwd_contract_bill_mapping_data");
    }
}
