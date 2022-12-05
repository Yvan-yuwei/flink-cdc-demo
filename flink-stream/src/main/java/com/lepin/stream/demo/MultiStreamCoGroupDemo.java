package com.lepin.stream.demo;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;


/**
 * CoGroup
 * 侧重与group，对同一个key上的两组集合进行操作
 */
@Slf4j
public class MultiStreamCoGroupDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Tuple2<String, String>> input = env.socketTextStream("localhost", 9000)
                .map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2.of(s.split(",")[0], s.split(",")[1]));
        DataStream<Tuple2<String, String>> input2 = env.socketTextStream("localhost", 9001)
                        .map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2.of(s.split(",")[0], s.split(",")[1]));


        input.coGroup(input2).where((KeySelector<Tuple2<String, String>, Object>) value -> value.f0)
                .equalTo((KeySelector<Tuple2<String, String>, Object>) value -> value.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
                        .trigger(CountTrigger.of(1)).apply((CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, Object>) (iterable, iterable1, collector) -> {
                            final StringBuffer buffer = new StringBuffer();
                            buffer.append("data stream first:\n");
                            for (Tuple2<String, String> value : iterable) {
                                buffer.append(value.f0).append("=>").append(value.f1).append("\n");
                            }
                            buffer.append("data stream second:\n");
                            for (Tuple2<String, String> value : iterable1) {
                                buffer.append(value.f0).append("=>").append(value.f1);
                            }
                            collector.collect(buffer);
                        }).print();

        env.execute("test");

        /*

        1> data stream first:
        data stream second:
        2222=>ixao
        5> data stream first:
        1111=>xiao
        data stream second:
        1111=>iopi
        1> data stream first:
        2222=>rere
        data stream second:

        1> data stream first:
        2222=>rere
        data stream second:
        2222=>teee

         */
    }
}
