package com.flink.hudi.example;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new UserScoreDataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(10L)) {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Integer> userAndScore) {
                        return userAndScore.f0;
                    }
                })
                .keyBy(record -> record.f1)
                .timeWindow(Time.seconds(1))
                .allowedLateness(Time.seconds(1))
                .process(new ProcessWindowFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    MapStateDescriptor<String, Integer> scoreStateDescriptor = new MapStateDescriptor<String, Integer>("scoreState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
                    MapState<String, Integer> scoreState;

                    @Override
                    public void process(String user, Context context, Iterable<Tuple3<Long, String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        scoreState = context.globalState().getMapState(scoreStateDescriptor);
                        Iterator<Tuple3<Long, String, Integer>> iterator = iterable.iterator();
                        int sum = 0;
                        while (iterator.hasNext()) {
                            sum = sum + iterator.next().f2;
                        }
                        if (!scoreState.contains(user)) {
                            scoreState.put(user, sum);
                            collector.collect(Tuple2.of(user, sum));
                        } else {
                            int score = scoreState.get(user) + sum;
                            scoreState.put(user, score);
                            collector.collect(Tuple2.of(user, score));
                        }

                    }
                }).print();
        env.execute();
    }
}
