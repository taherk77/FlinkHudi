package com.flink.hudi.example;

import com.flink.hudi.example.source.UserScoreDataSource;
import com.flink.hudi.example.timestamp.extractor.ExtractTimestamp;
import com.flink.hudi.example.operator.SumUserScores;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new UserScoreDataSource())
                .assignTimestampsAndWatermarks(new ExtractTimestamp(Time.milliseconds(10L)))
                .keyBy(record -> record.f1)
                .timeWindow(Time.seconds(1))
                .allowedLateness(Time.seconds(1))
                .process(new SumUserScores()).print();
        env.execute();
    }
}
