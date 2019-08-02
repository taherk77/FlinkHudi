package com.flink.hudi.example.timestamp.extractor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ExtractTimestamp extends BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long,String,Integer>>{

    public ExtractTimestamp(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long getMaxOutOfOrdernessInMillis() {
        return super.getMaxOutOfOrdernessInMillis();
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, Integer> element) {
        return element.f0;
    }
}
