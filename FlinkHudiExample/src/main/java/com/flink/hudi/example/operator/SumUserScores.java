package com.flink.hudi.example.operator;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class SumUserScores extends ProcessWindowFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
    private MapStateDescriptor<String, Integer> scoreStateDescriptor = new MapStateDescriptor<String, Integer>("scoreState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
    private MapState<String, Integer> scoreState;

    @Override
    public void clear(Context context) throws Exception {
        super.clear(context);
    }

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
}
