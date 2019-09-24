package org.apache.hudi;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StatefulSparkCacheMockFunction extends KeyedProcessFunction<String,MockedHoodieRecord<String>,MockedHoodieRecord<String>> {

  //state MapStateDescriptor<String,String> saves hoodie key and hoodie partitionPath
  private final MapStateDescriptor<String,String> hudiStateDescriptor= new MapStateDescriptor<String, String>("mockedSparkCache",
      TypeInformation.of(String.class),TypeInformation.of(String.class));

  private MapState<String, String> hudiState;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    hudiState =getRuntimeContext().getMapState(hudiStateDescriptor);
  }

  public StatefulSparkCacheMockFunction() {
    super();
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<MockedHoodieRecord<String>> out) throws Exception {
    super.onTimer(timestamp, ctx, out);
  }

  @Override
  public void processElement(MockedHoodieRecord<String> record, Context ctx, Collector<MockedHoodieRecord<String>> collector)
      throws Exception {

    //add to state same as cache
    addToState(record);
    //do some processing
    collector.collect(record);
   // hudiState.remove(record.getKey());

  }

  private void addToState(MockedHoodieRecord<String> record) throws Exception {
  hudiState.put(record.getKey(),record.getPartitionPath());
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}
