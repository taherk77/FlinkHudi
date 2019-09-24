package org.apache.hudi;

import org.apache.flink.api.common.functions.MapFunction;

public class ConverToHudiRecord implements MapFunction<String,MockedHoodieRecord<String>> {

  @Override
  public MockedHoodieRecord<String> map(String value) throws Exception {
    //first token is key and rest is set as data
    String[] split = value.split(",");
      return new MockedHoodieRecord<>(split[0], value);
  }
}
