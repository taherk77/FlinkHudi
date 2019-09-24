package org.apache.hudi;

import org.apache.flink.api.java.functions.KeySelector;

public class HudiKeySelector implements KeySelector<MockedHoodieRecord<String>, String> {

  @Override
  public String getKey(MockedHoodieRecord<String> value) throws Exception {
    //user by keyBy() for data partitioning
    return value.getKey();
  }
}
