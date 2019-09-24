package org.apache.hudi;

import org.apache.flink.shaded.guava18.com.google.common.base.Objects;

public class MockedHoodieRecord<T> {

  private String key;

  private T data;

  private String currentLocation;

  private String newLocation;

  public MockedHoodieRecord(String key, T data) {
    this.key = key;
    this.data = data;
    this.currentLocation = null;
    this.newLocation = null;
  }

  public MockedHoodieRecord(MockedHoodieRecord<T> record) {
    this(record.key, record.data);
    this.currentLocation = record.currentLocation;
    this.newLocation = record.newLocation;
  }

  public String getKey() {
    return key;
  }

  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }
  public void deflate() {
    this.data = null;
  }


  public MockedHoodieRecord<T> setCurrentLocation(String location) {
    assert currentLocation == null;
    this.currentLocation = location;
    return this;
  }

  public String getCurrentLocation() {
    return currentLocation;
  }

  /**
   * Sets the new currentLocation of the record, after being written. This again should happen
   * exactly-once.
   */
  public MockedHoodieRecord<T> setNewLocation(String location) {
    assert newLocation == null;
    this.newLocation = location;
    return this;
  }

  public boolean isCurrentLocationKnown() {
    return this.currentLocation != null;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockedHoodieRecord that = (MockedHoodieRecord) o;
    return Objects.equal(key, that.key)
        && Objects.equal(data, that.data)
        && Objects.equal(currentLocation, that.currentLocation)
        && Objects.equal(newLocation, that.newLocation);
  }
  @Override
  public int hashCode() {
    return Objects.hashCode(key, data, currentLocation, newLocation);
  }

  public static String generateSequenceId(String commitTime, int partitionId, long recordIndex) {
    return commitTime + "_" + partitionId + "_" + recordIndex;
  }

  public String getPartitionPath() {
    assert key != null;
    return currentLocation;
  }

  public String getRecordKey() {
    assert key != null;
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setData(T data) {
    this.data = data;
  }

  @Override
  public String toString() {
    return "MockedHoodieRecord{" +
        "key='" + key + '\'' +
        ", data=" + data +
        ", currentLocation='" + currentLocation + '\'' +
        ", newLocation='" + newLocation + '\'' +
        '}';
  }
}
