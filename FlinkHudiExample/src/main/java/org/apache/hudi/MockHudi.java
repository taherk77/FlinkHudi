package org.apache.hudi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MockHudi {

  private static final String checkPointPath="file://<enter some path here>";
  private static final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
  public static void main(String[] args) throws Exception {

    setStateConfig();
    DataStreamSource<String> source = env.fromCollection(dataGenerator(100));
    SingleOutputStreamOperator<MockedHoodieRecord<String>> records = source.map(new ConverToHudiRecord());
    SingleOutputStreamOperator<MockedHoodieRecord<String>> taggedRecords = records.keyBy(new HudiKeySelector())
        .process(new StatefulSparkCacheMockFunction());
    taggedRecords.print();

    env.execute();
  }

  private static void setStateConfig() throws IOException {

    /*To keep records in RocksDB, Please uncomment the line below
    If below lines are uncommented then all data cached in the StatefulSparkCacheMockFunction
     will be stored in RocksDB. No external DB is required. Each TaskManager manages its own RocksDB off Heap
     and will flush data after certain interval to disk*/
    env.setStateBackend((StateBackend) new RocksDBStateBackend(checkPointPath));

    /*To keep records in memory, Please uncomment the line below
    If below lines are uncommented then all data cached in the StatefulSparkCacheMockFunction
     will be stored on TaskManagers heap and will be deleted on job shutdown*/
    //env.setStateBackend((StateBackend) new MemoryStateBackend());

    /*To keep records in on FS(hdfs, s3 etc), Please uncomment the line below
    If below lines are uncommented then all data cached in the StatefulSparkCacheMockFunction
     will be written to FS. Each TaskManager writes his own data to FS upon receiving checkpoint notification*/
    //env.setStateBackend((StateBackend)new FsStateBackend(checkPointPath));

  }

  private static List<String> dataGenerator(int numRecords)
  {
    Random random = new Random();
    List<String> contract = Arrays.asList("FUT","OPT","SPR");
    List<String>countries=Arrays.asList("AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BH","BS","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BQ","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CW","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE");
    List<Integer>contractId=new ArrayList<>();
    IntStream.range(100,150).forEach(v->contractId.add(v));
    System.out.println(contractId.size());
    return IntStream.range(0, numRecords).mapToObj(r->
        contractId.get(random.nextInt(contractId.size())) + "," +
        countries.get(random.nextInt(countries.size())) + "," +
        contract.get(random.nextInt(contract.size()))+ "," +
        System.currentTimeMillis() + random.nextInt(1000)
    ).collect(Collectors.toList());


  }
}
