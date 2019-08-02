package com.flink.hudi.example.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;


public class UserScoreDataSource extends RichSourceFunction<Tuple3<Long, String, Integer>> {
    private final List<String> users = new ArrayList<>();
    private final List<Integer> scores = new ArrayList<>();
    private final Random random = new Random();
    private int numUsers;
    private int numScores;
    private boolean shouldProduce = true;
    private long recordTimestamp;

    @Override
    public void open(Configuration parameters) throws Exception {
        fill();
    }

    @Override
    public void close() throws Exception {
        clear();
    }

    @Override
    public void run(SourceContext<Tuple3<Long, String, Integer>> sourceContext) throws Exception {
        //Tuple3<Long,String,Integer> Contains record time user for windowing, username and score  of the user
        recordTimestamp = System.currentTimeMillis();
        while (shouldProduce) {
            IntStream.range(0, 10).forEach(x -> {
                        recordTimestamp += 200L;
                        sourceContext.collect(
                                Tuple3.of(recordTimestamp,
                                        users.get(random.nextInt(numUsers)),
                                        scores.get(random.nextInt(numScores)))
                        );
                    }
            );
            Thread.sleep(3000L);
        }

    }

    @Override
    public void cancel() {
        try {
            shouldProduce = false;
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void fill() {
        //Fill usernames
        users.add("Vinoth");
        users.add("Balaji");
        users.add("Taher");
        users.add("Vinay");
        users.add("Vino");
        users.add("Nishith");
        numUsers = users.size();

        //Fill scores
        scores.add(1);
        scores.add(2);
        scores.add(3);
        scores.add(4);
        numScores = scores.size();

    }

    private void clear() {
        users.clear();
        scores.clear();
    }
}