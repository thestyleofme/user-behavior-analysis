package com.github.codingdebugallday.analysis.hotitems;

import java.io.*;
import java.util.Objects;

/**
 * <p>
 * 往kafka里灌数据
 * java -cp /data/flink/hot-item-analysis-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
 * com.github.codingdebugallday.analysis.hotitems.KafkaSenderFromLog 1000 /data/flink/UserBehavior.csv \
 * | /usr/hdp/3.1.0.0-78/kafka/bin/kafka-console-producer.sh \
 * --broker-list hdspdev001:6667,hdspdev002:6667,hdspdev003:6667 \
 * --topic hotitems
 * </p>
 *
 * @author isacc 2020/04/15 11:30
 * @since 1.0
 */
public class KafkaSenderFromLog {

    /*
     * java -cp /data/flink/hot-item-analysis-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.github.codingdebugallday.analysis.hotitems.KfkaSenderFromLog 1000 /data/flink/UserBehavior.csv | /usr/hdp/3.1.0.0-78/kafka/bin/kafka-console-producer.sh --broker-list hdspdev001:6667,hdspdev002:6667,hdspdev003:6667 --topic hotitems
     */

    /**
     * 每秒1000条
     */
    private static final long SPEED = 1000;

    public static void main(String[] args) {
        long speed;
        String filePath;
        if (args.length == 1) {
            speed = SPEED;
            filePath = args[0];
        } else if (args.length == 2) {
            speed = Long.parseLong(args[0]);
            filePath = args[1];
        } else {
            throw new IllegalArgumentException("file path can not be null");
        }
        // 每条耗时多少毫秒
        long delay = 1000_000 / speed;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(filePath)))) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(inputStream)));
            long start = System.nanoTime();
            while (reader.ready()) {
                String line = reader.readLine();
                System.out.println(line);

                long end = System.nanoTime();
                long diff = end - start;
                while (diff < (delay * SPEED)) {
                    Thread.sleep(1);
                    end = System.nanoTime();
                    diff = end - start;
                }
                start = end;
            }
            reader.close();
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException();
        }
    }
}
