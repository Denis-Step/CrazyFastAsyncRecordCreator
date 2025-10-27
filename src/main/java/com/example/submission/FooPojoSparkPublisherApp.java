package com.example.submission;

import com.example.submission.dynamo.DynamoPublisher;
import com.example.submission.dynamo.DynamoPublisher.DynamoPublisherCreator;
import com.example.submission.pojo.FooPojo;
import com.example.submission.pojo.FooPojoCreator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

/**
 * Spark entry point that synthesizes {@link FooPojo} records per partition and publishes them via a
 * shared {@link DynamoPublisher}. Each Spark partition generates 1k FooPojos and hands them back to
 * the driver where the publisher batches and forwards them to Dynamo/Kinesis.
 */
public final class FooPojoSparkPublisherApp {

  private static final Logger LOGGER = LogManager.getLogger(FooPojoSparkPublisherApp.class);

  private FooPojoSparkPublisherApp() {
  }

  public static void main(String[] args) {
    int partitions = Integer.parseInt(System.getProperty("foo.partitions", "50"));
    int numRecords = Integer.parseInt(System.getenv().getOrDefault("RECORDS_TO_WRITE", "10000000"));

    int recordsPerPartition = numRecords / partitions;

    SparkSession SPARK = App.createSession(System.getProperty("spark.master"));

    SPARK.range(0, partitions)
        .coalesce(partitions)
        .mapPartitions((MapPartitionsFunction<Long, FooPojo>) iterator -> {
          // empty partition defensive check.


          List<FooPojo> fooPojos = new ArrayList<>();
          // 1. Create FooPojos.
          for (int i = 0; i < recordsPerPartition; i++) {

            FooPojoCreator creator = new FooPojoCreator();
            FooPojo fooPojo = creator.get();

            // 2. Non-blocking publish (unless queue full, then block).
            fooPojos.add(fooPojo);
            DynamoPublisherCreator.getInstance().publish(fooPojo);
          }

          return fooPojos.iterator();

        }, Encoders.bean(FooPojo.class))

        // 3. Await all async publishers.

        .mapPartitions( (MapPartitionsFunction<FooPojo, FooPojo>) iterator -> {
          try {
            CompletableFuture.allOf(DynamoPublisherCreator.getInstance().close())
                .get(10L, TimeUnit.MINUTES);
            return iterator;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }, Encoders.bean(FooPojo.class) )

        // 4. Write everything out to an s3 file or local
        .write()
        .json("fooPojos" + UUID.randomUUID().toString());

  }
}
