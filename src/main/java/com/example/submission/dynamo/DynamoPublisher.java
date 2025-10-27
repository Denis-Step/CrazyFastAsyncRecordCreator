package com.example.submission.dynamo;

import com.example.submission.pojo.FooPojo;
import com.example.submission.pojo.ShimUtil;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Thin async publisher that batches records and writes them to DynamoDB.
 * <p>
 * The class internally spins up a single worker thread that continuously drains the supplied queue,
 * converts records into Dynamo maps via {@link ShimUtil}, and schedules {@code BatchWriteItem}
 * requests with basic retry semantics.
 */
public class DynamoPublisher<T>  {

  private static final Logger logger = LoggerFactory.getLogger(DynamoPublisher.class);

  private static final int MAX_NUM_RETRIES = 9;
  private static final int BATCH_SIZE = 25;

  private final String dynamoTableName;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final BlockingDeque<T> recordQueue;
  private final Semaphore semaphore;
  private final int maxInFlight;
  private final CompletableFuture<Void> publisherTask;

  private static final java.util.concurrent.ExecutorService HANDOFF_EXECUTOR =
      Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r, "dynamo-publisher");
        thread.setDaemon(true);
        return thread;
      });

  private volatile boolean shutdownRequested = false;

  public DynamoPublisher(
      @NonNull String dynamoTableName,
      @NonNull DynamoDbAsyncClient dynamoDbAsyncClient) {
    this(dynamoTableName, dynamoDbAsyncClient, new LinkedBlockingDeque<>(), 32);
  }

  public DynamoPublisher(
      @NonNull String dynamoTableName,
      @NonNull DynamoDbAsyncClient dynamoDbAsyncClient,
      int maxInFlight) {
    this(dynamoTableName, dynamoDbAsyncClient, new LinkedBlockingDeque<>(), maxInFlight);
  }

  public DynamoPublisher(
      @NonNull String dynamoTableName,
      @NonNull DynamoDbAsyncClient dynamoDbAsyncClient,
      @NonNull BlockingDeque<T> recordQueue,
      int maxInFlight) {
    this.dynamoTableName = dynamoTableName;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.recordQueue = recordQueue;
    this.maxInFlight = Math.max(1, maxInFlight);
    this.semaphore = new Semaphore(this.maxInFlight);
    this.publisherTask = CompletableFuture.runAsync(this::publisherLoop, HANDOFF_EXECUTOR);
  }

  /**
   * Enqueues a record for asynchronous publishing. Blocks when the queue is full.
   */
  @SneakyThrows
  public void publish(@NonNull T record) {
    recordQueue.put(record);
  }

  public CompletableFuture<Void> close() {
    shutdownRequested = true;
    publisherTask.join();
    dynamoDbAsyncClient.close();
    return publisherTask;
  }

  @SneakyThrows
  private void publisherLoop() {
    while (!shutdownRequested
        || !recordQueue.isEmpty()
        || maxInFlight != semaphore.availablePermits()) {

      List<T> recordsToPublish = nextBatch();
      if (recordsToPublish.isEmpty()) {
        continue;
      }

      BatchWriteItemRequest request = batchWriteItemRequest(recordsToPublish);
      semaphore.acquire();
      createBatchWriteFuture(request, 0);
    }
  }

  @SneakyThrows
  private List<T> nextBatch() {
    List<T> recordsToPublish = new ArrayList<>(BATCH_SIZE);
    T first = recordQueue.poll(100, TimeUnit.MILLISECONDS);
    if (first != null) {
      recordsToPublish.add(first);
      recordQueue.drainTo(recordsToPublish, BATCH_SIZE - 1);
    }
    return recordsToPublish;
  }

  private void createBatchWriteFuture(BatchWriteItemRequest request, int numRetry) {
    dynamoDbAsyncClient.batchWriteItem(request)
        .whenComplete((response, error) -> {
          if (error != null) {
            logger.warn("Error while writing records to DynamoDB", error);
            retry(request, numRetry);
            return;
          }
          if (response == null) {
            retry(request, numRetry);
            return;
          }
          if (!response.unprocessedItems().isEmpty()) {
            // TODO: This part buggy.
            logger.warn("Retrying {} unprocessed DynamoDB requests {}", response.unprocessedItems().size(), response.unprocessedItems());
            BatchWriteItemRequest retryRequest = BatchWriteItemRequest.builder()
                .requestItems(response.unprocessedItems())
                .build();
            retry(retryRequest, numRetry);
            return;
          }

          semaphore.release();
          logger.info("Successfully wrote batch of {} records to DynamoDB", request.requestItems()
              .getOrDefault(dynamoTableName, List.of()).size());
        });
  }

  private void retry(BatchWriteItemRequest request, int numRetry) {
    if (numRetry >= MAX_NUM_RETRIES) {
      semaphore.release();
      throw new IllegalStateException("Retries exceeded when writing to DynamoDB");
    }
    createBatchWriteFuture(request, numRetry + 1);
  }

  private BatchWriteItemRequest batchWriteItemRequest(List<T> recordsToPublish) {
    List<WriteRequest> putRequests = recordsToPublish.stream()
        .map(r -> WriteRequest.builder()
            .putRequest(PutRequest.builder()
                .item(ShimUtil.getMap(r))
                .build())
            .build())
        .toList();

    return BatchWriteItemRequest.builder()
        .requestItems(Map.of(dynamoTableName, putRequests))
        .build();
  }

  /**
   * Spark partitions on an executor need to share a single object. The easiest way to do this to
   * avoid serialization issues in the closure broadcasted to all executors is to simply use static
   * variables as a kind of static registry that is scoped to the entire java process.
   */
  public static class DynamoPublisherCreator {

    private static final DynamoPublisher<FooPojo> INSTANCE =
        new DynamoPublisher<>( System.getenv().getOrDefault("FOO_TABLE_NAME", "SamplePojoTable"),
            DynamoDbAsyncClient.builder()
                .region(Region.of(System.getenv().getOrDefault("AWS_REGION", "US-WEST-2")))
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(1024)
                    // KEEP THIS SAME OR GREATER THAN  maxInFlight
                    .maxPendingConnectionAcquires(50_000)

                    .connectionAcquisitionTimeout(Duration.ofMinutes(5))
                    .readTimeout(Duration.ofMinutes(5))
                    .writeTimeout(Duration.ofMinutes(5))
                )
                .build(),
            new LinkedBlockingDeque<>(1_000_000),
            5_000);


    public static DynamoPublisher<FooPojo> getInstance() {
      return INSTANCE;
    }

  }
}
