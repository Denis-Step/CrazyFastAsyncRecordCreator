package com.example.submission.samples;

import com.example.submission.pojo.FooPojo;
import com.example.submission.pojo.FooPojoCreator;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Tiny utility with a {@code main} method that writes {@link FooPojo} records to a DynamoDB table.
 *
 * <p>Usage:
 * <pre>
 *   ./gradlew run --args="--table FooTable --count 25 --region us-east-1"
 * </pre>
 *
 * Region and table can also be provided via {@code FOO_TABLE_NAME} and {@code AWS_REGION}
 * environment variables respectively.
 */
public final class FooPojoSeeder {

  private static final Logger LOGGER = LogManager.getLogger(FooPojoSeeder.class);

  private FooPojoSeeder() {}

  public static void main(String[] args) {
    SeederConfig config = SeederConfig.from(args);
    LOGGER.info("Seeding Dynamo table '{}' with {} FooPojo items (region: {})",
        config.tableName, config.itemCount, config.region.id());

    try (DynamoDbClient dynamo = DynamoDbClient.builder()
        .region(config.region)
        .build()) {
      DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
          .dynamoDbClient(dynamo)
          .build();

      DynamoDbTable<FooPojo> table = enhancedClient.table(
          config.tableName,
          TableSchema.fromBean(FooPojo.class));

      FooPojoCreator creator = new FooPojoCreator();
      Instant start = Instant.now();

      IntStream.range(0, config.itemCount).forEach(idx -> {
        FooPojo pojo = creator.get();
        table.putItem(pojo);
        LOGGER.info("Wrote item {} with pojoId={} someValueToStore={}",
            idx + 1, pojo.getPojoId(), pojo.getJoinValue());
      });

      LOGGER.info("Finished writing {} items in {} ms",
          config.itemCount, Duration.between(start, Instant.now()).toMillis());
    }
  }

  private record SeederConfig(String tableName, int itemCount, Region region) {

    private static SeederConfig from(String[] args) {
      String table = null;
      Integer count = null;
      Region region = null;

      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--table" -> table = readValue(args, ++i, "--table");
          case "--count" -> count = Integer.parseInt(readValue(args, ++i, "--count"));
          case "--region" -> region = Region.of(readValue(args, ++i, "--region"));
          default -> {
            // ignore unknown flags to keep the helper permissive
          }
        }
      }

      String resolvedTable = Objects.requireNonNullElseGet(
          table,
          () -> System.getenv("FOO_TABLE_NAME"));
      if (resolvedTable == null || resolvedTable.isBlank()) {
        throw new IllegalArgumentException(
            "Table name must be supplied via --table or FOO_TABLE_NAME env var");
      }

      int resolvedCount = Objects.requireNonNullElse(count, 10);

      Region resolvedRegion = Objects.requireNonNullElseGet(
          region,
          () -> {
            String envRegion = System.getenv("AWS_REGION");
            return envRegion == null || envRegion.isBlank()
                ? Region.US_EAST_1
                : Region.of(envRegion);
          });

      return new SeederConfig(resolvedTable, resolvedCount, resolvedRegion);
    }

    private static String readValue(String[] args, int idx, String flag) {
      if (idx >= args.length) {
        throw new IllegalArgumentException("Missing value after " + flag);
      }
      return args[idx];
    }
  }
}
