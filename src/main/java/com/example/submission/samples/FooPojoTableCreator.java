package com.example.submission.samples;

import com.example.submission.pojo.FooPojo;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Small helper to create a DynamoDB table for {@link FooPojo} with partition key {@code pojoId}.
 *
 * Usage:
 *   ./gradlew run --args="--create-table --table FooPojoDummy --region us-east-1"
 * or run via the IntelliJ run configuration added for convenience.
 */
public final class FooPojoTableCreator {

  private static final Logger LOGGER = LogManager.getLogger(FooPojoTableCreator.class);

  private FooPojoTableCreator() {}

  public static void main(String[] args) {
    Config cfg = Config.from(args);

    try (DynamoDbClient dynamo = DynamoDbClient.builder()
        .region(cfg.region)
        .build()) {

      if (tableExists(dynamo, cfg.tableName)) {
        LOGGER.info("Table '{}' already exists in region {}", cfg.tableName, cfg.region.id());
        return;
      }

      // Use on-demand billing for simplicity.
      CreateTableRequest request = CreateTableRequest.builder()
          .tableName(cfg.tableName)
          .billingMode(BillingMode.PAY_PER_REQUEST)
          .keySchema(KeySchemaElement.builder()
              .attributeName("pojoId")
              .keyType(KeyType.HASH)
              .build())
          .attributeDefinitions(AttributeDefinition.builder()
              .attributeName("pojoId")
              .attributeType(ScalarAttributeType.S)
              .build())
          .build();

      dynamo.createTable(request);
      LOGGER.info("Created table '{}' (billing=PAY_PER_REQUEST)", cfg.tableName);

      // Optional: validate mapping using the enhanced client
      DynamoDbEnhancedClient enhanced = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamo).build();
      DynamoDbTable<FooPojo> table = enhanced.table(cfg.tableName, TableSchema.fromBean(FooPojo.class));
      LOGGER.info("Validated FooPojo schema mapping for table '{}': {}", cfg.tableName, table.tableName());
    }
  }

  private static boolean tableExists(DynamoDbClient dynamo, String table) {
    try {
      dynamo.describeTable(DescribeTableRequest.builder().tableName(table).build());
      return true;
    } catch (ResourceNotFoundException rnfe) {
      return false;
    }
  }

  private record Config(String tableName, Region region) {
    private static Config from(String[] args) {
      String table = null;
      Region region = null;
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--table" -> table = readValue(args, ++i, "--table");
          case "--region" -> region = Region.of(readValue(args, ++i, "--region"));
          default -> {}
        }
      }
      String resolvedTable = Objects.requireNonNullElseGet(
          table, () -> System.getenv().getOrDefault("FOO_TABLE_NAME", "FooPojoDummy"));
      Region resolvedRegion = Objects.requireNonNullElseGet(
          region, () -> Region.of(System.getenv().getOrDefault("AWS_REGION", "us-east-1")));
      return new Config(resolvedTable, resolvedRegion);
    }

    private static String readValue(String[] args, int idx, String flag) {
      if (idx >= args.length) {
        throw new IllegalArgumentException("Missing value after " + flag);
      }
      return args[idx];
    }
  }
}

