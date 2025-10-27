package com.example.submission.pojo;

import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Hack to get maps for dynamo.
 */
public class ShimUtil {

  public  static <T> Map<String, AttributeValue> getMap(T item) {
    if (item instanceof FooPojo fooPojo) {
      return Map.of(
          "pojoId", AttributeValue.builder().s(fooPojo.getPojoId()).build(),
          "joinValue", AttributeValue.builder().s(fooPojo.getJoinValue()).build()
      );
    }

    throw new IllegalArgumentException("You broke it!");
  }

}
