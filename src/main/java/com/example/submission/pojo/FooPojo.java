package com.example.submission.pojo;

import java.io.Serial;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * Represents some kind of object we wish to create en masse.
 * pojoId the primary key of this obj
 * someValueToStore the value we will retrieve it by later somewhere else.
 */
@DynamoDbBean
@Getter
@Setter
public class FooPojo implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;


  private String pojoId;
  private String joinValue;

  public FooPojo() {}

  public FooPojo(String pojoId, String joinValue) {
    this.pojoId = pojoId;
    this.joinValue = joinValue;
  }

  @DynamoDbPartitionKey
  public String getPojoId() {
    return pojoId;
  }

}
