package com.example.submission.pojo;

import java.util.UUID;
import java.util.function.Supplier;

public class FooPojoCreator implements Supplier<FooPojo> {

  @Override
  public FooPojo get() {
    return new FooPojo(
        "primaryKey-" + UUID.randomUUID(),
        "join-" + UUID.randomUUID()
    );
  }
}
