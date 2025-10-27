package com.example.submission;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppTest {

    private SparkSession spark;

    @BeforeEach
    void setUp() {
        spark = App.createSession("local[1]");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.close();
        }
    }

    @Test
    void computeSubmissionMetricsReturnsExpectedCounts() {
        Dataset<Row> metrics = App.computeSubmissionMetrics(spark);
        Row row = metrics.collectAsList().get(0);

        assertEquals(5L, row.getLong(row.fieldIndex("totalSubmissions")));
        assertEquals(4L, row.getLong(row.fieldIndex("lastSubmissionId")));
    }
}
