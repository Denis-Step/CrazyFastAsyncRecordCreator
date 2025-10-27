package com.example.submission;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Minimal Apache Spark entry point to run local data quality checks
 * before wiring additional Artemis submission logic.
 */
public final class App {

    private static final String APP_NAME = "submissionToArtemis";
    private static final String DEFAULT_MASTER = "local[*]";

    private App() {
        // Utility class
    }

    public static void main(String[] args) {
        try (SparkSession spark = createSession(System.getProperty("spark.master"))) {
            Dataset<Row> metrics = computeSubmissionMetrics(spark);
            metrics.show(false);
        }
    }

    public static SparkSession createSession(String masterOverride) {
        String master = (masterOverride == null || masterOverride.isBlank())
                ? DEFAULT_MASTER
                : masterOverride;

        return SparkSession.builder()
                .appName(APP_NAME)
                .master(master)
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    static Dataset<Row> computeSubmissionMetrics(SparkSession spark) {
        Dataset<Row> submissions = spark.range(5).withColumnRenamed("id", "submissionId");
        submissions.createOrReplaceTempView("submissions");

        return spark.sql("""
                SELECT COUNT(*) AS totalSubmissions,
                       MAX(submissionId) AS lastSubmissionId
                FROM submissions
                """);
    }
}
