package com.test.insert;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TestInsertApplication implements CommandLineRunner {

    public static void main(String[] args) {
        int exitValue = SpringApplication.exit(SpringApplication.run(TestInsertApplication.class, args));
        System.exit(exitValue);
    }

    @Override
    public void run(String... args) {
        try {

            System.out.println("");
            System.out.println("==> Start Test insert");

            long rowCount = args.length > 0 ? Long.parseLong(args[0]) : 2_000_000;
            long columnCount = args.length > 1 ? Long.parseLong(args[1]) : 50;

            boolean createDataFileWithId = (args.length > 2 && args[2].equals("create_data_file_with_id"));
            boolean addIdUsingSpark = (args.length > 2 && args[2].equals("add_id_using_spark"));

            String collectionName = "test-insert";

            if (createDataFileWithId) {
                System.out.println("==> 2/ Create data file with '_id'");
            } else if (addIdUsingSpark) {
                System.out.println("==> 3/ Add the '_id' column using Spark");
            } else {
                System.out.println("==> 1/ Let MongoDB generate the '_id' column");
            }

            System.out.println("==> Drop the MongoDB " + getDatabase() + " database");
            dropDatabase();

            File dataFile = createDataFile("test-data", rowCount, columnCount, createDataFileWithId);

            Dataset<Row> dataset = createDataset(dataFile);

            if (addIdUsingSpark) {
                dataset = dataset.withColumn("_id",
                        concat(col("col_0"), lit(';'), col("col_1"), lit(';'), col("col_2")));
            }

            System.out.println("==> Save the records into " + getURIWithCollection(collectionName));
            Instant start = Instant.now();
            saveDataset(collectionName, dataset);
            Instant end = Instant.now();
            System.out.println("==> saveDataset executed in " + computeDuration(start, end));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private File createDataFile(String filePrefix, long rowCount, long columnCount, boolean createDataFileWithId)
            throws IOException {

        File tempFile = File.createTempFile(filePrefix, ".csv");
        tempFile.deleteOnExit();
        System.out.println("==> Generating " + rowCount + " of test lines with " + columnCount
                + " columns, inside temp file " + tempFile);

        try (PrintWriter pw = new PrintWriter(tempFile)) {

            StringBuilder sb = new StringBuilder();
            if (createDataFileWithId) {
                sb.append("_id;");
            }
            for (int column = 0; column < columnCount; column++) {
                sb.append("col_").append(column);
                if (column < columnCount - 1) {
                    sb.append(';');
                }
            }
            pw.println(sb.toString());

            for (long row = 0; row < rowCount; row++) {
                sb = new StringBuilder();
                if (createDataFileWithId) {
                    sb.append("id_").append(row).append(';');
                }
                for (int column = 0; column < columnCount; column++) {
                    sb.append("the_test_value_").append(row).append("_").append(column);
                    if (column < columnCount - 1) {
                        sb.append(';');
                    }
                }
                pw.println(sb.toString());
            }
        }

        return tempFile;
    }

    private Dataset<Row> createDataset(File dataFile) throws IOException {
        return getSparkSession().read().option("header", true).option("delimiter", ";").csv(dataFile.getPath());
    }

    private void saveDataset(String collectionName, Dataset<Row> dataset) {
        dataset.write().format("com.mongodb.spark.sql")
                .option(outputOption("uri"), getURIWithCollection(collectionName))
                .option(outputOption("ordered"), false).mode(SaveMode.Append).save();
    }

    private String outputOption(String option) {
        return "spark.mongodb.output." + option;
    }

    private void dropDatabase() {
        try (MongoClient client = MongoClients.create(getURI())) {
            MongoDatabase database = client.getDatabase(getDatabase());
            database.drop();
        }
    }

    private String getURI() {
        return System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");
    }

    private String getDatabase() {
        return System.getenv().getOrDefault("DATABASE", "TestDB");
    }

    private String getURIWithCollection(String collectionName) {
        return getURI() + "/" + getDatabase() + "." + collectionName;
    }

    /**
     * Create a Spark session that is "local" by default
     */
    private SparkSession getSparkSession() {

        return SparkSession.builder().master(System.getenv().getOrDefault("MASTER", "local[*]")).appName("test-insert")
                .config("spark.app.id", "test-insert").config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g").config("spark.executor.cores", "4")
                .config("spark.cores.max", "4").config("spark.sql.shuffle.partitions", "8").getOrCreate();
    }

    private static String computeDuration(Instant start, Instant end) {
        Duration duration = Duration.between(start, end);
        long mins = duration.toMinutes();
        long secs = duration.toSeconds();

        if (mins > 0) {
            return String.format("%d minutes, %d seconds", mins, secs % 60);
        } else {
            return String.format("%d seconds", secs);
        }
    }
}
