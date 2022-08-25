import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class SparkStreamKafka {
    public void startStream() throws TimeoutException {
        // create spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        String BOOTSTRAP_SERVER = "host1:port1";
        String CONSUMER_GROUP = "test";
        //create committer consumer with group id "test"
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //add listener to streams
        spark.streams().addListener(new DummySparkStreamListener(consumer));

        Dataset<Row> streamDf = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("groupIdPrefix",CONSUMER_GROUP)
                .option("subscribe", "topic1").load();

        streamDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("topic", "topic1").trigger(Trigger.Continuous("1 second")).start();



    }
}
