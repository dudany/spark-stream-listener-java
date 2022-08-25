
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DummySparkStreamListener extends StreamingQueryListener {
    private KafkaConsumer<String, String> listenerConsumer;

    public DummySparkStreamListener(KafkaConsumer<String, String> consumer) {
        // consumer is connected to spark stream kafka consumer group
        super();
        this.listenerConsumer = consumer; 

    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {

    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        // runs on each sources in the event of the batch (maybe be different sources)
        for (SourceProgress sourceProgress : event.progress()
            .sources()) {
            Map<TopicPartition, OffsetAndMetadata> totalTopicPartitionMap = new HashMap<>();
            try {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Map<String, Integer>> endOffsetsJson = mapper.readValue(sourceProgress.endOffset(), Map.class);
                // runs for each topic of the consumer
                for (Map.Entry<String, Map<String, Integer>> entry : endOffsetsJson.entrySet()) {
                    String key = entry.getKey();
                    Map<String, Integer> value = entry.getValue();
                    Map<TopicPartition, OffsetAndMetadata> topicPartitionMap = getTopicPartitionsEndOffsets(key, value);
                    totalTopicPartitionMap.putAll(topicPartitionMap);
                }

                listenerConsumer.commitSync(totalTopicPartitionMap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getTopicPartitionsEndOffsets(String topic, Map<String, Integer> partitions) {
        Map<TopicPartition, OffsetAndMetadata> topicPartitionMap = new HashMap<>();
        // for each partition in the topic add it to the map then commits the map
        for (Map.Entry<String, Integer> entry : partitions.entrySet()) {
            Integer partition = Integer.parseInt(entry.getKey());
            ;
            Integer offset = entry.getValue();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            OffsetAndMetadata offsetMtData = new OffsetAndMetadata(offset);
            topicPartitionMap.put(topicPartition, offsetMtData);
        }
        return topicPartitionMap;
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {

    }
}
