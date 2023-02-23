package streams_dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-filter-application";

    private static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static String STREAM_LOG = "stream_log";

    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //stream_log 토픽을 가져오기 위한 소스 프로세서
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        KStream<String, String> filteredStream = streamLog.filter((key, value) -> value.length() > 5);

        //stream_log_filter 토픽에 저장하는 싱크 프로세서
        filteredStream.to(STREAM_LOG_FILTER);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();
    }
}
