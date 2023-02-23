package streams_dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {

    //애플리케이션 id 값을 기준으로 스트림즈 애플리케이션이 병렬처리한다.
    private static String APPLICATION_NAME="streams-application";

    private static String BOOTSTRAP_SEVERS="localhost:9092";

    private static String STREAM_LOG="stream_log";

    private static String STREAM_LOG_COPY="stream_log_copy";

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SEVERS);
        // 메시지 직렬화, 역직렬화 방식 지정
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder streamsBuilder=new StreamsBuilder();
        //최초의 토픽을 가져오는 소스 프로세서 활용
        KStream<String,String>streamLog=streamsBuilder.stream(STREAM_LOG);
        // 특정 토픽으로 저장하는 싱크 프로세서 활용
        streamLog.to(STREAM_LOG_COPY);

        KafkaStreams streams=new KafkaStreams(streamsBuilder.build(),properties);
        streams.start();

    }
}
