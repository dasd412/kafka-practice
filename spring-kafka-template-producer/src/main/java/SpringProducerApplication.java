import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    @Autowired
    private KafkaTemplate<String, String> customKafkaTemplate;//<-빈 메서드 이름과 동일한 변수명이여야 한다.

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        //ListenableFuture는 전송한 이후 정상 적재됬는지 여부를 확인해준다.
        ListenableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, "test");

        future.addCallback(
                new KafkaSendCallback<String, String>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {

                    }

                    @Override
                    public void onFailure(KafkaProducerException ex) {

                    }
                });
        System.exit(0);
    }
}
