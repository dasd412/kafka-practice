package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerCallback implements Callback {

    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        // 브로커에 정상 전송됬는지 비동기로 확인해줌. 단, 데이터 전송 순서는 보장하지 않으므로 전송 순서가 중요한 경우엔 사용하지 말 것.

        if(e!=null){
            logger.error(e.getMessage(),e);
        }else{
            logger.info(recordMetadata.toString());
        }
    }
}
