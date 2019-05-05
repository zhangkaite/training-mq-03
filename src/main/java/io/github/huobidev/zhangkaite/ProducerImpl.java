package io.github.huobidev.zhangkaite;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import kafka.common.KafkaException;

/**
 * @description: 消息生产者，开启消息发送事务，防止数据处理失败丢失
 */
public class ProducerImpl extends AbstractProducer {


    @Override
    public void sendMessages(String transactionId, String topic, String key, Object value) {
        Properties pro = initProperties();
        pro.put("transaction.id", transactionId);
        KafkaProducer kafkaProducer = new KafkaProducer(pro);
        try {
            kafkaProducer.beginTransaction();
            ProducerRecord record = new ProducerRecord(topic, key, value);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    kafkaProducer.abortTransaction();
                    throw new KafkaException(exception.getMessage() + " , data: " + record);
                }
            });
            kafkaProducer.commitTransaction();
        } catch (Throwable e) {
            kafkaProducer.abortTransaction();
        }
    }


}
