package io.github.huobidev.zhangkaite;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Properties;

/**
 * @description:
 */
public abstract class AbstractConsumer implements Consumer {

    /**
     * 保存offset和数据
     */
    abstract void saveRecordAndOffsetInDB(ConsumerRecord<String, String> record, long offset);

    /**
     * 业务处理
     */
    abstract void processRecord(ConsumerRecord<String, String> record);

    /**
     * 保存offset到数据库
     */
    abstract void commitOffsetToDB(Collection<TopicPartition> partitions);

    /**
     * 从数据库获取当前的offset
     */
    abstract long getOffsetFromDB(TopicPartition topicPartition);


    protected Properties initProperties() {
        Properties props = new Properties();
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.put("enable.auto.commit", "true");
        return props;

    }


}
