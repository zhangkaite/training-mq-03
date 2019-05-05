package io.github.huobidev.zhangkaite;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @description: 消息消费者，后端维护offset，不依赖kafka的offset
 */
public class ConsumerImpl extends AbstractConsumer implements  ApplicationListener {


    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        this.consumer("zhangkaite", "kate");
    }

    @Override
    public void consumer(String group, String... topics) {
        Properties props = initProperties();
        props.setProperty("group.id", group);
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topics), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffsetToDB(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(topicPartition -> consumer.seek(topicPartition, getOffsetFromDB(topicPartition)));
            }
        });
        while (true) {
            try {
                ConsumerRecords poll = consumer.poll(100);
                poll.forEach(o -> {
                    ConsumerRecord<String, String> record = (ConsumerRecord) o;
                    processRecord(record);
                    saveRecordAndOffsetInDB(record, record.offset());
                });
            } catch (Exception e) {
                //需打印错误日志
            }

        }
    }


    @Override
    void saveRecordAndOffsetInDB(ConsumerRecord<String, String> record, long offset) {

    }

    @Override
    void processRecord(ConsumerRecord<String, String> record) {

    }

    @Override
    void commitOffsetToDB(Collection<TopicPartition> partitions) {

    }

    @Override
    long getOffsetFromDB(TopicPartition topicPartition) {
        return 0;
    }
}
