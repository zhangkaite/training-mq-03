package io.github.huobidev.zhangkaite;

import java.util.Properties;

/**
 * @description:
 */
public abstract class AbstractProducer implements Producer {

    protected Properties initProperties() {
        Properties pro = new Properties();
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("bootstrap.servers", "127.0.0.1:9092");
        pro.put("batch.size", "10240");
        pro.put("linger.ms", "1");
        pro.put("enable.idempotence", "true");
        pro.put("retries", "3");
        pro.put("buffer.memory", "33554432");
        pro.put("max.in.flight.requests.per.connection", 1);
        pro.put("acks", "all");
        return pro;

    }

}
