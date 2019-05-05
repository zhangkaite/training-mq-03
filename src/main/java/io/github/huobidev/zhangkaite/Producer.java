package io.github.huobidev.zhangkaite;

public interface Producer {

    void sendMessages(String transactionId, String topic, String key, Object value);

}
