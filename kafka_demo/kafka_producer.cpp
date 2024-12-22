// g++ -o kafka_producer kafka_producer.cpp -lrdkafka++ -lrdkafka

#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

class KafkaProducer {
public:
    KafkaProducer(const std::string& brokers, const std::string& topic) {
        // 创建配置对象
        RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        // 配置broker列表
        conf->set("metadata.broker.list", brokers, errstr_);

        // 创建生产者对象
        producer_ = RdKafka::Producer::create(conf, errstr_);
        if (!producer_) {
            std::cerr << "Failed to create producer: " << errstr_ << std::endl;
            return;
        }
        
        // 创建 Topic 对象
        topic_ = RdKafka::Topic::create(producer_, topic, nullptr, errstr_);
        if (!topic_) {
            std::cerr << "Failed to create topic: " << errstr_ << std::endl;
            return;
        }
    }

    void sendLogMessage(const std::string& message) {
        // 使用 producer->produce 发送消息
        RdKafka::ErrorCode resp = producer_->produce(
            topic_,                              // 使用 topic 对象
            RdKafka::Topic::PARTITION_UA,        // 使用默认分区
            RdKafka::Producer::RK_MSG_COPY,      // 消息副本
            const_cast<char*>(message.c_str()), // 消息内容
            message.size(),                     // 消息大小
            nullptr,                            // Key
            nullptr                             // Headers
        );

        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to send message: " << RdKafka::err2str(resp) << std::endl;
        } else {
            std::cout << "Log message sent: " << message << std::endl;
        }

        // 使用flush()确保消息发送完毕
        producer_->flush(1000);  // 等待1秒钟，确保所有消息都已经发送
    }

    ~KafkaProducer() {
        delete producer_;
        delete topic_;  // 删除 topic 对象
    }

private:
    RdKafka::Producer* producer_;
    RdKafka::Topic* topic_;  // 使用 Topic 对象
    std::string errstr_;
};

int main() {
    std::string brokers = "localhost:9092";  // Kafka broker地址
    std::string topic = "my_topic";          // 主题

    KafkaProducer producer(brokers, topic);

    // 消息生产循环
    std::string input;
    while (true) {
        std::cout << "请输入要发送到Kafka的消息内容(输入'exit'退出): ";
        std::getline(std::cin, input);

        if (input == "exit") {
            break;
        }

        // 发送用户输入的消息
        producer.sendLogMessage(input);
    }

    return 0;
}

