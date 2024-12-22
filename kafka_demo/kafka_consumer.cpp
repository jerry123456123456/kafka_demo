// g++ -o kafka_consumer kafka_consumer.cpp -lrdkafka++ -lrdkafka

#include <iostream>
#include <string>
#include <fstream>
#include <librdkafka/rdkafkacpp.h>
#include <vector>

class KafkaConsumer {
public:
    KafkaConsumer(const std::string& brokers, const std::string& group_id, const std::string& topic, const std::string& log_file) 
        : log_file_(log_file) {

        // 配置Kafka消费者
        RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        conf->set("metadata.broker.list", brokers, errstr_);
        conf->set("group.id", group_id, errstr_);

        consumer_ = RdKafka::KafkaConsumer::create(conf, errstr_);
        if (!consumer_) {
            std::cerr << "Failed to create consumer: " << errstr_ << std::endl;
            return;
        }

        // 订阅主题
        consumer_->subscribe({topic});  // 只需要主题名称，不需要分区信息
    }

    void consumeLogs() {
        std::ofstream log_stream(log_file_, std::ios::app);  // 打开日志文件，追加模式
        if (!log_stream.is_open()) {
            std::cerr << "Failed to open log file: " << log_file_ << std::endl;
            return;
        }

        while (true) {
            // 消费消息，设置超时为1000ms（1秒）
            RdKafka::Message* msg = consumer_->consume(1000);
            if (msg->err()) {
                if (msg->err() == RdKafka::ERR__TIMED_OUT) {
                    //std::cerr << "Error: Local: Timed out" << std::endl;
                } else {
                    std::cerr << "Error: " << msg->errstr() << std::endl;
                }
            } else {
                // 获取消息内容，并格式化
                std::string payload = std::string(static_cast<char*>(msg->payload()), msg->len());
                // 将消息内容写入日志文件
                log_stream << "Consumed message: " << payload << std::endl;
                std::cout << "Consumed message: " << payload << std::endl;  // 可选：同时输出到控制台
            }
            delete msg;  // 别忘了释放消息内存
        }
    }

    ~KafkaConsumer() {
        if (consumer_) {
            consumer_->close();
            delete consumer_;
        }
    }

private:
    RdKafka::KafkaConsumer* consumer_ = nullptr;
    std::string errstr_;
    std::string log_file_;  // 存储日志文件路径
};

int main() {
    // 连接Kafka的broker地址
    std::string brokers = "localhost:9092";
    // 消费者组ID
    std::string group_id = "my_consumer_group";
    // 订阅的Kafka主题
    std::string topic = "my_topic";
    // 日志文件路径
    std::string log_file = "kafka_logs.txt";

    KafkaConsumer kafkaConsumer(brokers, group_id, topic, log_file);
    kafkaConsumer.consumeLogs();

    return 0;
}
