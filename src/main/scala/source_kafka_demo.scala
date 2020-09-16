import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

// ./kafka-console-producer.sh --broker-list 172.31.1.213:9092 --topic flink_test


object source_kafka_demo {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    streamEnv.setParallelism(2)
    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","172.31.1.213:9092")
    props.setProperty("group.id","test")
    //读取数据
    val stream = streamEnv.addSource(new FlinkKafkaConsumer[String]("flink_test",new SimpleStringSchema(),props))
    stream.print()
    streamEnv.execute("kafka source demo")
  }
}