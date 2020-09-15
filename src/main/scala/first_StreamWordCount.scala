import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


// 在本地客户端 输入命令  nc -lk 8888
object first_StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 初始化Flink的Streaming(流计算）上下文执行环境
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐士转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    //读取数据
    val stream:DataStream[String] = streamEnv.socketTextStream("127.0.0.1",8888)
    //转换计算
    val result:DataStream[(String,Int)] = stream.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .sum(1)

    //打印到控制台
    result.print()
    //启动流式计算，如果没有该代码上面的程序不会执行
    streamEnv.execute("first_streamwordcount")
 }
}
