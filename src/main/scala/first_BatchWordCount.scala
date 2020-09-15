import org.apache.flink.api.scala.ExecutionEnvironment

// 需求：读取本地数据文件，统计文件中每个单词出现的次数
// 根据需求，很明显是有界流（批计算），所以采用另外一个上下文环境  :ExecutionEnvironment

object first_BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 读取数据
    val dataURL = getClass.getResource("/batch_wc.txt") // 文件存放在 resources中
    val data:DataSet[String] = env.readTextFile(dataURL.getPath)
    // 计算
    val result:AggregateDataSet[(String,Int)] = data.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    // 打印结果
    result.print()

  }
}
