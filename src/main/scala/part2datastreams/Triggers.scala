package part2datastreams

import generators.shopping._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Triggers {

  // Triggers -> WHEN a window function is executed
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def demoFirstTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
      .process(new CountByWindowAll)

    shoppingCartEvents.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoFirstTrigger()
  }
}

// copied from Time Based Transformations
class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
  override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
    val window = context.window
    out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
  }
}
