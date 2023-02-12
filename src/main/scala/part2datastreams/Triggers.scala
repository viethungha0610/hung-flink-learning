package part2datastreams

import generators.shopping._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Triggers {

  // Triggers -> WHEN a window function is executed
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def demoCountTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events/seconds
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
      .trigger(CountTrigger.of[TimeWindow](5)) // the window function runs every 5 elements
      .process(new CountByWindowAll) // runs twice for the same window

    shoppingCartEvents.print()
    env.execute()
  }

  // purging trigger - clear the window when it fires
  def demoPurgingTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events/seconds
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5))) // the window function runs every 5 elements THEN CLEARS THE WINDOW
      .process(new CountByWindowAll) // runs twice for the same window
    shoppingCartEvents.print()
    env.execute()
  }

    /*
    * Other triggers:
    * - EventTimeTrigger - happens by default when the watermark is > window end time (automatic for event time windows)
    * - ProcessingTimeTrigger - fires when the current time system > window end time (automatic for processing time windows)
    * - Custom triggers - powerful APIs for custom firing behaviour
    * */

  /**
   * Triggers:
   *  Specify when a window function will be executed
   *    - Can run multiple times on the same window
   *    - Has nothing to do with the window, i.e. event grouping
   *  Purging trigger: clear the contents of the window after trigger fires
   *    - Wraps another trigger
   *  Other triggers: event time/processing time (automatic), custom
   * */

  def main(args: Array[String]): Unit = {
    demoPurgingTrigger()
  }
}

// copied from Time Based Transformations
class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
  override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
    val window = context.window
    out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
  }
}
