package part2datastreams

import generators.shopping.{ShoppingCartEvent, ShoppingCartEventsGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Instant

object TimeBasedTransformation {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents = env.addSource(new ShoppingCartEventsGenerator(
    sleepMillisPerEvent = 100,
    batchSize = 5,
    baseInstant = Instant.parse("2022-02-15T00:00:00.000Z")
  ))

  // 1. Event time = the moment the event was CREATED
  // 2. Processing time = the moment the event ARRIVES AT FLINK

  /*
  * Group by window, every 3s, tumbling window, PROCESSING TIME
  * */
  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
    }
  }

  def demoProcessingTime(): Unit = {
    def groupedEventsByWindow = shoppingCartEvents.windowAll(
      TumblingProcessingTimeWindows.of(Time.seconds(3))
    )

    def countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(
      new CountByWindowAll
    )

    countEventsByWindow.print()
    env.execute()
  }

  def demoEventTime(): Unit = {
    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500 millis
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(
      new CountByWindowAll
    )

    countEventsByWindow.print()
    env.execute()
  }


  /*
  * With processing time
  * - We don't care when the event was created
  * - Multiple runs generate different results
  * */

  /*
  * With event time
  * - we NEED to care about handling late data - done with watermarks
  * - we don't care about Flink internal time
  * - we might see faster results
  * - same events + different runs => same results
  * */

  /**
   * Custom watermark
   * */

  // with every new MAX timestamp, every new incoming element with event time < max timestamp - max delay will be discarded
  class BoundedOutOfOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {

    var currentMaxTimestamp: Long = 0L

    // maybe emit watermark on a particular event
    override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
      //                ^actual event being processed ^ts to attach to the event
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)

      output.emitWatermark(new Watermark(event.time.toEpochMilli)) // every new event older than THIS EVENT will be discarded
    }

    // can also choose on periodic emit to MAYBE emit watermarks regularly
    // Flink can also can onPeriodicEmit regularly - up to us to maybe emit a watermark at these times
    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
    }
  }

  def demoEventTime_v2(): Unit = {
    // controls how often Flink calls onPeriodicEmit
    env.getConfig.setAutoWatermarkInterval(1000L) // call onPeriodicEmit every 1s

    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator(context => new BoundedOutOfOrdernessGenerator(500L))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(
      new CountByWindowAll
    )

    countEventsByWindow.print()
    env.execute()
  }
  def main(args: Array[String]): Unit = {
    demoEventTime_v2()
  }
}
