package part2datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration.DurationInt

object WindowFunction {

  // use-case: stream of events for a gaming session
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z") // is used in ServerEvent

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player Bob registered 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(3.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  // how many players were registered every 3 seconds?
  // [0...3s] [3...6s] [6...9s]

  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks( // extract timestamps for events (event time) + watermarks
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // once you get an event with time T, you will NOT accept further events with time < T - 500
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long =
            element.eventTime.toEpochMilli
        })
    )

  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    // ^input    ^output  ^window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative: process window function which offers a much richer API (lower-level)
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window: TimeWindow = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindowV2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative 2: aggregate function
  class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                              ^input    ^acc   ^output

    override def createAccumulator(): Long = 0L

    // every element increases the accumulator by 1
    override def add(value: ServerEvent, accumulator: Long): Long = {
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator
    }

    // push a final output out of the final accumulator
    override def getResult(accumulator: Long): Long = accumulator

    // accum1 + accum2 = a bigger accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

  def demoCountByWindowV3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  /**
   * Keyed streams and window functions
   */

  // each element will be assigned to a "mini-stream" for its own key
  val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName)
  //          ^event type  ^key type

  // for every key, we'll have a separate window allocation
  val threeSecondsTumblingWindowsByType = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(s"$key - $window - ${input.size}")
    }
  }


  class CountByWindowV2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(s"$key - ${context.window} - ${elements.size}")
    }
  }

  // process function by function
  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindowsByType.apply(new CountByWindow)
    finalStream.print()
    env.execute()
  }

  def demoCountByTypeByWindow_v2(): Unit = {
    val finalStream = threeSecondsTumblingWindowsByType.process(new CountByWindowV2)
    finalStream.print()
    env.execute()
  }

  // one task processes all the data for a particular key

  /**
   * Sliding windows
   */

  // how many players were registered every 3 seconds, UPDATED EVERY 1s?
  // [0...3s] [1...4s] [2...5s] ...

  def demoSlidingAllWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)

    val slidingWindowsAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)

    registrationCountByWindow.print()

    env.execute()
  }

  /**
   * Session windows = groups of events with NO MORE THAN a certain time gap in between them
   *
   */
  // how many registration events do we have NO MORE than 1s apart?

  def demoSessionWindows(): Unit = {
    val groupBySessionWindows = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))

    // operate any kind of window function
    val countBySessionWindows = groupBySessionWindows.apply(new CountByWindowAll)

    // same thing as before
    countBySessionWindows.print()
    env.execute()
  }

  /**
   * Global window
   */
  // how many registration events do we have every 10 events?

  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    // ^input    ^output  ^window type
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window}] $registrationEventCount")
    }
  }

  def demoGlobalWindow(): Unit = {
    val globalWindowsEvents = eventStream.windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of[GlobalWindow](10))
      .apply(new CountByGlobalWindowAll)

    globalWindowsEvents.print()
    env.execute()
  }

  /**
   * Exercise: what was the time window (continuous 2s) when we had the most registration events?
   * eventStream.executeAndCollect()
   */
  class CountByWindowAllV3 extends AllWindowFunction[ServerEvent, (TimeWindow, Int), TimeWindow] {
                                                  // ^input    ^output  ^window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Int)]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      println(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
      out.collect((window, registrationEventCount))
    }
  }

  def windowFunctionExercise(): Unit = {
//    val serverEvents: CloseableIterator[ServerEvent] = eventStream.executeAndCollect()
    val windowSize = Time.seconds(2)
    val slidingTime = Time.seconds(1)

    val slidingWindowAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

    val registrationCountByWindow = slidingWindowAll.apply(new CountByWindowAllV3)

    val resultTuple: (TimeWindow, Int) = registrationCountByWindow.executeAndCollect().maxBy(_._2)

    print(s"Result time window is ${resultTuple._1}")

//    env.execute()
  }

  /**
   * Solution below
   */

  class KeepWindowAndCountFunction extends AllWindowFunction[ServerEvent, (TimeWindow, Long), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Long)]): Unit = {
      out.collect((window, input.size))
    }
  }

  def windowFunctionsSolution(): Unit = {
    val slidingWindows: DataStream[(TimeWindow, Long)] = eventStream
      .filter(_.isInstanceOf[PlayerRegistered])
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .apply(new KeepWindowAndCountFunction)

    val localWindows: List[(TimeWindow, Long)] = slidingWindows.executeAndCollect().toList
    val bestWindow = localWindows.maxBy(_._2)

    println(s"The best time window is ${bestWindow._1} with ${bestWindow._2} registration events")
  }

  def main(args: Array[String]): Unit = {
//    windowFunctionExercise()
    demoSlidingAllWindows()
  }

}
