package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object MultipleStreams {

  /*
  * - union
  * - window join
  * - interval join
  * - connect
  * */

  // Unioning - combining the output of multiple streams into just one

  def demoUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // define two streams of the same type
    val shoppingCartEventsKafka = env
      .addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))

    val shoppingCartEventsFiles = env
      .addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("file")))

    val combinedShoppingCartEventStream: DataStream[ShoppingCartEvent] =
      shoppingCartEventsKafka.union(shoppingCartEventsFiles)

    combinedShoppingCartEventStream.print()
    env.execute()
  }

  def demoWindowJoins() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env
      .addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))

    val catalogEvents = env
      .addSource(new CatalogEventsGenerator(200))

    val joinedStream = shoppingCartEvents
      .join(catalogEvents)
      // provide a join condition
      .where(shoppingCartEvent => shoppingCartEvent.userId)
      .equalTo(catalogEvent => catalogEvent.userId)
      // provide the same window grouping
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      // do something with the correlated window
      .apply(
        (shoppingCartEvent, catalogEvent) =>
          s"User ${shoppingCartEvent.userId} browsed at ${catalogEvent.time} and bought ${shoppingCartEvent.time}"
      )

    joinedStream.print()
    env.execute()
  }

  // interval join = correlation between events A & B if durationMin < timeA - timeB < durationMax
  // involves EVENT TIME

  def demoIntervalJoin() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env
      .addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = {
              element.time.toEpochMilli
            }
          })
      )
      .keyBy(_.userId)

    val catalogEvents = env
      .addSource(new CatalogEventsGenerator(500))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
            override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long = {
              element.time.toEpochMilli
            }
          })
      )
      .keyBy(_.userId)

    val intervalJoinedStream = shoppingCartEvents
      .intervalJoin(catalogEvents)
      .between(Time.seconds(-2), Time.seconds(2))
      .lowerBoundExclusive() // interval is by default inclusive
      .upperBoundExclusive()
      .process(new ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String] {
        override def processElement(
                                     left: ShoppingCartEvent,
                                     right: CatalogEvent,
                                     ctx: ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String]#Context,
                                     out: Collector[String]): Unit = {
          out.collect(s"User ${left.userId} browsed at ${right.time} and bought at ${left.time}")
        }
      })

    intervalJoinedStream.print()
    env.execute()
  }


  // connect = 2 streams are treated with the same "operator"
  def demoConnect() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env
      .addSource(new SingleShoppingCartEventsGenerator(100)).setParallelism(1)
    val catalogEvents = env
      .addSource(new CatalogEventsGenerator(1000)).setParallelism(1)

    // connect the streams
    val connectedStream: ConnectedStreams[ShoppingCartEvent, CatalogEvent] = shoppingCartEvents.connect(catalogEvents)

    // variables - will use single-threaded
    env.setParallelism(1)
    env.setMaxParallelism(1)
    val ratioStream: DataStream[Double] = connectedStream.process(
      new CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double] {
        var shoppingCartEventCount = 0
        var catalogEventCount = 0

        override def processElement1(
                                      value: ShoppingCartEvent,
                                      ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
                                      out: Collector[Double]): Unit = {
          shoppingCartEventCount += 1
          out.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }

        override def processElement2(value: CatalogEvent, ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context, out: Collector[Double]): Unit = {
          catalogEventCount += 1
          out.collect(catalogEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }
      }
    )

    ratioStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoConnect()
  }

  // window join = elements belong to the same window + some join condition

}
