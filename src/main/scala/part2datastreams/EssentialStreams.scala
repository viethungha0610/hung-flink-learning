package part2datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object EssentialStreams {

  // 1 - execution env

  def applicationTemplate(): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your DataStreams
    // in between, add any sort of computations
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleNumberStream.print()

    // at the end
    env.execute() // trigger all the computations that were described earlier
  }


  // transformations
  def demoTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    // checking parallelism
    println(s"Current parallelism: ${env.getParallelism}")
    env.setParallelism(2)
    println(s"New parallelism: ${env.getParallelism}")

    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n + 1))

    val filteredNumbers: DataStream[Int] = numbers.filter(_ % 2 == 0)
      /* You can set parallelism for each computations */
      .setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt")
    finalData.setParallelism(3)

    env.execute()
  }

  case class FizzBuzzResult(n: Long, output: String)

  def fizzBuzzFlink(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers = env.fromSequence(1, 100)

    val fizzBuzz = numbers
      .map[FizzBuzzResult] { (x: Long) => x match {
        case n if n % 3 == 0 && n % 5 == 0 => FizzBuzzResult(n, "fizzbuzz")
        case n if n % 3 == 0 => FizzBuzzResult(n, "fizz")
        case n if n % 5 == 0 => FizzBuzzResult(n, "buzz")
        case n => FizzBuzzResult(n, s"$n")
        }
      }
//      .map{ n =>
//      val output =
//        if (n % 3 == 0 && n % 5 == 0) "fizzbuzz"
//        else if (n % 3 == 0) "fizz"
//        else if (n % 5 == 0) "buzz"
//        else s"$n"
//      FizzBuzzResult(n, output)
//    }
      .filter(_.output == "fizzbuzz")
      .map(_.n)

//    val finalData = fizzBuzz.writeAsText("output/fizzBuzzFlink.txt").setParallelism(1)

    fizzBuzz.addSink(
      StreamingFileSink.forRowFormat(
        new Path("output/streaming_sink"),
        new SimpleStringEncoder[Long]("UTF-8")
      )
        .build()
    )
      .setParallelism(1)

    env.execute()
  }

  // explicit transformations
  def demoExplicitTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(1, 100)

    // map doubledNumbvers
    val doubledNumbers = numbers.map(_ * 2)

    val doubledNumbers_v2 = numbers.map(new MapFunction[Long, Long] {
      // declare fields, methods, ...
      override def map(value: Long): Long = value * 2
    })

    val expandedNumbers = numbers.flatMap(n => Range.Long(1, n, 1).toList)

    // explicit version
    val expandedNumbers_v2 = numbers.flatMap(new FlatMapFunction[Long, Long] {
      override def flatMap(n: Long, out: Collector[Long]): Unit =
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) // imperative style - pushes the new element downstream
        }
    })

    // process method
    // ProcessFunction is the most general function to process elements in Flink
    val expandedNumbers_v3 = numbers.process(new ProcessFunction[Long, Long] {
      override def processElement(n: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit = {
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i)
        }
      }
    })

    // reduce
    // happens to keyed streams
    // types are reverse compared to normal hashmap, the latter type is the key
    // e.g. for every long, we attach a key
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)

    val sumByKey = keyedNumbers.reduce(_ + _) // sum up all elements BY KEY

    // reduce - explicit approach
    val sumByKey_v2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      override def reduce(value1: Long, value2: Long): Long =
        value1 + value2
    })

    sumByKey_v2.print()
    env.execute()
  }

  /*
  * Flink Application
  * Needs an environment to run (StreamExecutionEnvironment)
  * rich data structure with access to all Flink APIs
  * description of all streams and transformations
  * lazy evaluation
  */

  /*
  * Watermarks
  * Mechanism to discard late data
  *   Event may arrive much later than they were created
  *   We can set a marker to ignore events older than a threshold (watermark)
  *   As events arrive at Flink, the marker is automatically (or manually) updated
  *
  *
  * Example:
  *   We emit a watermark with every incoming element with event time > current max
  *   Logic: ignore all data older than the watermark minus 2 seconds
  * */

  def main(args: Array[String]): Unit = {
    demoExplicitTransformation()
  }
}
