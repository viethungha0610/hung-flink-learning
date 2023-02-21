package part3state

import generators.shopping.{AddToShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFunctions {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val numbersStream = env.fromElements(1, 2, 3, 4, 5, 6)

  val tenxNumbers = numbersStream.map(_ * 10)

  val tenxNumbers_v2 = numbersStream.map(
    new MapFunction[Int, Int] {
      override def map(value: Int): Int = value * 10
    }
  )

  val tenxNumbersWithLifecycle = numbersStream.map(

    // Rich Map function + lifecycle methods
    new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = value * 10

      // optional overrides: lifecycle methods open/close
      override def open(parameters: Configuration): Unit = println("Starting my work")

      override def close(): Unit = println("Finishing my work")
    })

  val tenxNumbersWithLifecycle_2 = numbersStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10

    override def open(parameters: Configuration): Unit = println("Starting my second work ...")

    override def close(): Unit = println("Finishing my second work ...")
  })

  // ProcessFunction - the most general function abstraction in Flink
  val tenxNumbersProcess: DataStream[Int] = numbersStream.process(
    new ProcessFunction[Int, Int] {
      override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
        out.collect(value * 10)
      }
      // can also override the lifecycle methods
      override def open(parameters: Configuration): Unit = println("Process function starting")

      override def close(): Unit = println("Process function closing")
    }
  )

  /**
   * Exercise: "explode" all purchase events to a single item
   * [("boots", 2), ("iPhone", 1)] ->
   * ["boots", "boots", "iPhone"]
   * - lambdas
   * - explicit functions
   * - rich functions
   * - process functions
   * */

  def exercise(): Unit = {
    val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
    exerciseEnv.setParallelism(1)

    val shoppingCartStream = exerciseEnv.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .map(_.asInstanceOf[AddToShoppingCartEvent])
      .setParallelism(1)

    val skuStream = shoppingCartStream.process(
      new ProcessFunction[AddToShoppingCartEvent, String] {
        override def processElement(value: AddToShoppingCartEvent, ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context, out: Collector[String]): Unit = {
          println(value.toString)
          val sku: String = value.sku
          val quantity: Int = value.quantity
          for {
            i <- 1 to quantity
          } yield out.collect(sku)
        }
      }
    )
      .setParallelism(1)

    skuStream.print()
    exerciseEnv.execute()
  }

  def exerciseSolution(): Unit = {
    val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartStream = exerciseEnv.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .map(_.asInstanceOf[AddToShoppingCartEvent])

    // 1 - lambdas: flatMap
    val itemsPurchasedStream: DataStream[String] = shoppingCartStream.flatMap(
      event => (1 to event.quantity).map(_ => event.sku)
    )

    // 2 - explicit flatMap functions
    val itemPurchasedStream_v2: DataStream[String] = shoppingCartStream.flatMap(new FlatMapFunction[AddToShoppingCartEvent, String] {
      override def flatMap(value: AddToShoppingCartEvent, out: Collector[String]): Unit = {
        (1 to value.quantity).map(_ => value.sku).foreach(_ => out.collect(_))
      }
    })

    // 3 - rich flatMapFunction
    val itemPurchasedStream_v3: DataStream[String] = shoppingCartStream.flatMap(new RichFlatMapFunction[AddToShoppingCartEvent, String] {
      override def flatMap(value: AddToShoppingCartEvent, out: Collector[String]): Unit = {
        (1 to value.quantity).map(_ => value.sku).foreach(_ => out.collect(_))
      }
    })

    // TODO
    exerciseEnv.execute()
  }

  def main(args: Array[String]): Unit = {
    exercise()
  }
}
