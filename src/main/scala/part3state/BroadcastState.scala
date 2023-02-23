package part3state

import generators.shopping._
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastState {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100))
  val eventsByUser = shoppingCartEvents.keyBy(_.userId)

  // issue a warning if quantity > threshold

  def purchaseWarnings(): Unit = {
    val threshold = 2

    val notificationsStream = eventsByUser
      .filter((_.isInstanceOf[AddToShoppingCartEvent]))
      .filter(_.asInstanceOf[AddToShoppingCartEvent].quantity > threshold)
      .map(
        event => event match {
          case AddToShoppingCartEvent(userId, sku, quantity, time) =>
            s"User $userId attempting to purchase $quantity items of $sku when threshold is $threshold"
        }
      )

    notificationsStream.print()
    env.execute()
  }

  // ... if the threshold CHANGES over time
  // thresholds will be BROADCAST

  def changingThreshold(): Unit = {
    val threshold: DataStream[Int] = env.addSource(new SourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        List(2, 0, 4, 5, 6, 3).foreach { newThreshold =>
          Thread.sleep(1000)
          ctx.collect(newThreshold)
        }
      }

      override def cancel(): Unit = ()
    })

    // broadcast state is ALWAYS a map
    val broadcastStateDescriptor = new MapStateDescriptor[String, Int]("threshold", classOf[String], classOf[Int])
    val broadcastThreshold: BroadcastStream[Int] = threshold.broadcast(broadcastStateDescriptor)

    val notificationsStream = eventsByUser
      .connect(broadcastThreshold)
      .process(new KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String] {
        //                                        ^key    ^ first event    ^broadcast ^output

        val thresholdDescriptor = new MapStateDescriptor[String, Int]("threshold", classOf[String], classOf[Int])

        override def processElement(
                                     shoppingCartEvent: ShoppingCartEvent,
                                     ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#ReadOnlyContext,
                                     out: Collector[String]): Unit = {
          shoppingCartEvent match {
            case AddToShoppingCartEvent(userId, sku, quantity, item) => {
              val currentThreshold: Int = ctx.getBroadcastState(thresholdDescriptor).get("quantity-threshold")
              if (quantity > currentThreshold) {
                out.collect(s"User $userId attempting to purchase $quantity when the threshold is $currentThreshold")
              }
            }

            case _ =>
          }
        }

        override def processBroadcastElement(
                                              newThreshold: Int,
                                              ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#Context,
                                              out: Collector[String]): Unit = {
          println(s"Threshold about to be changed -- $newThreshold")
          val stateThresholds = ctx.getBroadcastState(thresholdDescriptor)
          // update the state
          stateThresholds.put("quantity-threshold", newThreshold)
        }
      })

    notificationsStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    changingThreshold()
  }
}
