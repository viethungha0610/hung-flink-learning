package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object Partitions {

  // splitting = partitioning
  def demoPartitioner(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100)) // 10 events/s

    // partitioner = logic to logic the data
    val partitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        // hash code % number of partition ~ even distribution
        println(s"Number of max partition: $numPartitions")
        key.hashCode % numPartitions
      }
    }

    val badPartitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        numPartitions - 1 // last partition index since it starts from 0
      }
    }

    /*
    * Bad because
    * - you lose parallelism
    * - you risk overloading the task with the disproportionate data
    *
    * Good for e.g. sending HTTP requests
    * */

    val partitionedStream = shoppingCartEvents.partitionCustom(
      partitioner,
      event => event.userId
    )

    val badPartitionedStream = shoppingCartEvents.partitionCustom(
      badPartitioner,
      event => event.userId
    )
      // redistribution of data evenly
      // involves data transfer through network
      .shuffle


    badPartitionedStream.print()
    env.execute()
  }


  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }
}
