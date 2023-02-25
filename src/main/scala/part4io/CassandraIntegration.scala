package part4io

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object CassandraIntegration {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int)

  // write data to Cassandra
  def demoWriteDataToCassandra() = {
    val people = env.fromElements(
      Person("Daniel", 99),
      Person("Alice", 12),
      Person("Julie", 14),
      Person("Mom", 54),
    )

    val personTuples: DataStream[(String, Int)] = people.map {
      p => (p.name, p.age)
    }

    CassandraSink.addSink(personTuples)
      .setQuery("insert into rtjvm.people(name, age) values (?, ?)")
      .setHost("localhost")
      .build()

    env.execute()

  }

  // we can only write TUPLES to Cassandra

  def main(args: Array[String]): Unit = {
    demoWriteDataToCassandra()
  }
}
