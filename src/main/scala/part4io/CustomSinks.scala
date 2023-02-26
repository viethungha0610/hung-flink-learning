package part4io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{FileWriter, PrintStream, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.Scanner

object CustomSinks {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stringsStream: DataStream[String] = env.fromElements(
    "This is an example",
    "some other string",
    "Daniel says this is ok",
    "This is Hung"
  )

  // push the strings to a file sink
  // instantiated once per thread
  class FileSink(path: String) extends RichSinkFunction[String] {

    /*
    - hold state
    - lifecycle methods
     */
    var writer: PrintWriter = _

    // called once per event in the datastream
    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      writer.println(event)
      writer.flush()
      writer.close()
    }

    override def open(parameters: Configuration): Unit = {
      writer = new PrintWriter(new FileWriter(path, true))
    }

    override def close(): Unit = {
      writer.close()
    }
  }

  def demoFileSink(): Unit = {
    stringsStream.addSink(new FileSink("output/demoFileSink.txt"))
    stringsStream.print()
    env.execute()
  }

  /*
  Create a sink function that will push data (as strings) to a socket sink.
   */
  class SocketSink_V0(host: String, port: Int) extends RichSinkFunction[String] {

    var printer: PrintWriter = _

    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      printer.println(event)
    }

    override def open(parameters: Configuration): Unit = {
      printer = new PrintWriter(new Socket(host, port).getOutputStream)
    }

    override def close(): Unit = {
      printer.close()
    }

  }

  def demoSocketSink_V0() = {
    stringsStream.addSink(new SocketSink_V0("localhost", 12345)).setParallelism(1)
    stringsStream.print()
    env.execute()
  }

  class SocketSink(host: String, port: Int) extends RichSinkFunction[String] {

    var socket: Socket = _
    var writer: PrintWriter = _

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      writer.println(value)
      writer.flush()
    }

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host, port)
      writer = new PrintWriter(socket.getOutputStream)
    }

    override def close(): Unit = {
      socket.close() // closes the writer as well
    }

  }

  def demoSocketSink() = {
    // socket can only be instantiated from a single thread
    stringsStream.addSink(new SocketSink_V0("localhost", 12345)).setParallelism(1)
    stringsStream.print()
    env.execute()
  }


  def main(args: Array[String]): Unit = {
//    demoSocketSink()
    demoSocketSink_V0()
  }
}

/*
- start data receiver
- start flink
 */

object DataReceiver {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(12345)
    println("Waiting for Flink to connect ...")

    val socket = server.accept()
    val reader = new Scanner(socket.getInputStream)

    while (reader.hasNextLine) {
      println(s"> ${reader.nextLine()}")
    }

    socket.close()
    println("All data read. Closing app ...")
    server.close()
  }
}
