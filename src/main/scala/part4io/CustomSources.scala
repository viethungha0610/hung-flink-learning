package part4io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{BufferedReader, DataInputStream, InputStreamReader, PrintStream}
import java.net.{ServerSocket, Socket}
import java.util.Scanner
import scala.util.Random

object CustomSources {

  // Source of numbers, randomly generated
  class RandomNumberGeneratorSource(minEventsPerSecond: Double)
    extends RichParallelSourceFunction[Long] {

    // create local fields/methods
    val maxSleepTime = (1000 / minEventsPerSecond).toLong
    var isRunning: Boolean = true

    // called ONCE
    // SourceFunction/RichSourceFunction runs a (single) dedicated thread
    // runs on a dedicated thread

    // Parallel function is called ONCE PER THREAD, each instance has its own thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      while (isRunning) {
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)

        ctx.collect(nextNumber)
      }
    }

    // called at application shutdown
    // contract: the run method should stop immediately
    override def cancel(): Unit = {
      isRunning = false
    }

    // capability of lifecycle methods - initialize state ...
    override def open(parameters: Configuration): Unit = {
      println(s"[${Thread.currentThread().getName}] starting source function")
    }

    override def close(): Unit = {
      println(s"[${Thread.currentThread().getName}] closing")
    }

    // can hold state - ValueState, ListState, MapState
  }

  def demoSourceFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbersStream: DataStream[Long] = env.addSource(new RandomNumberGeneratorSource(10)).setParallelism(8)
    numbersStream.print()
    env.execute()
  }

  /**
   * Create a source function that reads data from a socket.
   *
   * */


  // TODO
  class SocketStringSource_V0(host: String, port: Int) extends SourceFunction[String] {

    var isRunning = true

    val socket = new Socket(host, port)

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val inputStream = new DataInputStream(socket.getInputStream())
      val bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()))

      while (isRunning) {
        val string = bufferedReader.readLine()
        //        println(s"Server says: $string")
        ctx.collect(string)
      }
    }

    override def cancel(): Unit = {
      isRunning = false
      socket.close()
    }
  }

  class SocketStringSource(host: String, port: Int) extends RichSourceFunction[String] {
    var socket: Socket = _
    var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val scanner = new Scanner(socket.getInputStream)

      while (isRunning && scanner.hasNextLine) {
        ctx.collect(scanner.nextLine())
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host, port)
    }

    override def close(): Unit = {
      socket.close()
    }
  }

  def socketStringExercise() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketStrings = env.addSource(new SocketStringSource_V0("127.0.0.1", 12345))

    socketStrings.print()
    env.execute()
  }

  def socketStringSolution() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketStrings = env.addSource(new SocketStringSource("127.0.0.1", 12345))

    socketStrings.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    //    demoSourceFunction()
    //    socketStringExercise()
    socketStringSolution()
  }
}

/*
* - start DataSender
* - start Flink
* - DataSender -> Flink
* */

object DataSender {
  def main(args: Array[String]): Unit = {
    val serverSocket: ServerSocket = new ServerSocket(12345)
    println("Waiting for Flink to connect ...")

    val socket: Socket = serverSocket.accept()
    println("Flink connected. Sending data ...")

    val printer: PrintStream = new PrintStream(socket.getOutputStream)

    printer.println("Hello from the other side ...")
    Thread.sleep(3000)
    printer.println("Almost ready ...")
    Thread.sleep(500)
    (1 to 10).foreach { i =>
      Thread.sleep(200)
      printer.println(s"Number $i")
    }

    println("Data sending completed.")
    serverSocket.close()
  }
}