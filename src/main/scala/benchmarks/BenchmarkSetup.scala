package benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._

object BenchmarkSetup {
  def logEnvironment(sc: SparkContext): Unit = {
    
    // Hardware
    println("\n--- Hardware ---")
    println(s"Available Processors: ${Runtime.getRuntime.availableProcessors()}")
    println(s"Max Memory: ${Runtime.getRuntime.maxMemory() / (1024*1024*1024)} GB")
    
    // Spark Configuration
    println("\n--- Spark Configuration ---")
    println(s"Spark Version: ${sc.version}")
    println(s"Master: ${sc.master}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")
    sc.getConf.getAll.foreach { case (k, v) => println(s"$k = $v") }
    
    // System Info
    println("\n--- System Info ---")
    println(s"OS: ${System.getProperty("os.name")}")
    println(s"Java Version: ${System.getProperty("java.version")}")
    println(s"Scala Version: ${util.Properties.versionString}")
  }
}