// package benchmarks

// import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.spark.sql.SparkSession
// import engine.storage._
// import scala.collection.mutable.ArrayBuffer

// object ScalabilityBenchmarks {
  
//   def runScalabilityTest(
//     baseConf: SparkConf,
//     matrixSize: Int,
//     sparsity: Double,
//     dataDir: String,
//     workerCounts: Seq[Int] = Seq(1, 2, 4, 8)
//   ): Seq[(Int, Double, Double)] = {
    
//     val results = ArrayBuffer[(Int, Double, Double)]()
    
//     for (workers <- workerCounts) {
//       println(s"\n=== Testing with $workers workers ===")
      
//       val conf = baseConf.clone()
//         .set("spark.executor.instances", workers.toString)
//         .set("spark.default.parallelism", (workers * 2).toString)
      
//       val sc = new SparkContext(conf)
      
//       try {
//         val matrix = SmartLoader.loadMatrix(
//           sc,
//           s"$dataDir/sparse_matrix_${matrixSize}x${matrixSize}.csv"
//         )
//         val vector = SmartLoader.loadVector(
//           sc,
//           s"$dataDir/vector_${matrixSize}.csv"
//         )
        
//         // Warmup
//         (matrix * vector).entries.count()
        
//         // Benchmark
//         val times = (1 to 3).map { _ =>
//           System.gc()
//           val start = System.nanoTime()
//           (matrix * vector).entries.count()
//           (System.nanoTime() - start) / 1000000.0
//         }
        
//         val avgTime = times.sum / times.length
//         val speedup = results.headOption.map(_._2 / avgTime).getOrElse(1.0)
//         val efficiency = speedup / workers
        
//         results += ((workers, avgTime, efficiency))
        
//         println(f"Avg time: ${avgTime}%.2f ms")
//         println(f"Speedup: ${speedup}%.2fx")
//         println(f"Efficiency: ${efficiency * 100}%.1f%%")
        
//       } finally {
//         sc.stop()
//       }
//     }
    
//     results.toSeq
//   }
// }