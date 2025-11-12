package engine.optimization

import org.apache.spark.rdd.RDD

object DataStatistics {
  case class Stats(
      estimatedSize: Long,
      numPartitions: Int,
      avgPartitionSize: Long
  )

  /// Estimate the size of an RDD using sampling
  def estimateSize(rdd: RDD[_], sampleFraction: Double = 0.01): Long = {
    require(sampleFraction > 0 && sampleFraction <= 1.0, 
      "Sample fraction must be between 0 and 1")
    
    val sampledCount = rdd
      .sample(withReplacement = false, sampleFraction)
      .count()
    
    (sampledCount / sampleFraction).toLong
  }

  def getStats(rdd: RDD[_]): Stats = {
    val numPartitions = rdd.getNumPartitions
    val estimatedSize = estimateSize(rdd)
    val avgPartitionSize = if (numPartitions > 0) estimatedSize / numPartitions else 0L
    
    Stats(estimatedSize, numPartitions, avgPartitionSize)
  }

  def optimalPartitions(
      estimatedSize: Long,
      defaultParallelism: Int,
      targetPartitionSize: Long = 128 * 1024 * 1024 // 128 MB
  ): Int = {
    val calculated = (estimatedSize / targetPartitionSize).toInt
    math.max(defaultParallelism, math.min(calculated, defaultParallelism * 4))
  }
}