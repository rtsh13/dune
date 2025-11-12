package engine.optimization

import org.apache.spark.rdd.RDD
import engine.storage._

object AdaptiveSelector {
  // Thresholds for strategy selection
  private val SMALL_VECTOR_THRESHOLD = 50000L
  private val LARGE_VECTOR_THRESHOLD = 1000000L
  private val SKEW_THRESHOLD = 10.0

  def adaptiveSpMV(
      matrixA: RDD[COOEntry],
      vectorX: RDD[(Int, Double)]
  ): RDD[(Int, Double)] = {
    val vectorStats = DataStatistics.getStats(vectorX)

    println(
      s"Vector stats: size=${vectorStats.estimatedSize}, " +
        s"partitions=${vectorStats.numPartitions}, " +
        s"avg_partition=${vectorStats.avgPartitionSize}"
    )

    val strategy =
      selectStrategy(vectorStats.estimatedSize, vectorStats.avgPartitionSize)

    println(s"Selected strategy: $strategy")

    strategy match {
      case Strategy.MapSideJoin => 
        OptimizationStrategies.cooSpMV_CoPartitioning(matrixA, vectorX)  // ← Use existing
      
      case Strategy.OptimizedShuffle => 
        OptimizationStrategies.cooSpMV_InPartitionAgg(matrixA, vectorX)  // ← Use existing
      
      case Strategy.InPartitionAgg => 
        OptimizationStrategies.cooSpMV_InPartitionAgg(matrixA, vectorX)
      
      case Strategy.LoadBalanced => 
        OptimizationStrategies.cooSpMV_Balanced(matrixA, vectorX)
      
      case Strategy.Baseline => 
        OptimizationStrategies.cooSpMV_Baseline(matrixA, vectorX)
    }
  }

  private def selectStrategy(
      vectorSize: Long,
      avgPartitionSize: Long
  ): Strategy = {
    vectorSize match {
      // Small vector: map-side join minimizes shuffle
      case size if size < SMALL_VECTOR_THRESHOLD =>
        Strategy.MapSideJoin

      // Medium vector: in-partition aggregation reduces shuffle volume
      case size if size < LARGE_VECTOR_THRESHOLD =>
        Strategy.InPartitionAgg

      // Large vector: check for skew
      case _ =>
        if (avgPartitionSize > 0 && false) {
          Strategy.LoadBalanced
        } else {
          Strategy.OptimizedShuffle
        }
    }
  }

  sealed trait Strategy
  object Strategy {
    case object MapSideJoin extends Strategy {
      override def toString: String = "Map-Side Join"
    }
    case object OptimizedShuffle extends Strategy {
      override def toString: String = "Optimized Shuffle"
    }
    case object InPartitionAgg extends Strategy {
      override def toString: String = "In-Partition Aggregation"
    }
    case object LoadBalanced extends Strategy {
      override def toString: String = "Load Balanced"
    }
    case object Baseline extends Strategy {
      override def toString: String = "Baseline"
    }
  }
}
