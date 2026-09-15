import org.apache.spark.{SparkConf, SparkContext}

object Day8DAGExecution {

  def main(args: Array[String]): Unit = {

    println("\n===== DAY 8 - DAG AND SPARK EXECUTION =====")

    val conf = new SparkConf()
      .setAppName("Day8DAGExecution")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // ============================================================
    // 1. JOB WITH MULTIPLE TRANSFORMATIONS AND ACTIONS
    // ============================================================

    println("\n===== 1. JOB WITH MULTIPLE TRANSFORMATIONS AND ACTIONS =====")

    val numbers = sc.parallelize(1 to 20, 4)

    val mapped = numbers.map(_ * 2)

    val filtered = mapped.filter(_ > 20)

    val pairRDD = filtered.map(x => (x % 4, x))

    val reduced = pairRDD.reduceByKey(_ + _)

    println("Original Numbers:")
    println(numbers.collect().mkString(", "))

    println("After MAP (* 2):")
    println(mapped.collect().mkString(", "))

    println("After FILTER (> 20):")
    println(filtered.collect().mkString(", "))

    println("After MAP to Key-Value:")
    println(pairRDD.collect().mkString(", "))

    println("After REDUCEBYKEY:")
    println(reduced.collect().sortBy(_._1).mkString(", "))

    println("\nActions executed:")
    println("- collect() on numbers")
    println("- collect() on mapped")
    println("- collect() on filtered")
    println("- collect() on pairRDD")
    println("- collect() on reduced")

    println("\nEach action creates a Spark job.")
    println("Transformations build the DAG and actions trigger execution.")

    // ============================================================
    // 2. NARROW AND WIDE TRANSFORMATIONS
    // ============================================================

    println("\n===== 2. NARROW AND WIDE TRANSFORMATIONS =====")

    println("Narrow Transformations:")
    println("- map")
    println("- filter")
    println("- flatMap")
    println("- mapPartitions")

    println("\nWide Transformations:")
    println("- reduceByKey")
    println("- groupByKey")
    println("- distinct")
    println("- join")
    println("- repartition")

    println("\nNarrow transformation:")
    println("Each output partition depends on a small number of input partitions.")

    println("\nWide transformation:")
    println("Data must be shuffled across partitions.")

    // ============================================================
    // 3. SHUFFLE BOUNDARY
    // ============================================================

    println("\n===== 3. SHUFFLE BOUNDARY =====")

    println("Pipeline:")
    println("Source RDD")
    println("    |")
    println("    v")
    println("map")
    println("    |")
    println("    v")
    println("filter")
    println("    |")
    println("    v")
    println("reduceByKey")
    println("    |")
    println("    v")
    println("map")
    println("    |")
    println("    v")
    println("Final RDD")

    println("\nShuffle Boundary:")
    println("The shuffle occurs at reduceByKey.")
    println("The map and filter before reduceByKey are narrow transformations.")
    println("reduceByKey is a wide transformation and creates a shuffle boundary.")
    println("The map after reduceByKey belongs to the next stage.")

    // ============================================================
    // 4. JOBS, STAGES, TASKS AND PARTITIONS
    // ============================================================

    println("\n===== 4. JOBS, STAGES, TASKS AND PARTITIONS =====")

    println("Job:")
    println("A job is created when an action is called.")

    println("\nStage:")
    println("A stage is a group of tasks separated by shuffle boundaries.")

    println("\nTask:")
    println("A task is the smallest unit of work sent to an executor.")
    println("Usually one task processes one partition.")

    println("\nPartition:")
    println("A partition is a logical chunk of an RDD.")
    println("Partitions allow Spark to process data in parallel.")

    println("\nRelationship:")
    println("Job -> Stages -> Tasks -> Partitions")

    // ============================================================
    // 5. DAG EXPLANATION
    // ============================================================

    println("\n===== 5. DAG EXPLANATION =====")

    println("DAG means Directed Acyclic Graph.")

    println("\nExample DAG:")
    println("Parallelize")
    println("    |")
    println("    v")
    println("Map")
    println("    |")
    println("    v")
    println("Filter")
    println("    |")
    println("    v")
    println("ReduceByKey")
    println("    |")
    println("    v")
    println("Map")
    println("    |")
    println("    v")
    println("Action")

    println("\nSpark creates the DAG from the transformations.")
    println("The DAG Scheduler divides the DAG into stages.")
    println("Shuffle boundaries separate stages.")

    // ============================================================
    // 6. PREDICT NUMBER OF STAGES
    // ============================================================

    println("\n===== 6. PREDICT NUMBER OF STAGES =====")

    println("Scenario:")
    println("RDD -> map -> filter -> reduceByKey -> map -> filter -> count")

    println("\nStage Prediction:")
    println("Stage 1: map -> filter")
    println("Stage 2: reduceByKey -> map -> filter")

    println("Total Stages: 2")

    println("\nReason:")
    println("map and filter are narrow transformations.")
    println("reduceByKey causes a shuffle.")
    println("The shuffle creates the boundary between Stage 1 and Stage 2.")
    println("The final count action triggers execution.")

    // ============================================================
    // 7. ACTUAL RDD LINEAGE
    // ============================================================

    println("\n===== 7. ACTUAL RDD LINEAGE =====")

    val sales = sc.parallelize(
      Seq(
        ("Laptop", 75000),
        ("Mouse", 1500),
        ("Keyboard", 3000),
        ("Laptop", 75000),
        ("Monitor", 20000),
        ("Mouse", 1500)
      ),
      4
    )

    val salesFiltered = sales.filter(_._2 > 2000)

    val salesMapped = salesFiltered.map {
      case (product, amount) =>
        (product, amount)
    }

    val salesTotal = salesMapped.reduceByKey(_ + _)

    println("Sales Data:")
    sales.collect().foreach(println)

    println("\nFiltered Sales:")
    salesFiltered.collect().foreach(println)

    println("\nFinal Product Totals:")
    salesTotal.collect().sortBy(_._1).foreach(println)

    println("\nRDD Debug Lineage:")
    println(salesTotal.toDebugString)

    // ============================================================
    // 8. PARTITIONS AND TASKS
    // ============================================================

    println("\n===== 8. PARTITIONS AND TASKS =====")

    println(s"Input RDD Partitions: ${sales.getNumPartitions}")
    println("Each partition can be processed by a separate task.")

    println("\nBefore shuffle:")
    println("Tasks process the input partitions in parallel.")

    println("\nAt reduceByKey:")
    println("Data is shuffled according to keys.")

    println("\nAfter shuffle:")
    println("New tasks process the shuffled partitions.")

    // ============================================================
    // 9. FINAL SUMMARY
    // ============================================================

    println("\n===== 9. FINAL SUMMARY =====")

    println("1. Transformations are lazy.")
    println("2. Actions trigger Spark jobs.")
    println("3. A job is divided into stages.")
    println("4. Shuffle boundaries separate stages.")
    println("5. Narrow transformations do not require a shuffle.")
    println("6. Wide transformations require a shuffle.")
    println("7. Tasks process partitions.")
    println("8. Spark uses the DAG to optimize execution.")

    println("\n===== DAY 8 DAG AND SPARK EXECUTION COMPLETED =====")

    sc.stop()
  }
}
