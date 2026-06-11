import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Main {
  def main(args: Array[String]): Unit = {
    // 1. Crear la SparkSession en modo local
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // 2. Inicializar los Accumulators
    val feedsExitoAcc: LongAccumulator = sc.longAccumulator("FeedsExito")
    val feedsFalloAcc: LongAccumulator = sc.longAccumulator("FeedsFallo")
    val postsDescargadosAcc: LongAccumulator = sc.longAccumulator("PostsDescargados")
    val postsFalloAcc: LongAccumulator = sc.longAccumulator("PostsFallo")
    val postsFiltradosAcc: LongAccumulator = sc.longAccumulator("PostsFiltrados")
    val totalCaracteresFiltradosAcc: LongAccumulator = sc.longAccumulator("TotalCaracteresFiltrados")

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)
    val subscriptions = subscriptionOpts.flatten
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // ejercicio 2 
    val postsRDD = subscriptionsRDD.flatMap { subscription =>
      FileIO.downloadFeed(subscription.url) match {
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          feedsFalloAcc.add(1)
          Iterator.empty

        case Some(feedJson) =>
          feedsExitoAcc.add(1)
          try {
            val posts = JsonParser.parsePosts(feedJson, subscription.name)
            postsDescargadosAcc.add(posts.length)
            posts.iterator
          } catch {
            case _: Exception =>
              postsFalloAcc.add(1)
              println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
              Iterator.empty
          }
      }
    }

    val filteredPostsRDD = postsRDD.filter { post =>
      val valid = post.title.nonEmpty && post.selftext.nonEmpty
      if (!valid)
        postsFiltradosAcc.add(1)
      else
        totalCaracteresFiltradosAcc.add(post.title.length + post.selftext.length)
      valid
    }.cache() // cache: este RDD se usa en el conteo Y en el pipeline de entidades

    val totalValidPosts = filteredPostsRDD.count() // acción terminal → activa los accumulators

    val avgChars =
      if (totalValidPosts > 0) totalCaracteresFiltradosAcc.value / totalValidPosts
      else 0

    val stats = Map(
      "feedsSuccess"  -> feedsExitoAcc.value.toInt,
      "feedsFailed"   -> feedsFalloAcc.value.toInt,
      "postsSuccess"  -> postsDescargadosAcc.value.toInt,
      "postsFailed"   -> postsFalloAcc.value.toInt,
      "postsFiltered" -> postsFiltradosAcc.value.toInt,
      "avgChars"      -> avgChars.toInt
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    if (totalValidPosts == 0) {
      println("Error: No valid posts downloaded after filtering")
      filteredPostsRDD.unpersist()
      spark.stop()
      return
    }

    // --- EJERCICIO 3 ---

    // el diccionario se carga en el Driver y Spark lo serializa a cada Worker
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // a) flatMap: cada post → 0..N NamedEntity
    val entitiesRDD = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // b) map: cada entidad → ((tipo, nombre), 1)
    val entityPairsRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // c) reduceByKey: barrera de sincronización — suma conteos por clave
    //    La función debe ser conmutativa y asociativa para que Spark
    //    pueda combinar resultados parciales en cualquier orden.
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)

    // Conteo por tipo (pipeline paralelo sobre el mismo entitiesRDD)
    val typeCountsRDD = entitiesRDD
      .map(entity => (entity.entityType, 1))
      .reduceByKey(_ + _)

    // d) Traer resultados al Driver para formatear e imprimir
    val entityCountsMap: Map[(String, String), Int] = entityCountsRDD.collect().toMap
    val typeCountsArray = typeCountsRDD.collect()
    val typeCountsMap: Map[String, Int] =
      typeCountsArray.toMap + ("total" -> entityCountsMap.values.sum)

    println(Formatters.formatTypeStats(typeCountsMap))
    println()
    println(Formatters.formatEntityStats(entityCountsMap, cmdArgs.topK))

    filteredPostsRDD.unpersist()
    spark.stop()
  }
}
