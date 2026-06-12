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
    }.cache() // Ejercicio 5, cache: este RDD se usa en el conteo y en el pipeline de entidades

    // Ejercicio 4: parte c (Primera accion terminal)
    val t0 = System.currentTimeMillis()
    val totalValidPosts = filteredPostsRDD.count() // acción terminal → activa los accumulators y materializa el cahce
    val t1 = System.currentTimeMillis()
    println(s"[Tiempo] Etapa 1 (descarga, parseo y filtrado de posts): ${(t1 - t0) / 1000.0} s")

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
      filteredPostsRDD.unpersist()      // Ejercicio 5: liberar memoria antes de salir, incluso en rutas de error
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
    }.cache() // Ejercicio 5, cache: evita re-detectar entidades 2 veces (conteo por: entidad especifica / tipo)

    // b) map: cada entidad → ((tipo, nombre), 1)
    val entityPairsRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // c) reduceByKey: barrera de sincronización — suma conteos por clave
    //    La función debe ser conmutativa y asociativa para que Spark
    //    pueda combinar resultados parciales en cualquier orden.
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)

    // Conteo por tipo (pipeline paralelo sobre el mismo entitiesRDD, leido del cache)
    val typeCountsRDD = entitiesRDD
      .map(entity => (entity.entityType, 1))
      .reduceByKey(_ + _)

    // Ejercicio 4: parte c (Segunda accion terminal)
    val t2 = System.currentTimeMillis()

    // d) Traer resultados al Driver para formatear e imprimir
    val entityCountsMap: Map[(String, String), Int] = entityCountsRDD.collect().toMap
    val typeCountsArray = typeCountsRDD.collect()

    // Ejercicio 4: parte c
    val t3 = System.currentTimeMillis()
    println(s"[Tiempo] Etapa 2 (detección de entidades, reducción y recolección): ${(t3 - t2) / 1000.0} s")

    val typeCountsMap: Map[String, Int] =
      typeCountsArray.toMap + ("total" -> entityCountsMap.values.sum)

    println(Formatters.formatTypeStats(typeCountsMap))
    println()
    println(Formatters.formatEntityStats(entityCountsMap, cmdArgs.topK))

    Thread.sleep(60000)

    // Ejercicio 5c: liberar memoria una vez que ya no necesitamos los RDDs cacheados
    entitiesRDD.unpersist()
    filteredPostsRDD.unpersist()
    spark.stop()
  }
}
