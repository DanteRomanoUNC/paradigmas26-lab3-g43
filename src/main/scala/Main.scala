import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Main {
  def main(args: Array[String]): Unit = {
    // 1. Crear la SparkSession en modo local
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]") 
      .getOrCreate()
    val sc = spark.sparkContext

    // 2. Inicializar los Accumulators (Requerido para Ejercicio 2.c y Ejercicio 4) 
    val feedsExitoAcc: LongAccumulator = sc.longAccumulator("FeedsExito")
    val feedsFalloAcc: LongAccumulator = sc.longAccumulator("FeedsFallo")
    val postsDescargadosAcc: LongAccumulator = sc.longAccumulator("PostsDescargados")
    val postsFalloAcc: LongAccumulator = sc.longAccumulator("PostsFallo")
    val postsFiltradosAcc: LongAccumulator = sc.longAccumulator("PostsFiltrados")
    // Acumulador auxiliar para calcular el largo promedio de caracteres
    val totalCaracteresFiltradosAcc: LongAccumulator = sc.longAccumulator("TotalCaracteresFiltrados")
    
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }
    val subscriptionsRDD = sc.parallelize(subscriptions)

    val postsRDD = subscriptionsRDD.flatMap { subscription =>
      FileIO.downloadFeed(subscription.url) match {

        case None =>
          println(
            s"Warning: Failed to download from '${subscription.name}' (${subscription.url})"
          )

          feedsFalloAcc.add(1)

          Iterator.empty

        case Some(feedJson) =>

          feedsExitoAcc.add(1)

          try {

            val posts =
              JsonParser.parsePosts(feedJson, subscription.name)

            postsDescargadosAcc.add(posts.length)

            posts.iterator

          } catch {

            case _: Exception =>

              postsFalloAcc.add(1)

              println(
                s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})"
              )

              Iterator.empty
          }
      }
    }

    val filteredPostsRDD = postsRDD.filter { post =>
      val valid =
        post.title.nonEmpty &&
        post.selftext.nonEmpty

      if (!valid)
        postsFiltradosAcc.add(1)

      else
        totalCaracteresFiltradosAcc.add(
          post.title.length + post.selftext.length
        )

      valid
    }.cache()

    val totalValidPosts = filteredPostsRDD.count()
    val feedsSuccess = feedsExitoAcc.value
    val feedsFailed = feedsFalloAcc.value
    val postsSuccess = postsDescargadosAcc.value
    val postsFailed = postsFalloAcc.value
    val postsFiltered = postsFiltradosAcc.value
    val avgChars =
      if (totalValidPosts > 0)
        totalCaracteresFiltradosAcc.value / totalValidPosts
      else
        0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.toInt,
      "feedsFailed" -> feedsFailed.toInt,
      "postsSuccess" -> postsSuccess.toInt,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (totalValidPosts == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }
    val filteredPosts = filteredPostsRDD.collect().toList

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }}
