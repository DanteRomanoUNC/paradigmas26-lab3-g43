import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    implicit val formats: Formats = DefaultFormats

    try {
      val source = Source.fromFile(filePath)
      val content = source.mkString
      source.close()

      val json = parse(content)
      val subscriptions = json.extract[List[Map[String, String]]]

      subscriptions.map { sub =>
        if (sub.contains("name") && sub.contains("url"))
          Some(Subscription(sub("name"), sub("url")))
        else {
          println(
            "Warning: Skipping malformed subscription (missing 'name' or 'url' field)"
          )
          None
        }
      }

    } catch {

      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load $filePath - file not found")
        List()

      case _: Exception =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        List()
    }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    try {
      val connection = new java.net.URL(url).openConnection()
      .asInstanceOf[java.net.HttpURLConnection]
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(15000) // debe superar el sleep(5000) del mock
      val source = Source.fromInputStream(connection.getInputStream)
      val content = source.mkString
      source.close()
      Some(content)
    } catch {
      case _: Exception => None
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      val source = Source.fromFile(filePath)

      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList

      source.close()

      Some(lines)

    } catch {
      case _: Exception => None
    }
  }
}
