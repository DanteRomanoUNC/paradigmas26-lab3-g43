A-Diagrama.

1-carga de argumentos de la linea de comandos (Driver)
|
2-carga de suscripciones  (Driver)  -> List[Option[Subscription]]
|
3-filtrado de las suscripciones incorrectas (los None en la lista) (Driver) -> List[Subscription] 
|
4-paralelilazacion de datos (Driver) -> RDD[Subscription]
|
5-flatmap con acumulador de fallo y exito al descargar y parsear(Worker) -> RDD[Post], acumuladores feedsFailed, feedsSuccess, postsFailed, postsSuccess
|
6-filtrado de posts vacíos (filter) (Worker) -> RDD[Post], acumulador postFiltered
|            |
|            |mapeo a longitud de caracteres (map) (Worker) -> RDD[Int]
|            |
|            |sumatoria total de caracteres (sum o reduce) (Driver) -> Int
|            |
|            |impresion de estadisticas de procesamiento (Driver) -> Unit (Salida por consola)
|
|
7-Carga de diccionario (Driver) ->List[NamedEntity] (el driver tambien manda copia del diccionario a cada worker)
|
8-flatmap de cada posteo a una lista de entidades detectadas (Worker) ->RDD[NamedEntity]
|            |
|            |preparación para conteo por categoría/tipo (map) (Worker) -> RDD[(String, Int)] donde la tupla es (entityType, 1)
|            |
|            |agrupación y suma por categoría (reduceByKey) (Worker) -> RDD[(String, Int)]
|            |
|            |recolección de resultados por tipo (collect) (Driver) -> Array[(String, Int)]
|
9-preparación para conteo por entidad específica (map) (Worker) -> RDD[((String, String), Int)] donde la tupla es ((entityType, entityName), 1)
|
10-agrupación y suma de entidades (reduceByKey) (Worker) -> RDD[((String, String), Int)]
|
11-recolección de resultados por entidad (collect) (Driver) -> Array[((String, String), Int)]
|
12-formateo e impresión final de diccionarios/entidades (Driver) -> Unit (Salida por consola)



B-Abstracciones de Spark.

flatMap (Transforma cada elemento en cero o más resultados):

    Paso 5: Proponemos usar flatMap ya que cada URL puede devolver una cantidad variable de posts (N elementos). Además, si la descarga o el parseo fallan, no devuelve ninguno.

    Paso 8: Se utiliza porque al procesar el texto de un post, se pueden detectar múltiples entidades, o bien puede que el post no contenga ninguna entidad relevante.

map (Transforma cada elemento en exactamente un resultado):

    Sub-paso de 6: Sobre el RDD filtrado, se mapea cada post transformándolo en un único valor entero (Int) correspondiente a su longitud.

    Sub-paso de 8: Cada entidad detectada se transforma en una tupla de clave-valor para prepararla para la suma.

    Paso 9: Similar al anterior, cada entidad se transforma en una tupla compuesta.

reduceByKey (Combina múltiples elementos agrupando por clave):

    Sub-paso de 8 y Paso 10: Se aplica para tomar todas las tuplas generadas por los map anteriores en todos los Workers y sumar sus valores agrupándolas por su clave.

Transformaciones alternativas (filter): El Paso 6 evalúa una condición para mantener o descartar un post.

Las Acciones (sum y collect): El sub-paso de 6 (sum) y los pasos 8 y 11 (collect) no son transformaciones.

Lógica secuencial del Driver: Los Pasos 1 al 4, el Paso 7 y el Paso 12 representan código estándar de Scala.


C-Barreras y acciones independientes.

Paso 10 y sub-paso de 8 (Agrupación - reduceByKey): 
Constituyen una barrera porque requieren un proceso de Shuffle. Spark debe pausar el procesamiento y recolectar las tuplas generadas por todos los Workers

Sub-paso de 6, sub-paso de 8 y Paso 11 (Acciones - sum, collect): 
Las operaciones de recolección hacia el Driver son barreras absolutas. El Driver no puede continuar con el código secuencial hasta que el Worker más lento haya terminado su tarea.

Los Pasos 5 (flatMap), 6 (filter) y los mapeos de los sub-pasos 6, 8 y 9 se ejecutan de forma completamente independiente.


D-Restricciones de Spark.

Serializacion estricta:
El código de la función y todos los objetos externos que esta referencie deben poder convertirse a bytes dado que el Driver compila la función y la envía a través de la red a cada Worker.

Estado compartido:
Las funciones en los Workers no pueden utilizar variables mutables tradicionales del Driver.
Para solucionar esto en los Pasos 5 y 6, utilizamos explícitamente Accumulators.

Efectos secundarios:
Las funciones deben ser lo más "puras" posibles.Spark provee tolerancia a fallos recalculando particiones perdidas o reintentando tareas. 
Esto significa que la función de un Worker podría ejecutarse más de una vez para el mismo dato.

