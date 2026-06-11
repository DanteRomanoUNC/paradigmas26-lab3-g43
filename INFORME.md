# Ejercicio 1 — Identificar las regiones paralelizables

## A. Diagrama de flujo de dependencias

```text
1. Carga de argumentos de la línea de comandos (Driver)
|
2. Carga de suscripciones (Driver) -> List[Option[Subscription]]
|
3. Filtrado de las suscripciones incorrectas (los None en la lista) (Driver) -> List[Subscription]
|
4. Paralelización de datos (Driver) -> RDD[Subscription]
|
5. flatMap con acumulador de fallo y éxito al descargar y parsear (Worker) -> RDD[Post] 
   (Acumuladores: feedsFailed, feedsSuccess, postsFailed, postsSuccess)
|
6. Filtrado de posts vacíos [filter] (Worker) -> RDD[Post] (Acumulador: postFiltered)
|            |
|            |-- Mapeo a longitud de caracteres [map] (Worker) -> RDD[Int]
|            |
|            |-- Sumatoria total de caracteres [sum o reduce] (Driver) -> Int
|            |
|            |-- Impresión de estadísticas de procesamiento (Driver) -> Unit (Salida por consola)
|
7. Carga de diccionario (Driver) -> List[NamedEntity] 
   (El Driver también manda copia del diccionario a cada Worker)
|
8. flatMap de cada posteo a una lista de entidades detectadas (Worker) -> RDD[NamedEntity]
|            |
|            |-- Preparación para conteo por categoría/tipo [map] (Worker) -> RDD[(String, Int)] 
|            |   donde la tupla es (entityType, 1)
|            |
|            |-- Agrupación y suma por categoría [reduceByKey] (Worker) -> RDD[(String, Int)]
|            |
|            |-- Recolección de resultados por tipo [collect] (Driver) -> Array[(String, Int)]
|
9. Preparación para conteo por entidad específica [map] (Worker) -> RDD[((String, String), Int)] 
   donde la tupla es ((entityType, entityName), 1)
|
10. Agrupación y suma de entidades [reduceByKey] (Worker) -> RDD[((String, String), Int)]
|
11. Recolección de resultados por entidad [collect] (Driver) -> Array[((String, String), Int)]
|
12. Formateo e impresión final de diccionarios/entidades (Driver) -> Unit (Salida por consola)



B. Abstracciones de Spark

    flatMap (Transforma cada elemento en cero o más resultados):

        Paso 5: Proponemos usar flatMap ya que cada URL puede devolver una cantidad variable de posts (N elementos). Además, si la descarga o el parseo fallan, no devuelve ninguno.

        Paso 8: Se utiliza porque al procesar el texto de un post, se pueden detectar múltiples entidades, o bien puede que el post no contenga ninguna entidad relevante.

    map (Transforma cada elemento en exactamente un resultado):

        Sub-paso de 6: Sobre el RDD filtrado, se mapea cada post transformándolo en un único valor entero (Int) correspondiente a su longitud.

        Sub-paso de 8: Cada entidad detectada se transforma en una tupla de clave-valor para prepararla para la suma.

        Paso 9: Similar al anterior, cada entidad se transforma en una tupla compuesta.

    reduceByKey (Combina múltiples elementos agrupando por clave):

        Sub-paso de 8 y Paso 10: Se aplica para tomar todas las tuplas generadas por los map anteriores en todos los Workers y sumar sus valores agrupándolas por su clave.

Pasos que no encajan en estas abstracciones:

    Transformaciones alternativas (filter): El Paso 6 evalúa una condición para mantener o descartar un post.

    Las Acciones (sum y collect): El sub-paso de 6 (sum) y los pasos 8 y 11 (collect) no son transformaciones.

    Lógica secuencial del Driver: Los Pasos 1 al 4, el Paso 7 y el Paso 12 representan código estándar de Scala.

C. Barreras y acciones independientes

Barreras de sincronización:

    Paso 10 y sub-paso de 8 (Agrupación - reduceByKey): Constituyen una barrera porque requieren un proceso de Shuffle. Spark debe pausar el procesamiento y recolectar las tuplas generadas por todos los Workers.

    Sub-paso de 6, sub-paso de 8 y Paso 11 (Acciones - sum, collect): Las operaciones de recolección hacia el Driver son barreras absolutas. El Driver no puede continuar con el código secuencial hasta que el Worker más lento haya terminado su tarea.

Acciones independientes:

    Los Pasos 5 (flatMap), 6 (filter) y los mapeos de los sub-pasos 6, 8 y 9 se ejecutan de forma completamente independiente. Cada Worker procesa su partición de datos sin necesidad de comunicarse con el resto.

D. Restricciones de Spark

    Serialización estricta: El código de la función y todos los objetos externos que esta referencie deben poder convertirse a bytes dado que el Driver compila la función y la envía a través de la red a cada Worker.

    Estado compartido: Las funciones en los Workers no pueden utilizar variables mutables tradicionales del Driver. Para solucionar esto en los Pasos 5 y 6, utilizamos explícitamente Accumulators.

    Efectos secundarios: Las funciones deben ser lo más "puras" posibles. Spark provee tolerancia a fallos recalculando particiones perdidas o reintentando tareas. Esto significa que la función de un Worker podría ejecutarse más de una vez para el mismo dato.
```

# Ejercicio 2 — Manejo de errores y métricas

## Uso de Accumulators

Se utilizaron acumuladores de Spark para registrar métricas globales durante la ejecución distribuida del programa:

* `feedsExitoAcc`: cantidad de feeds descargados correctamente.
* `feedsFalloAcc`: cantidad de feeds cuya descarga falló.
* `postsDescargadosAcc`: cantidad total de posts obtenidos desde los feeds.
* `postsFalloAcc`: cantidad de feeds cuyo parseo falló.
* `postsFiltradosAcc`: cantidad de posts descartados por contener título o contenido vacío.
* `totalCaracteresFiltradosAcc`: suma de caracteres de todos los posts válidos, utilizada para calcular el largo promedio.

Los acumuladores permiten que los Workers reporten información al Driver sin compartir estado mutable, respetando las restricciones del modelo de ejecución de Spark.

## Manejo de errores

Se implementó manejo de excepciones en las operaciones de entrada/salida para evitar que errores externos provoquen la finalización inesperada del programa.

### Lectura de suscripciones

La función `readSubscriptions` captura excepciones asociadas a:

* Archivos inexistentes.
* Archivos JSON con formato inválido.
* Suscripciones mal formadas (sin los campos obligatorios `name` o `url`).

Las suscripciones inválidas son descartadas y se informa la situación mediante mensajes descriptivos.

### Descarga de feeds

La función `downloadFeed` captura errores de red, URLs inválidas y fallas durante la descarga. Ante cualquier error retorna `None`, permitiendo que el procesamiento continúe con el resto de las suscripciones.

### Lectura de diccionarios

La función `readDictionaryFile` captura errores de acceso a archivos y retorna `None` cuando un diccionario no puede ser leído.

La función `loadAll` verifica además la existencia del directorio de entidades y genera advertencias para cada archivo faltante, permitiendo utilizar los diccionarios que sí pudieron cargarse.

### Validaciones adicionales

Antes de continuar con el procesamiento se verifica que existan suscripciones válidas y posts válidos luego del filtrado. En caso contrario, el programa finaliza de manera controlada mostrando un mensaje de error apropiado.

## Casos de prueba realizados

Para validar el manejo de errores se realizaron las siguientes pruebas:

1. Archivo de suscripciones inexistente.
2. Archivo JSON con formato inválido.
3. Suscripciones sin los campos obligatorios.
4. URLs de feeds inválidas o inaccesibles.
5. Directorio de entidades inexistente.
6. Archivos de diccionario faltantes.

En todos los casos el programa respondió correctamente, informando el error correspondiente y evitando excepciones no controladas.

# Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

## reduceByKey como barrera de sincronización

Cuando Spark ejecuta el `reduceByKey`, ocurre un proceso de shuffle: todas las
tuplas `((tipo, nombre), 1)` generadas por los Workers son redistribuidas a través
de la red para que cada clave quede concentrada en un único nodo. Recién entonces
cada Worker puede sumar los valores de su subconjunto de claves y producir el
conteo final.

Esta barrera es inevitable para este problema porque el conteo de una entidad
depende de todos los posts procesados por todos los Workers. No existe forma
de saber cuántas veces aparece "Python" sin haber visto la contribución de cada
partición — es una dependencia global que ninguna transformación independiente
puede resolver.

## Restricciones de la función pasada a reduceByKey

La función debe ser **conmutativa** (`f(a, b) == f(b, a)`) y **asociativa**
(`f(f(a, b), c) == f(a, f(b, c))`).

Estas restricciones existen porque Spark puede aplicar la función en cualquier
orden y combinar resultados parciales antes del shuffle final (optimización llamada
combiner). Si la función no fuera asociativa o conmutativa, el resultado
dependería del orden de ejecución y variaría entre corridas.

La suma `_ + _` cumple ambas propiedades, por lo que es la elección correcta para
contar ocurrencias.

## Dónde se carga el diccionario de entidades

El diccionario se carga en el **Driver**, mediante `Dictionary.loadAll(...)`, antes
de que comience el `flatMap`. Cuando Spark serializa la función del `flatMap` para
enviarla a los Workers, incluye el valor de `dictionary` como parte del closure.
Cada Worker recibe su propia copia serializada del diccionario y la utiliza
localmente para detectar entidades, sin necesidad de acceder al sistema de archivos
ni comunicarse con el Driver durante la ejecución.

Esto implica que el diccionario debe ser serializable y de tamaño razonable para
ser enviado por red. Para diccionarios muy grandes, la alternativa sería usar una
*broadcast variable*, que Spark distribuye de forma más eficiente evitando enviar
una copia por cada tarea.