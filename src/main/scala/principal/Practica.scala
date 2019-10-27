package principal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Practica {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Practica Procesing")
      .master("local[*]")
      .getOrCreate()

    //Para importar las conversiones implicitas de spark
    import spark.implicits._

    // En primer lugar vamos a generar el DataSet (DS en adelante) con las palabras que habra que descartar
    // como son las preposiciones, articulos, conjunciones, abrviaturas, etc
    val palabrasDescartadas = spark.read.textFile("resources")

    //De la misma manera crearemos el DataSet con las palabras de la lista Negra
    val listaNegra = spark.read.textFile("AlertWord/listaNegra.txt")

    // A continuacion nos conectaremos al topico de Kafka que nos manda los mensajes de los distintos IoTs
    // que estan espiando (se llama Iot)
    // Por sencillez la estructura de los datos que nos llegan son directamente los mensajes interceptados
    // En nuevas versiones se tendra en cuenta los ids de usaurios y de los IoTs

    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","iot")
      .load()
      .selectExpr("CAST(value AS STRING) AS Mensaje","timestamp as Tiempo")
      .as[String]

    // En el selectExpr he incluido el timestamp de kafa

    /************
     * data no es un DataSet Normal, "Queries with streaming sources must be executed with writeStream.start()"
     * Desgraciadamente no he sido capaz de adquir los conocimientos necesarios para saber tratarlos, asi que
     * a partir de aqui mi codigo no compila y sera unicamente ilustrativo
     *
     ************/

    //Lo siguiente seria llevar a cabo el conteo de palabras y o
    val conteoPalabras = data.flatMap(entrada => entrada.split(" ")).groupBy("value").count()

    //Una vez obtenido el dataset con las palabras deberiamos hacer un join left (que no incluya la interseccion)
    // entre conteoPalabras y palabrasDescartadas
    val palabrasUtiles = conteoPalabras.join(palabrasDescartadas,"joinExpression","left_outer")

    // Y por ultimo hacer un inner join entre palabrasUtiles y listaNegra para ver si hay palabras
    val palabrasProhibidas = palabrasUtiles.join(listaNegra,"joinExpression","inner")

    // Si palabrasProhibidas fuera distinto de null debreriamos escribir un mensaje mostrando dichas palabras
    // al ministro de Interior. Para ello deberiamos usar writeStream, pero tampoco tengo nada claro como hacerlo

    val query = palabrasProhibidas.writeStream
      .outputMode("update")
      .format("console")
      .option("checkpointLocation","checkpoint")
      .start()
      .awaitTermination()

    spark.sparkContext.stop()
  }
}
