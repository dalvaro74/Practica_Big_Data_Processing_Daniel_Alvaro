package principal

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import util.Util._

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
    val listaNegra = spark.read.textFile("AlertWords/listaNegra.txt")

    // A continuacion nos conectaremos al topico de Kafka que nos manda los mensajes de los distintos IoTs
    // que estan espiando (se llama Iot)
    // Por sencillez la estructura de los datos que nos llegan son directamente los mensajes interceptados
    // En nuevas versiones se tendra en cuenta los ids de usaurios y de los IoTs

    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","bigdata")
      .load()
      .selectExpr("CAST(value AS STRING) AS Mensaje","timestamp as Tiempo")
      .as[(String, Timestamp)]
    // En el selectExpr he incluido el timestamp de kafa

    //Separamos las palabras de cada frase con su respectivo timestamp
    val palabras = data.as[(String, Timestamp)]
      .flatMap(par => par._1.split(" ")
        .map(palabra => (palabra, par._2)))
          .toDF("palabra","timestamp") //Con esto pasamos palabras de un RDD a un DataFrame


    // A continuacion aplicaremos la funcion de desencriptado haciendo un maping sobre los palabras
    // Para ello usaremos la funcion mapDesco definida en el package util
    //Vamos a suponer que los ciudanos no son demasiado cuidadosos con su encriptado y que lo
    // unico que hacen es cambiar la "a" por la "o", la "i" por la "u" y viceversa
    val palabrasDesco = palabras.as[(String, Timestamp)]
      .map(p => (p._1.map(mapDesco),p._2))
      .toDF("palabra","timestamp")


    //Obtenemos el recuento de palabras para una ventana de tiempo de una hora usando  windowing
    // Para 3600 segundos hemos considerado un slide de 30 (confio en que estara bien)
    val palabrasDescoVentana = palabrasDesco.
      groupBy(window($"timestamp", "60 seconds","10 seconds"),$"palabra")
      .count()
      .orderBy("window")


    //Para ordenar el DF por el numero repeticiones (count) y quedarnos con las 10 primeras
    val resumenPalabras = palabrasDescoVentana.select("palabra").orderBy("count")

    //Como la opcion limit no funciona no tengo claro como hacer para que solo salgan las diez primeras

    //Con el codigo de abajo (writeStream) pintariaos cada hora la lista de palabras mas usadas, en este caso
    // lo mostaria como un show que truncaria en los 20 primeros
    val query = resumenPalabras.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation","checkpoint")
      .start()
      .awaitTermination()



  /*
    Por ultimo y una vez llegado al punto de pintar por consola la lista de palabras mas usadas
    Soy consciente de que falta la parte de eliminar las palabras de descarte que estan recogidas en el DataSet
    palabrasDescartadas
    Tambien deberiamos tener en cuenta el DataSet de la ListaNegra para comprobar si se hace necesario mandar un
    mensaje de aviso al minitro del interior

    Supongo que podriamos hacerlo a traves de joins como vimos en clase pero ambos temas escapan a mi conocimiento
     */

    spark.sparkContext.stop()
  }
}
