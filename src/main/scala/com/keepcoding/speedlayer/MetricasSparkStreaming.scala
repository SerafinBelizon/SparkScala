package com.keepcoding.speedlayer

import java.text.SimpleDateFormat
import java.util.Properties

import com.keepcoding.dominio.com.keepcoding.dominio._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import java.sql.Timestamp


object MetricasSparkStreaming extends App{

  def run(args: Array[String]) : Unit = {

    // Funcion que determina la Categoria en funcion de la Descripcion de la Transaccion
    def determinarCategoria(descripcion: String): String = {
      descripcion match {
        case descripcion if descripcion.equalsIgnoreCase("CAR INSURACE") => "SEGUROS"
        case descripcion if descripcion.equalsIgnoreCase("HOME INSURANCE") => "SEGUROS"
        case descripcion if descripcion.equalsIgnoreCase("LIFE INSURANCE") => "SEGUROS"
        case descripcion if descripcion.equalsIgnoreCase("CINEMA") => "OCIO"
        case descripcion if descripcion.equalsIgnoreCase("RESTAURANT") => "OCIO"
        case descripcion if descripcion.equalsIgnoreCase("SHOPPING MALL") => "OCIO"
        case descripcion if descripcion.equalsIgnoreCase("SPORTS") => "OCIO"
        case descripcion if descripcion.equalsIgnoreCase("LEASING") => "ARRENDAMIENTO"
        case descripcion if descripcion.equalsIgnoreCase("RENT") => "ARRENDAMIENTO"
        case descripcion => "NNN"
      }
    }

    val spark = SparkSession.builder
      .appName("Practica Final - Speed Layer")
      .master("local[*]")
      .getOrCreate

    val kafkaParams = Map[String, Object](
      elems =
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "transacciones",
        "kafka.consumer.id" -> "kafka-consumer-01"
    )

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))


    // Consumer que leera los datos de un producer que enviará los datos desde terminal
    val inputEnBruto: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, String](Array(args(0)), kafkaParams))

    val transaccionesStream: DStream[String] = inputEnBruto.map(_.value())

    val streamTransformado = transaccionesStream.map(_.split(","))
      .map(registro => {
          Transaccion(scala.util.Random.nextInt(Integer.MAX_VALUE), new Timestamp(new SimpleDateFormat("MM/dd/yy HH:mm").parse(registro(0)).getTime),
            registro(4).toString.trim.toUpperCase(), registro(2).toDouble,
            registro(10).toString.trim.toUpperCase, determinarCategoria(registro(10).toString.trim.toUpperCase), registro(3).toString.trim.toUpperCase, registro(6).toLong,
            Geolocalizacion(registro(8).toDouble, registro(9).toDouble, registro(5).toString.trim.toUpperCase, "N/A"))
        }
      )
    streamTransformado.print()


    // Configuracion del topic de salida de kafka
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    // Funciones para escribir los datos de vuelta a Kafka
    def writeTokafkaStringInt(outputTopic: String)(partitionOfRecords: Iterator[(String, Int)]): Unit = {
      val producer = new KafkaProducer[String, String](properties)
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
      producer.flush()
      producer.close()
    }

    def writeTokafkaDoubleString(outputTopic: String)(partitionOfRecords: Iterator[(Double, String)]): Unit = {
      val producer = new KafkaProducer[String, String](properties)
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
      producer.flush()
      producer.close()
    }

    def writeTokafkaStringTupleInt(outputTopic: String)(partitionOfRecords: Iterator[((String, String), Int)]): Unit = {
      val producer = new KafkaProducer[String, String](properties)
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
      producer.flush()
      producer.close()
    }

    def writeTokafkaStringTupleDouble(outputTopic: String)(partitionOfRecords: Iterator[((String, String), Double)]): Unit = {
      val producer = new KafkaProducer[String, String](properties)
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
      producer.flush()
      producer.close()
    }

    def writeTokafkaTransaccion(outputTopic: String)(partitionOfRecords: Iterator[Transaccion]): Unit = {
      val producer = new KafkaProducer[String, String](properties)
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
      producer.flush()
      producer.close()
    }

    def writeTokafkaStringTransaccion(outputTopic: String)(partitionOfRecords: Iterator[(String, Iterable[Transaccion])]): Unit = {
      val producer = new KafkaProducer[String, String](properties)
      partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
      producer.flush()
      producer.close()
    }


    // Transacciones realizadas por Ciudad
    val TransaccionesCiudad = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.map(registro => (registro.geolocalizacion.ciudad, 1)).reduceByKey(_+_)
        .foreachPartition(writeTokafkaStringInt("topicoTransaccionesCiudad"))
    })


    // Clientes que hayan realizado pagos superiores a 500
    val ClientesSupQuini = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.filter(_.importe > 500)
        .map(registro => (registro.nombre, 1)).reduceByKey(_+_)
        .foreachPartition(writeTokafkaStringInt("topicoClientesSupQuini"))
    })


    // Clientes por Ciudad
    val ClientesCiudad = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.map(registro => ((registro.geolocalizacion.ciudad, registro.nombre), 1)).reduceByKey(_+_)
        .foreachPartition(writeTokafkaStringTupleInt("topicoClienteCiudad"))
    })

    // Transacciones relacionadas con Ocio
    val TransaccionesOcio = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.filter(_.categoria == "OCIO")
        .foreachPartition(writeTokafkaTransaccion("topicoTransaccionesOcio"))
    })

    // Transacciones ultimo mes
    val TransaccionesUltimoMes = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.filter(registro => registro.fecha.after(new Timestamp(new SimpleDateFormat("MM/dd/yy HH:mm").parse("01/01/09 00:00").getTime))
        && registro.fecha.before(new Timestamp(new SimpleDateFormat("MM/dd/yy HH:mm").parse("01/30/09 00:00").getTime)))
        .groupBy(registro => registro.nombre)
        .foreachPartition(writeTokafkaStringTransaccion("topicoTransaccionesUltimoMes"))
    })

    // Clientes que mas dinero han gastado
    val MejoresClientes = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.map(registro => (registro.nombre, registro.importe)).reduceByKey(_+_)
        .map(tupla => tupla.swap).sortByKey(false)
        .foreachPartition(writeTokafkaDoubleString("topicoMejoresClientes"))
    })

    // Uso de Tarjetas por Ciudad
    val TarjetasCiudad = streamTransformado.foreachRDD(rdd => {
      val producer = new KafkaProducer[String, String](properties)
      rdd.map(registro => ((registro.geolocalizacion.ciudad, registro.tarjetaCredito), registro.importe)).reduceByKey(_+_)
        .foreachPartition(writeTokafkaStringTupleDouble("topicoTarjetasCiudad"))
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
