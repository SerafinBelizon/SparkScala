package com.keepcoding.batchlayer

import java.text.SimpleDateFormat

import com.keepcoding.dominio.com.keepcoding.dominio._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import com.databricks.spark.avro._
import java.sql.Timestamp

object MetricasSparkSQL {

  def run(args: Array[String]) : Unit = {

    // Creamos el SparkSession
    val sparkSession = SparkSession.builder().master(master = "local[*]")
      .appName(name = "Practica Final - Batch Layer - Spark SQL")
      .getOrCreate()

    // Importamos los implicitsS
    import sparkSession.implicits._

    // Cargamos el fichero de entrada
    val rddTransactiones = sparkSession.read.csv(path = s"file:///${args(0)}")
    // Omitimos la cabecera del fichero CSV
    val cabecera = rddTransactiones.first()
    val rddSinCabecera = rddTransactiones.filter(!_.equals(cabecera))
      .map(_.toString().split(","))



    // Creamos Dataframe Clientes
    val dfClientes = rddSinCabecera.map(columna =>
      Cliente(columna(4).trim.toUpperCase)).distinct()
    dfClientes.show(numRows = 10)

    // Creamos el Dataframe de Cuentas Corrientes
    val dfCuentas = rddSinCabecera.map(columna => {
      CuentaCorriente(columna(4).trim.toUpperCase, columna(6).toLong)
    })
    dfCuentas.show(numRows = 10)

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
    // Generamos ID para Transaccion
    val acumuladorTransaccion: LongAccumulator = sparkSession.sparkContext.longAccumulator(name = "IdTransaccion")
    // Creamos el Data Frame de Transacciones
    val dfTransacciones = rddSinCabecera.map(columna => {
      acumuladorTransaccion.add(1)
      Transaccion(acumuladorTransaccion.value, new Timestamp(new SimpleDateFormat("MM/dd/yy HH:mm").parse(columna(0).substring(1, columna(0).length)).getTime),
        columna(4).trim.toUpperCase, columna(2).toDouble,
        columna(10).trim.toUpperCase.dropRight(1), determinarCategoria(columna(10).trim.toUpperCase.dropRight(1)), columna(3).trim.toUpperCase, columna(6).toLong,
        Geolocalizacion(columna(8).toDouble, columna(9).toDouble, columna(5).trim.toUpperCase, "N/A"))
    })
    // Comprobamos que el Dataframe se ha creado correctamente
    dfTransacciones.show(numRows = 10)


    // Creamos las TempViews
    dfClientes.createOrReplaceGlobalTempView(viewName = "CLIENTES")
    dfTransacciones.createOrReplaceGlobalTempView(viewName = "TRANSACCIONES")
    dfCuentas.createGlobalTempView(viewName = "CUENTAS")


    // Transacciones realizadas por Ciudad
    val dfAgruparClientesTotal = sparkSession.sql(sqlText =
      "SELECT Geolocalizacion.ciudad CIUDAD,COUNT(Geolocalizacion.ciudad) TOTAL_TRANSACCIONES " +
        "FROM global_temp.TRANSACCIONES " +
        "GROUP BY Geolocalizacion.ciudad")
    dfAgruparClientesTotal.show(numRows = 10)
    // Guardamos resultado
    dfAgruparClientesTotal.write.mode("append").avro(args(1))

    // Recuperar Clientes que hayan realizado pagos superiores a 500
    val dfSupQuini = sparkSession.sql(sqlText =
      "SELECT COUNT(TRAN.importe) TRANS_SUP_500, CLI.nombre " +
        "FROM global_temp.TRANSACCIONES TRAN " +
        "INNER JOIN global_temp.CLIENTES CLI ON (TRAN.nombre = CLI.nombre) " +
        "WHERE TRAN.importe > 500 " +
        "GROUP BY TRAN.importe, CLI.nombre")
    dfSupQuini.show(numRows = 10)
    // Guardamos resultado
    dfSupQuini.write.mode("append").avro(args(1))

    // Clientes por Ciudad
    val dfAgruparClientes = sparkSession.sql(sqlText =
      "SELECT Geolocalizacion.ciudad CIUDAD, CLI.nombre, COUNT(CLI.nombre) TRANSACCIONES " +
        "FROM global_temp.TRANSACCIONES TRAN " +
        "INNER JOIN global_temp.CLIENTES CLI ON (TRAN.nombre = CLI.nombre) " +
        "GROUP BY Geolocalizacion.ciudad, CLI.nombre")
    dfAgruparClientes.show(numRows = 10)
    // Guardamos resultado
    dfAgruparClientes.write.mode("append").avro(args(1))

    // Transacciones relacionadas con Ocio
    val dfTransaccionesOcio = sparkSession.sql(sqlText =
      "SELECT * " +
        "FROM global_temp.TRANSACCIONES TRAN " +
        "WHERE TRAN.categoria = 'OCIO'")
    dfTransaccionesOcio.show(numRows = 10)
    // Guardamos resultado
    dfTransaccionesOcio.write.mode("append").avro(args(1))

    //Transacciones ultimo mes
    val dfUltimasTransacciones = sparkSession.sql(sqlText =
      "SELECT TRAN.nombre CLIENTE, TRAN.fecha FECHA, TRAN.importe IMPORTE " +
        "FROM global_temp.TRANSACCIONES TRAN " +
        "WHERE TRAN.fecha between '2009-01-01' and '2009-01-30' " +
        "GROUP BY TRAN.nombre, TRAN.fecha, TRAN.importe " +
        "SORT BY TRAN.nombre")
    dfUltimasTransacciones.show(numRows = 10)
    // Guardamos resultado
    dfUltimasTransacciones.write.mode("append").avro(args(1))

    // Clientes que mas dinero han gastado
    val dfMejoresClientes = sparkSession.sql(sqlText =
      "SELECT CLI.nombre, SUM(TRAN.importe) TOTAL_GASTADO " +
        "FROM global_temp.TRANSACCIONES TRAN " +
        "INNER JOIN global_temp.CLIENTES CLI ON (TRAN.nombre = CLI.nombre) " +
        "GROUP BY CLI.nombre " +
        "ORDER BY TOTAL_GASTADO DESC")
    dfMejoresClientes.show(numRows = 10)
    // Guardamos resultado
    dfMejoresClientes.write.mode("append").avro(args(1))

    // Uso de Tarjetas por Ciudad
    val dfTarjetasCiudad = sparkSession.sql(sqlText =
      "SELECT Geolocalizacion.ciudad CIUDAD, TRAN.tarjetaCredito, SUM(TRAN.importe) TRANSACCIONES " +
        "FROM global_temp.TRANSACCIONES TRAN " +
        "GROUP BY Geolocalizacion.ciudad, TRAN.tarjetaCredito")
    dfTarjetasCiudad.show(numRows = 10)
    // Guardamos resultado
    dfTarjetasCiudad.write.mode("append").avro(args(1))
  }
}
