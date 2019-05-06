package com.keepcoding.dominio

package com.keepcoding.dominio

import java.sql.Timestamp

//import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp


case class Geolocalizacion(latitud: Double, longitud: Double, ciudad: String, pais: String)

case class Transaccion(ID_Transaccion: Long, fecha: Timestamp, nombre: String, importe: Double, descripcion: String, categoria: String, tarjetaCredito: String, cuentaCorriente: Long,
                       geolocalizacion: Geolocalizacion)

case class Cliente(nombre: String)

case class CuentaCorriente(nombre: String, cuentaCorriente: Long)