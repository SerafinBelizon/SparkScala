package com.keepcoding

import com.keepcoding.batchlayer.MetricasSparkSQL

object BatchApplication {

   def main(args: Array[String]): Unit = {
     if(args.length == 2) {
       MetricasSparkSQL.run(args)
     } else {
       println("Debe introducirse Path de Entrada y de Salida")
     }
   }

}
