package com.keepcoding

import com.keepcoding.speedlayer.MetricasSparkStreaming

object StreamingApplication {

  def main(args: Array[String]): Unit = {
    if(args.length == 1) {
      MetricasSparkStreaming.run(args)
    } else {
      println("Debe introducirse topico")
    }
  }
}
