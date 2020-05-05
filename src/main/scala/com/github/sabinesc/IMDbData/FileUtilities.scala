package com.github.sabinesc.IMDbData

import scala.language.postfixOps
import sys.process._
import java.net.URL
import java.io.File

object FileUtilities {
  def fileDownloader(url:String, fileName:String, newLocation:String):Unit = {
    new URL(url) #> new File(newLocation+"/"+fileName) !!
  }
}
