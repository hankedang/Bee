package com.bee.core.offset

import java.nio.file.{FileSystems, Paths}

import org.apache.kafka.common.utils.Utils

import java.io._

import com.bee.common.util._


/**
  *
  */
class OffsetMgr(val file: File)
  extends Logging {

  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  file.createNewFile() // in case the file doesn't exist

  def write(offsets: String) {
    lock synchronized {
      // write to temp file and then swap with the existing file
      val fileOutputStream = new FileOutputStream(tempPath.toFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {

        writer.write(offsets)
        writer.newLine()

        writer.flush()
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            error("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
            Runtime.getRuntime.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }

      Utils.atomicMoveWithFallback(tempPath, path)
    }
  }

  def read(): Long = {

    def malformedLineException(line: String) =
      new IOException(s"Malformed line in offset checkpoint file: $line'")

    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      var line: String = null
      try {
        line = reader.readLine()
        if (line == null)
          return 0

        val lines = line.split("|")
        if(lines.length == 2) {
          val now = com.bee.common.util.Utils.getDateStr("yyyyMMddHH")
          if(now.equals(lines(0))) {
            lines(1).toLong
          } else {
            0L
          }
        }
        0L
      } catch {
        case e: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }

}
