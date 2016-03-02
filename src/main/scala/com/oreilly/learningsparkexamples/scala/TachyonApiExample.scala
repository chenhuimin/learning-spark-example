package com.oreilly.learningsparkexamples.scala

import java.io.{DataInputStream, DataOutputStream, IOException}

import tachyon.client.file.options.{DeleteOptions, InStreamOptions, OutStreamOptions}
import tachyon.client.file.{FileOutStream, TachyonFile, TachyonFileSystem}
import tachyon.client.{ClientContext, TachyonStorageType, UnderStorageType}
import tachyon.exception.{FileAlreadyExistsException, TachyonException}
import tachyon.{Constants, TachyonURI}

/**
 * Created by user on 2016/3/2.
 */
object TachyonApiExample {
  def main(args: Array[String]) {
    val mMasterLocation = new TachyonURI("tachyon://node3.laanto.com:19998")
    val mTachyonConf = ClientContext.getConf()
    mTachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost)
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort))
    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "false")
    ClientContext.reset(mTachyonConf)
    val tachyonFileSystem: TachyonFileSystem = TachyonFileSystem.TachyonFileSystemFactory.get()
    //set raw data file path
    val rawDataFilePath = "/data/raw/weixin-consultation.parquet"
    //delete raw data file if existed
    val rawDataFileURI = new TachyonURI(rawDataFilePath)
        val rawDataFile: Option[TachyonFile] = Option(tachyonFileSystem.openIfExists(rawDataFileURI))
        if (rawDataFile.isDefined) {
          val deleteOptions: DeleteOptions = new DeleteOptions.Builder(ClientContext.getConf).setRecursive(true).build
          tachyonFileSystem.delete(rawDataFile.get,deleteOptions)
        }
    //    val mMasterLocation = new TachyonURI("tachyon://node3.laanto.com:19998")
    //    val mTachyonConf = ClientContext.getConf()
    //    mTachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost)
    //    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort))
    //    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "false")
    //    ClientContext.reset(mTachyonConf)
    //    val tfs: TachyonFileSystem = TachyonFileSystem.TachyonFileSystemFactory.get()
    //    val firstRead = read(tfs, new TachyonURI("/test/number.txt"))
    //    println("firstRead=" + firstRead)
    //    write(tfs, new TachyonURI("/test/number.txt"), true, 1000)
    //    println("write success")
    //    val secondRead = read(tfs, new TachyonURI("/test/number.txt"))
    //    println("secondRead=" + secondRead)
  }

  @throws(classOf[IOException])
  @throws(classOf[TachyonException])
  private def write(tachyonClient: TachyonFileSystem, filePath: TachyonURI, deleteIfExists: Boolean, mLength: Int) {
    val clientOptions: OutStreamOptions = new OutStreamOptions.Builder(ClientContext.getConf).setTachyonStorageType(TachyonStorageType.STORE).setUnderStorageType(UnderStorageType.NO_PERSIST).build
    val fileOutStream: FileOutStream = getOrCreate(tachyonClient, filePath, deleteIfExists, clientOptions)
    val os: DataOutputStream = new DataOutputStream(fileOutStream)
    try {
      os.writeInt(mLength)
      for (i <- 0 until mLength) {
        os.writeInt(i)
      }
    } finally {
      os.close
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[TachyonException])
  private def read(tachyonClient: TachyonFileSystem, mFilePath: TachyonURI): Boolean = {
    val clientOptions: InStreamOptions = new InStreamOptions.Builder(ClientContext.getConf).setTachyonStorageType(TachyonStorageType.STORE).build
    val file: Option[TachyonFile] = Option(tachyonClient.openIfExists(mFilePath))
    if (file.isDefined) {
      val input: DataInputStream = new DataInputStream(tachyonClient.getInStream(file.get, clientOptions))
      try {
        val length: Int = input.readInt
        for (i <- 0 until length) {
          if (input.readInt() != i) {
            return false;
          }
        }
      } finally {
        input.close
      }
      return true
    } else {
      return false;
    }

  }

  @throws(classOf[IOException])
  @throws(classOf[TachyonException])
  private def getOrCreate(tachyonFileSystem: TachyonFileSystem, filePath: TachyonURI, deleteIfExists: Boolean, clientOptions: OutStreamOptions): FileOutStream = {
    var file: TachyonFile = null
    try {
      file = tachyonFileSystem.open(filePath)
    } catch {
      case e: Exception => {
        file = null
      }
    }
    if (file == null) {
      return tachyonFileSystem.getOutStream(filePath, clientOptions)
    } else if (deleteIfExists) {
      tachyonFileSystem.delete(file)
      return tachyonFileSystem.getOutStream(filePath, clientOptions)
    }
    throw new FileAlreadyExistsException("File exists and deleteIfExists is false")
  }


}
