/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
  * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
  *
  * The mechanism is as follows:
  *
  * The driver divides the serialized object into small chunks and
  * stores those chunks in the BlockManager of the driver.
  *
  * On each executor, the executor first attempts to fetch the object from its BlockManager. If
  * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
  * other executors if available. Once it gets the chunks, it puts the chunks in its own
  * BlockManager, ready for other executors to fetch from.
  *
  * This prevents the driver from being the bottleneck in sending out multiple copies of the
  * broadcast data (one per executor).
  *
  * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
  *
  * @param obj object to broadcast
  * @param id  A unique identifier for the broadcast variable.
  */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
    * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
    * which builds this value by reading blocks from the driver and/or other executors.
    *
    * On the driver, if the value is required, it is read lazily from the block manager.
    */
  /**
    * 当 TorrentBroadcast 实例的_value属性值在需要的时候，才会调用 readBroadcastBlock 方法获取值。
    */
  @transient private lazy val _value: T = readBroadcastBlock()

  /** The compression codec to use, or None if compression is disabled */
  /**
    * 用于广播对象的压缩解码器
    */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  /**
    * 每个块的大小，是个只读属性，可以使用spark.broadcast.blockSize属性配置，默认是4MB.
    */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
    checksumEnabled = conf.getBoolean("spark.broadcast.checksum", true)
  }

  setConf(SparkEnv.get.conf)
  /**
    * 广播id
    */
  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains. */
  /**
    * 在构造 TorrentBroadcast 实例的时候就会调用writeBlocks方法。
    */
  private val numBlocks: Int = writeBlocks(obj)

  /** Whether to generate checksum for blocks or not. */
  private var checksumEnabled: Boolean = false
  /** The checksum for all the blocks. */
  private var checksums: Array[Int] = _

  override protected def getValue() = {
    _value
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position, block.limit - block.position)
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
    * Divide the object into multiple blocks and put those blocks in the block manager.
    *
    * @param value the object to divide
    * @return number of blocks this broadcast variable is divided into
    */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    /**
      * 获取当前 SparkEnv 的 BlockManager组件。
      */
    val blockManager = SparkEnv.get.blockManager

    /**
      * 调用BlockManager的putSingle方法将广播对象写入本地的存储体系。
      */
    if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }

    /**
      * 将对象转换成一系列的块。每个块的大小由BlockSize决定。
      */
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)

    /**
      * 是否需要给分片广播生成校验和。
      */
    if (checksumEnabled) {
      checksums = new Array[Int](blocks.length)
    }
    blocks.zipWithIndex.foreach { case (block, i) =>
      if (checksumEnabled) {
        checksums(i) = calcChecksum(block)
      }
      val pieceId = BroadcastBlockId(id, "piece" + i)
      val bytes = new ChunkedByteBuffer(block.duplicate())
      if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    // 返回块的数量
    blocks.length
  }

  /** Fetch torrent blocks from the driver and/or other executors. */
  /**
    * 调用 readBlocks方法从Driver，Executor的存储体系中获取块。
    *
    * @return
    */
  private def readBlocks(): Array[ChunkedByteBuffer] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    // 新建数组，用来存储每个分片广播块。
    val blocks = new Array[ChunkedByteBuffer](numBlocks)
    // 获取当前SparkEnv 的 BlockManager组件
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      // 使用blockManager的getLocalBytes方法从本地的存储体系中获取序列化的分片广播块。
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          // 如果本地可以获取到，则将分片广播块放入blocks，并且调用releaseLock方法释放此分片广播块的锁。
          blocks(pid) = block
          releaseLock(pieceId)
        case None =>
          // 如果本地没有，则调用BlockManager 的 getRemoteBytes方法从远端的存储体系中获取分片广播块。
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              // 判断是否用来计算校验和。
              if (checksumEnabled) {
                // 用来获取存入checksums数组的校验和。
                val sum = calcChecksum(b.chunks(0))
                // 如果校验和不相同，说明块的数据有损坏，此时会抛出异常。
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              // 如果校验和相同，则调用BlockManager的putBytes方法将分片广播块写入本地存储体系中。
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = b
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    // 返回blocks中的所有分片广播。
    blocks
  }

  /**
    * Remove all persisted state associated with this Torrent broadcast on the executors.
    */
  /**
    * 对由 id 标记的广播对象去持久化
    *
    *
    * @param blocking
    */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
    * Remove all persisted state associated with this Torrent broadcast on the executors
    * and driver.
    */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  /**
    * 当 TorrentBroadcast 实例的_value属性值在需要的时候，才会调用 readBroadcastBlock 方法获取值。
    *
    * @return
    */
  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)
      // 获取当前SparkEnv的BlockManager组件
      val blockManager = SparkEnv.get.blockManager
      // 调用BlockManager的getLocalValues方法从本地的存储系统中获取广播对象。
      blockManager.getLocalValues(broadcastId).map(_.data.next()) match {
        case Some(x) =>
          // 如果从本地的存储体系中可以获取广播对象，则调用releaseLock方法（这个锁保证当块被一个运行中的任务使用时，不能被其他任务再次使用）
          releaseLock(broadcastId)
          x.asInstanceOf[T]
        // 如果从本地的存储系统中没有获取到广播对象，那说明数据是通过BlockManager的putBytes方法以序列化方式写入存储体系中。
        case None =>
          logInfo("Started reading broadcast variable " + id)
          val startTimeMs = System.currentTimeMillis()
          // 需要调用readBlocks方法从Driver 或 Executor的存储体系中获取广播块。
          val blocks = readBlocks().flatMap(_.getChunks())
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

          val obj = TorrentBroadcast.unBlockifyObject[T](
            blocks, SparkEnv.get.serializer, compressionCodec)
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          val storageLevel = StorageLevel.MEMORY_AND_DISK
          if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
            throw new SparkException(s"Failed to store $broadcastId in BlockManager")
          }
          obj
      }
    }
  }

  /**
    * If running in a task, register the given block's locks for release upon task completion.
    * Otherwise, if not running in a task then immediately release the lock.
    */
  private def releaseLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener(_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  def blockifyObject[T: ClassTag](
                                   obj: T,
                                   blockSize: Int,
                                   serializer: Serializer,
                                   compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
                                     blocks: Array[ByteBuffer],
                                     serializer: Serializer,
                                     compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(
      blocks.iterator.map(new ByteBufferInputStream(_)).asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
    * Remove all persisted blocks associated with this torrent broadcast on the executors.
    * If removeFromDriver is true, also remove these persisted blocks on the driver.
    */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
