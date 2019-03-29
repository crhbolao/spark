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

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging

/**
  * 广播管理器 BroadcastManager 初始化
  *
  * @param isDriver        是否是driver
  * @param conf            spark conf
  * @param securityManager 安全管理器
  */
private[spark] class BroadcastManager(
                                       val isDriver: Boolean,
                                       conf: SparkConf,
                                       securityManager: SecurityManager)
  extends Logging {
  /**
    * 用来表示 BroadcastManager 是否初始化完成
    */
  private var initialized = false
  /**
    * 广播工厂实例
    */
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  /**
    * 自身 BroadcastManager的初始化方法
    */
  private def initialize() {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  /**
    * 下一个广播对象的广播ID，类型为 AtomicLong.
    */
  private val nextBroadcastId = new AtomicLong(0)

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
