/*
 * Copyright (C) 2012-2013 Age Mooij (http://scalapenos.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalapenos.riak
package internal

import scala.concurrent.Future

private[riak] sealed class RiakHttpBucket(helper: RiakHttpClientHelper, server: RiakServerInfo, val name: String, val resolver: RiakConflictsResolver) extends RiakBucket {
  def fetch(key: String): Future[Option[RiakValue]] = helper.fetch(server, name, key, resolver)
  def fetchWithSiblings(key: String): Future[Option[Set[RiakValue]]] = helper.fetchWithSiblings(server, name, key, resolver)
  def fetch(index: RiakIndex): Future[List[RiakValue]] = helper.fetch(server, name, index, resolver)
  def fetch(indexRange: RiakIndexRange): Future[List[RiakValue]] = helper.fetch(server, name, indexRange, resolver)

  def store(key: String, value: RiakValue): Future[Unit] = helper.store(server, name, key, value, resolver)
  def storeAndFetch(key: String, value: RiakValue): Future[RiakValue] = helper.storeAndFetch(server, name, key, value, resolver)

  def delete(key: String): Future[Unit] = helper.delete(server, name, key)

  def properties: Future[RiakBucketProperties] = helper.getBucketProperties(server, name)
  def properties_=(newProperties: Set[RiakBucketProperty[_]]): Future[Unit] = helper.setBucketProperties(server, name, newProperties)

  lazy val unsafe = new UnsafeRiakHttpBucket(helper, server, name, resolver)
}

private[riak] final class UnsafeRiakHttpBucket(helper: RiakHttpClientHelper, server: RiakServerInfo, name: String, resolver: RiakConflictsResolver) extends RiakHttpBucket(helper, server, name, resolver) with UnsafeBucketOperations {
  def allKeys(): Future[RiakKeys] = helper.allKeys(server, name)
}
