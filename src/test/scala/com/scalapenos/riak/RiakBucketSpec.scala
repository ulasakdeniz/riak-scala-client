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

class RiakBucketSpec extends RiakClientSpecification with RandomKeySupport {
  private def randomBucket = client.bucket("riak-bucket-tests-" + randomKey)

  "A RiakBucket" should {
    "not be able to store an empty String value" in {
      val bucket = randomBucket
      val key = randomKey

      // Riak will reject the request with a 400 because the request will
      // not have a body (because Spray doesn't allow empty bodies).
      bucket.store(key, "").await must throwA[BucketOperationFailed]
    }

    "treat tombstone values as if they don't exist when allow_mult = false" in {
      val bucket = randomBucket
      val key = randomKey

      bucket.store(key, "value").await
      bucket.delete(key).await

      val fetched = bucket.fetch(key).await

      fetched should beNone
    }

    "treat tombstone values as if they don't exist when allow_mult = true" in {
      val bucket = randomBucket
      val key = randomKey

      (bucket.allowSiblings = true).await

      bucket.store(key, "value").await
      bucket.delete(key).await

      val fetched = bucket.fetch(key).await

      fetched should beNone
    }

    "list all keys" in {
      val bucket = randomBucket
      val numberOfKeys = 5
      val keys = (1 to numberOfKeys).map(_ ⇒ randomKey)

      keys.map { key ⇒
        bucket.store(key, "value").await
      }

      val allKeys = bucket.allKeys().await

      keys.map { key ⇒
        bucket.delete(key).await
      }

      allKeys.keys.size must beEqualTo(numberOfKeys)
      allKeys.keys.toSet must beEqualTo(keys.toSet)
    }

    "list all keys from an empty bucket" in {
      val bucket = randomBucket

      val allKeys = bucket.allKeys().await

      allKeys.keys should be(Nil)
    }
  }

}
