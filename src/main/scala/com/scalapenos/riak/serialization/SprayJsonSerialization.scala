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
package serialization

trait SprayJsonSerialization {
  import scala.reflect._
  import scala.util._
  import spray.json._

  class SprayJsonSerializer[T: RootJsonWriter] extends RiakSerializer[T] {
    def serialize(t: T) = (implicitly[RootJsonWriter[T]].write(t).compactPrint, ContentTypes.`application/json`)
  }

  class SprayJsonDeserializer[T: RootJsonReader: ClassTag] extends RiakDeserializer[T] {
    def deserialize(data: String, contentType: ContentType) = {
      contentType match {
        case ContentType(MediaTypes.`application/octet-stream` | MediaTypes.`application/json`, _) ⇒ parseAndConvert(data)
        case _                                                                                     ⇒ throw RiakUnsupportedContentType(ContentTypes.`application/json`, contentType)
      }
    }

    private def parseAndConvert(data: String): T = {
      Try(implicitly[RootJsonReader[T]].read(JsonParser(data))) match {
        case Success(t)         ⇒ t
        case Failure(throwable) ⇒ throw RiakDeserializationFailed(data, classTag[T].runtimeClass.getName, throwable)
      }
    }
  }
}

object SprayJsonSerialization extends SprayJsonSerialization
