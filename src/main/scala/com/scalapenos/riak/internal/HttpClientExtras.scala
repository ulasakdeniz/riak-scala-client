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

private[riak] object HttpClientExtras {
  import akka.http.scaladsl.model.{ HttpHeader, HttpRequest }
  import akka.http.scaladsl.model.headers.RawHeader

  implicit class EnrichedHttpRequest(protected val httpRequest: HttpRequest) {
    def addOptionalHeader(header: => Option[HttpHeader]): HttpRequest = httpRequest.mapHeaders(headers => header.toList ++ headers)

    def addRawHeader(name: String, value: String): HttpRequest = httpRequest.addHeader(RawHeader(name, value))
  }
}
