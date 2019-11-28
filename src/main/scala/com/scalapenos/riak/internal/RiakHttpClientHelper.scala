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

import java.util.zip.ZipException

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.scaladsl.{ Flow, Sink }
import akka.util.ByteString

import scala.util.Try

private[riak] object RiakHttpClientHelper {
  import akka.http.scaladsl.model.HttpCharsets
  import akka.http.scaladsl.marshalling._

  /**
   * Spray Marshaller for turning RiakValue instances into HttpEntity instances so they can be sent to Riak.
   */
  implicit val RiakValueMarshaller: ToEntityMarshaller[RiakValue] = {
    Marshaller.apply(implicit ec => riakValue => {
      val charset = riakValue.contentType.charsetOption.map(_.nioCharset).getOrElse(HttpCharsets.`UTF-8`.nioCharset)
      Marshaller.ByteArrayMarshaller(riakValue.data.getBytes(charset))
    })
  }
}

private[riak] class RiakHttpClientHelper(system: ActorSystem) extends RiakUriSupport with RequestBuilding with RiakIndexSupport with DateTimeSupport {
  import scala.concurrent.Future
  import scala.concurrent.Future._

  import akka.http.scaladsl.model.{ HttpEntity, HttpHeader, HttpRequest, HttpResponse }
  import akka.http.scaladsl.model.StatusCodes._
  import akka.http.scaladsl.model.headers._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import akka.http.scaladsl.coding.Gzip

  import HttpClientExtras._
  import RiakHttpHeaders._
  import RiakHttpClientHelper._

  import system.dispatcher

  private implicit val sys: ActorSystem = system
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private val http = Http()
  private val settings = RiakClientExtension(system).settings

  // ==========================================================================
  // Main HTTP Request Implementations
  // ==========================================================================

  def ping(server: RiakServerInfo): Future[Boolean] = {
    httpRequest(Get(PingUri(server))).mapResponse { response =>
      response.status match {
        case OK    => true
        case other => throw OperationFailed(s"Ping on server '$server' produced an unexpected response code '$other'.")
      }
    }
  }

  def fetch(server: RiakServerInfo, bucket: String, key: String, resolver: RiakConflictsResolver): Future[Option[RiakValue]] = {
    httpRequest(Get(KeyUri(server, bucket, key))).flatMapResponse { response =>
      response.status match {
        case OK              => toRiakValue(response)
        case NotFound        => successful(None)
        case MultipleChoices => resolveConflict(server, bucket, key, response, resolver).map(Some(_))
        case other           => throw BucketOperationFailed(s"Fetch for key '$key' in bucket '$bucket' produced an unexpected response code '$other'.")
        // TODO: case NotModified => successful(None)
      }
    }
  }

  def fetchWithSiblings(server: RiakServerInfo, bucket: String, key: String, resolver: RiakConflictsResolver): Future[Option[Set[RiakValue]]] = {
    httpRequest(Get(KeyUri(server, bucket, key))).flatMapResponse { response =>
      response.status match {
        case OK              => toRiakValue(response).map(_.map(Set(_)))
        case NotFound        => successful(None)
        case MultipleChoices => toRiakSiblingValues(response).map(Option(_))
        case other           => throw BucketOperationFailed(s"Fetch for key '$key' in bucket '$bucket' produced an unexpected response code '$other'.")
      }
    }
  }

  def fetch(server: RiakServerInfo, bucket: String, index: RiakIndex, resolver: RiakConflictsResolver): Future[List[RiakValue]] = {
    httpRequest(Get(IndexUri(server, bucket, index))).flatMapResponse { response =>
      response.status match {
        case OK         => fetchWithKeysReturnedByIndexLookup(server, bucket, response, resolver)
        case BadRequest => throw ParametersInvalid(s"""Invalid index name ("${index.fullName}") or value ("${index.value}").""")
        case other      => throw BucketOperationFailed(s"""Fetch for index "${index.fullName}" with value "${index.value}" in bucket "$bucket" produced an unexpected response code: $other.""")
      }
    }
  }

  def fetch(server: RiakServerInfo, bucket: String, indexRange: RiakIndexRange, resolver: RiakConflictsResolver): Future[List[RiakValue]] = {
    httpRequest(Get(IndexRangeUri(server, bucket, indexRange))).flatMapResponse { response =>
      response.status match {
        case OK         => fetchWithKeysReturnedByIndexLookup(server, bucket, response, resolver)
        case BadRequest => throw ParametersInvalid(s"""Invalid index name ("${indexRange.fullName}") or range ("${indexRange.start}" to "${indexRange.start}").""")
        case other      => throw BucketOperationFailed(s"""Fetch for index "${indexRange.fullName}" with range "${indexRange.start}" to "${indexRange.start}" in bucket "$bucket" produced an unexpected response code: $other.""")
      }
    }
  }

  def store(server: RiakServerInfo, bucket: String, key: String, value: RiakValue, resolver: RiakConflictsResolver): Future[Unit] = {
    val request = createStoreHttpRequest(value)(_)

    request(Put(KeyUri(server, bucket, key), value)).flatMapResponse { response =>
      response.status match {
        case NoContent => successful(())
        case other     => throw BucketOperationFailed(s"Store of value '$value' for key '$key' in bucket '$bucket' produced an unexpected response code '$other'.")
        // TODO: case PreconditionFailed => ... // needed when we support conditional request semantics
      }
    }
  }

  def storeAndFetch(server: RiakServerInfo, bucket: String, key: String, value: RiakValue, resolver: RiakConflictsResolver): Future[RiakValue] = {
    val request = createStoreHttpRequest(value)(_)

    request(Put(KeyUri(server, bucket, key, StoreQueryParameters(true)), value)).flatMapResponse { response =>
      response.status match {
        case OK              => toRiakValue(response).map(_.getOrElse(throw BucketOperationFailed(s"Store of value '$value' for key '$key' in bucket '$bucket' produced an unparsable reponse.")))
        case MultipleChoices => resolveConflict(server, bucket, key, response, resolver)
        case other           => throw BucketOperationFailed(s"Store of value '$value' for key '$key' in bucket '$bucket' produced an unexpected response code '$other'.")
        // TODO: case PreconditionFailed => ... // needed when we support conditional request semantics
      }
    }
  }

  def delete(server: RiakServerInfo, bucket: String, key: String): Future[Unit] = {
    httpRequest(Delete(KeyUri(server, bucket, key))).mapResponse { response =>
      response.status match {
        case NoContent => ()
        case NotFound  => ()
        case other     => throw BucketOperationFailed(s"Delete for key '$key' in bucket '$bucket' produced an unexpected response code '$other'.")
      }
    }
  }

  def getBucketProperties(server: RiakServerInfo, bucket: String): Future[RiakBucketProperties] = {
    import akka.http.scaladsl.unmarshalling._

    httpRequest(Get(PropertiesUri(server, bucket))).flatMapResponse { response =>
      response.status match {
        case OK => Unmarshal(response.entity).to[RiakBucketProperties].recover {
          case _ => throw BucketOperationFailed(s"Fetching properties of bucket '$bucket' failed because the response entity could not be parsed.")
        }
        case other => throw BucketOperationFailed(s"Fetching properties of bucket '$bucket' produced an unexpected response code '$other'.")
      }
    }
  }

  def setBucketProperties(server: RiakServerInfo, bucket: String, newProperties: Set[RiakBucketProperty[_]]): Future[Unit] = {
    import spray.json._

    val entity = JsObject("props" -> JsObject(newProperties.map(property => property.name -> property.json).toMap))

    // *Warning*: for some reason, Riak set bucket props HTTP endpoint doesn't handle compressed request properly.
    // Do not try to enable it here. Issue for tracking: https://github.com/agemooij/riak-scala-client/issues/41
    httpRequest(Put(PropertiesUri(server, bucket), entity)).mapResponse { response =>
      response.status match {
        case NoContent            => ()
        case BadRequest           => throw ParametersInvalid(s"Setting properties of bucket '$bucket' failed because the http request contained invalid data.")
        case UnsupportedMediaType => throw BucketOperationFailed(s"Setting properties of bucket '$bucket' failed because the content type of the http request was not 'application/json'.")
        case other                => throw BucketOperationFailed(s"Setting properties of bucket '$bucket' produced an unexpected response code '$other'.")
      }
    }
  }

  // ==========================================================================
  // Unsafe bucket operations
  // ==========================================================================

  def allKeys(server: RiakServerInfo, bucket: String): Future[RiakKeys] = {
    import akka.http.scaladsl.unmarshalling._

    httpRequest(Get(AllKeysUri(server, bucket))).flatMapResponse { response =>
      response.status match {
        case OK => Unmarshal(response.entity).to[RiakKeys].recover {
          case _ => throw BucketOperationFailed(s"List keys of bucket '$bucket' failed because the response entity could not be parsed.")
        }
        case other => throw BucketOperationFailed(s"""List keys of bucket "$bucket" produced an unexpected response code: $other.""")
      }
    }
  }

  // ==========================================================================
  // Request building
  // ==========================================================================

  private lazy val clientId = java.util.UUID.randomUUID().toString
  private val clientIdHeader = if (settings.AddClientIdHeader) Some(RawHeader(`X-Riak-ClientId`, clientId)) else None

  /**
   * Tries to decode gzipped response payload if response has an appropriate `Content-Encoding` header.
   * Returns the payload 'as is' if Gzip decoder throws a [[ZipException]].
   */
  private def safeDecodeGzip(response: HttpResponse): HttpResponse = {
    val entity = response.entity
    // TODO check if Try is still necessary ??? "Not in GZIP format"
    if (response.status.allowsEntity) {
      Try(response.withEntity(entity.transformDataBytes(Gzip.decoderFlow))).recover {
        case _: ZipException => response
      }.get
    } else response
  }

  private def basePipeline(request: HttpRequest, enableCompression: Boolean): Future[HttpResponse] = {
    if (enableCompression) {
      // Note that we don't compress request payload in here (e.g. using `encode(Gzip)` transformer).
      // This is due to a number of known shortcomings of Riak in regards to handling gzipped requests.
      http.singleRequest(request.addHeader(`Accept-Encoding`(Gzip.encoding))).map(safeDecodeGzip)
    } else {
      // So one might argue why would you need even to decode if you haven't asked for a gzip response via `Accept-Encoding` header? (the enableCompression=false case).
      // Well, there is a surprise from Riak: it will respond with gzip anyway if previous `store value` request was performed with `Content-Encoding: gzip` header! o_O
      // Yes, it's that weird...
      // And adding `addHeader(`Accept-Encoding`(NoEncoding.encoding))` directive for request will break it: Riak might respond with '406 Not Acceptable'
      // Issue for tracking: https://github.com/agemooij/riak-scala-client/issues/42
      http.singleRequest(request) // add safeDecodeGzip
    }
  }

  private def httpRequest(request: HttpRequest): Future[HttpResponse] = {
    val requestWithHeaders = request
      .addOptionalHeader(clientIdHeader)
      .addRawHeader("Accept", "*/*, multipart/mixed")

    basePipeline(requestWithHeaders, settings.EnableHttpCompression)
  }

  private def createStoreHttpRequest(value: RiakValue)(request: HttpRequest): Future[HttpResponse] = {
    val vclockHeader = value.vclock.toOption.map(vclock => RawHeader(`X-Riak-Vclock`, vclock))
    val indexHeaders = value.indexes.map(toIndexHeader).toList

    val requestWithHeaders = request
      .addOptionalHeader(vclockHeader)
      .mapHeaders(_ ++ indexHeaders)

    httpRequest(requestWithHeaders)
  }

  // ==========================================================================
  // Response => RiakValue
  // ==========================================================================

  private def toRiakValue(response: HttpResponse): Future[Option[RiakValue]] = toRiakValue(response.entity, response.headers)
  private def toRiakValue(entity: HttpEntity, headers: Seq[HttpHeader]): Future[Option[RiakValue]] = {
    entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map { data =>
      val vClockOption = headers.find(_.is(`X-Riak-Vclock`.toLowerCase)).map(_.value)
      val eTagOption = headers.find(_.is("etag")).map(_.value)
      val lastModifiedOption = headers.find(_.is("last-modified")).map(h => dateTimeFromLastModified(h.asInstanceOf[`Last-Modified`]))
      val indexes = toRiakIndexes(headers)

      for (vClock ← vClockOption; eTag ← eTagOption; lastModified ← lastModifiedOption)
        yield RiakValue(data.utf8String, entity.contentType, vClock, eTag, lastModified, indexes)
    }
  }

  private def dateTimeFromLastModified(lm: `Last-Modified`): DateTime = fromSprayDateTime(lm.date)

  // ==========================================================================
  // Index result fetching
  // ==========================================================================

  private def fetchWithKeysReturnedByIndexLookup(server: RiakServerInfo, bucket: String, response: HttpResponse, resolver: RiakConflictsResolver): Future[List[RiakValue]] = {
    response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).flatMap { data =>
      import spray.json._
      val keys = data.utf8String.parseJson.convertTo[RiakIndexQueryResponse].keys
      traverse(keys)(fetch(server, bucket, _, resolver)).map(_.flatten)
    }
  }

  // ==========================================================================
  // Conflict Resolution
  // ==========================================================================

  import akka.http.scaladsl.model._

  private def resolveConflict(server: RiakServerInfo, bucket: String, key: String, response: HttpResponse, resolver: RiakConflictsResolver): Future[RiakValue] = {
    import akka.http.scaladsl.unmarshalling._
    import FixedMultipartContentUnmarshalling._

    implicit val FixedMultipartContentUnmarshaller: FromEntityUnmarshaller[Multipart.General] =
      // we always pass a Gzip decoder. Just in case if Riak decides to respond with gzip suddenly. o_O
      // Issue for tracking: https://github.com/agemooij/riak-scala-client/issues/42
      multipartContentUnmarshaller(HttpCharsets.`UTF-8`, decoder = Gzip)

    val vclockHeader = response.headers.find(_.is(`X-Riak-Vclock`.toLowerCase)).toList

    Unmarshal(response.entity).to[Multipart.General].flatMap { multipartContent =>
      val bodyParts =
        if (settings.IgnoreTombstones)
          multipartContent.parts.filterNot(part => part.headers.exists(_.lowercaseName == `X-Riak-Deleted`.toLowerCase))
        else
          multipartContent.parts

      val conflictSource = bodyParts.foldAsync(Set[RiakValue]()) {
        case (values, part) => toRiakValue(part.entity, vclockHeader ++ part.headers).map(r => r.toSet ++ values)
      }

      val resolution = Flow[Set[RiakValue]].mapAsync(1) { values =>
        // Store the resolved value back to Riak and return the resulting RiakValue
        val ConflictResolution(result, writeBack) = resolver.resolve(values)
        if (writeBack) {
          storeAndFetch(server, bucket, key, result, resolver)
        } else {
          successful(result)
        }
      }

      conflictSource.via(resolution).runWith(Sink.head[RiakValue])
    }.recover {
      case notImplementedException: ConflicResolutionNotImplemented => throw notImplementedException
      case t                                                        => throw ConflictResolutionFailed(t.toString)
    }
  }

  private def toRiakSiblingValues(response: HttpResponse): Future[Set[RiakValue]] = {
    import akka.http.scaladsl.model._
    import akka.http.scaladsl.unmarshalling._
    import FixedMultipartContentUnmarshalling._

    implicit val FixedMultipartContentUnmarshaller: FromEntityUnmarshaller[Multipart.General] =
      // we always pass a Gzip decoder. Just in case if Riak decides to respond with gzip suddenly. o_O
      // Issue for tracking: https://github.com/agemooij/riak-scala-client/issues/42
      multipartContentUnmarshaller(HttpCharsets.`UTF-8`, decoder = Gzip)

    val vclockHeader = response.headers.find(_.is(`X-Riak-Vclock`.toLowerCase)).toList

    Unmarshal(response.entity).to[Multipart.General].flatMap { multipartContent =>
      val bodyParts =
        if (settings.IgnoreTombstones)
          multipartContent.parts.filterNot(part => part.headers.exists(_.lowercaseName == `X-Riak-Deleted`.toLowerCase))
        else
          multipartContent.parts

      bodyParts.foldAsync(Set[RiakValue]()) {
        case (values, part) => toRiakValue(part.entity, vclockHeader ++ part.headers).map(r => r.toSet ++ values)
      }.runWith(Sink.head[Set[RiakValue]])
    }.recover {
      case t => throw BucketOperationFailed(s"Failed to parse the server response as multipart content due to: '${t.toString}'")
    }
  }

  implicit class ResponseDiscardingMapper(responseF: Future[HttpResponse]) {
    def mapResponse[T](f: HttpResponse => T): Future[T] = {
      responseF.map { response =>
        val result = f(response)
        if (!response.entity.isStrict) response.discardEntityBytes()
        result
      }
    }

    def flatMapResponse[T](f: HttpResponse => Future[T]): Future[T] = {
      responseF.flatMap { response =>
        val result = f(response).transform(
          success => {
            if (!response.entity.isStrict) response.discardEntityBytes()
            success
          },
          t => {
            if (!response.entity.isStrict) response.discardEntityBytes()
            t
          }
        )
        result
      }
    }
  }
}

/**
 * Fix to work around the fact that Riak produces illegal Http content by not quoting ETag
 * headers in multipart/mixed responses. This breaks the Spray header parser so the below
 * class reproduces the minimal parts of Spray that are needed to provide a custom
 * unmarshaller for multipart/mixed responses.
 */
private[internal] object FixedMultipartContentUnmarshalling {
  import scala.concurrent.{ ExecutionContext, Future }
  import akka.http.scaladsl.coding._
  import akka.http.scaladsl.unmarshalling._
  import MultipartUnmarshallers._
  import akka.http.scaladsl.model._
  import MediaTypes._
  import headers._

  def multipartContentUnmarshaller(defaultCharset: HttpCharset, decoder: Decoder)(implicit executionContext: ExecutionContext, mat: Materializer): FromEntityUnmarshaller[Multipart.General] = {

    def fixHeaders(headers: List[HttpHeader]) = {
      val fixedHeaders = headers.map { header =>
        // from spray.util
        def startsWith(str: String, char: Char) = str.nonEmpty && str.charAt(0) == char

        if (header.is("etag") && !startsWith(header.value, '"')) RawHeader(header.name, "\"" + header.value + "\"")
        else header
      }
      fixedHeaders
    }

    multipartUnmarshaller[Multipart.General, Multipart.General.BodyPart, Multipart.General.BodyPart.Strict](
      mediaRange = `multipart/mixed`,
      defaultContentType = MediaTypes.`text/plain` withCharset defaultCharset,
      createBodyPart = {
        case (bodyPartEntity, headers) =>
          val fixedHeaders: List[HttpHeader] = fixHeaders(headers)

          val dataSource = bodyPartEntity.dataBytes.fold(ByteString.empty)(_ ++ _).mapAsync(1) { bs =>
            if (checkHeaderForDecoding(fixedHeaders, decoder)) {
              decoder.decode(bs)
            } else Future.successful(bs)
          }

          Multipart.General.BodyPart(
            // from akka-http documentation:
            // In a Multipart.Bodypart use IndefiniteLength for content of unknown length.
            HttpEntity.IndefiniteLength.apply(bodyPartEntity.contentType, dataSource), fixedHeaders
          )
      },
      createStreamed = Multipart.General(_, _),
      createStrictBodyPart = {
        case (strictEntity, headers) =>
          val fixedHeaders = fixHeaders(headers)

          // TODO decode ???
          //          val data =
          //            if (checkHeaderForDecoding(fixedHeaders, decoder)) {
          //              decoder.decodeData(strictEntity.data)
          //            } else strictEntity.data

          Multipart.General.BodyPart.Strict(
            HttpEntity.Strict(strictEntity.contentType, strictEntity.data), fixedHeaders
          )
      },
      createStrict = Multipart.General.Strict
    )
  }

  private def checkHeaderForDecoding(headers: List[HttpHeader], decoder: Decoder): Boolean = {
    // According to RFC (https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html),
    // "If multiple encodings have been applied to an entity, the content codings MUST be listed in the order in which
    // they were applied. Additional information about the encoding parameters MAY be provided by other entity-header
    // fields not defined by this specification."
    // This means that, if there were multiple encodings applied, this will NOT work.

    headers.exists(header => header.is("content-encoding") && header.value().contains(decoder.encoding.value))
    // { case `Content-Encoding`(encs) => encs.contains(decoder.encoding) }
  }
}
