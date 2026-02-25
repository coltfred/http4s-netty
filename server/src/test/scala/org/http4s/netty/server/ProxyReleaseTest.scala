/*
 * Copyright 2020 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.netty.server

import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.implicits._
import fs2.Stream
import munit.catseffect.IOFixture
import org.http4s.HttpRoutes
import org.http4s.Request
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.jdkhttpclient.JdkHttpClient
import org.http4s.server.Server

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.Socket
import java.net.http.HttpClient
import scala.concurrent.duration._

/** Tests that emulate the proxy pattern:
  *
  * client.run(backendReq).allocated -> (response, release) resp.copy(body =
  * resp.body.onFinalize(release))
  *
  * This pattern relies on body stream finalizers firing after the Netty server finishes writing the
  * response. If they don't fire, upstream client connections leak.
  *
  * The critical scenario is when the downstream client closes the connection WITHOUT reading the
  * response body. The server must still finalize the body stream so that the upstream connection
  * (proxy -> backend) is released back to the pool.
  */
class ProxyReleaseTest extends IOSuite {
  // Composed resource: backend server -> proxy client -> proxy server + release counter
  val setup: IOFixture[(Server, Ref[IO, Int])] = resourceFixture(
    for {
      releaseCount <- Resource.eval(Ref[IO].of(0))

      // Backend server serving actual response bodies
      backend <- NettyServerBuilder[IO]
        .withHttpApp(
          HttpRoutes
            .of[IO] {
              case GET -> Root / "data" =>
                Ok(Stream.emits("hello from backend".getBytes).covary[IO])
              case GET -> Root / "large" =>
                // ~500KB response body — large enough that it won't fit in a single TCP segment
                Ok(Stream.emit("x" * 1024).repeat.covary[IO].take(500))
            }
            .orNotFound
        )
        .withNioTransport
        .withoutBanner
        .bindAny()
        .resource

      // Client used by the proxy to reach the backend (simulates ember-client in production)
      proxyClient <- Resource.pure(JdkHttpClient[IO](HttpClient.newHttpClient()))

      // Proxy server: uses client.run().allocated + onFinalize(release)
      proxy <- NettyServerBuilder[IO]
        .withHttpApp(
          HttpRoutes
            .of[IO] {
              case GET -> Root / "proxy" =>
                val backendUri = backend.baseUri / "data"
                proxyClient.run(Request[IO](uri = backendUri)).allocated.flatMap {
                  case (resp, release) =>
                    val tracked = release *> releaseCount.update(_ + 1)
                    IO.pure(resp.copy(body = resp.body.onFinalize(tracked)))
                }
              case GET -> Root / "proxy-large" =>
                val backendUri = backend.baseUri / "large"
                proxyClient.run(Request[IO](uri = backendUri)).allocated.flatMap {
                  case (resp, release) =>
                    val tracked = release *> releaseCount.update(_ + 1)
                    IO.pure(resp.copy(body = resp.body.onFinalize(tracked)))
                }
            }
            .orNotFound
        )
        .withNioTransport
        .withoutBanner
        .bindAny()
        .resource
    } yield (proxy, releaseCount),
    "setup"
  )

  val client: IOFixture[Client[IO]] =
    resourceFixture(Resource.pure(JdkHttpClient[IO](HttpClient.newHttpClient())), "client")

  test("proxy releases backend connection after response body is consumed") {
    val (proxy, releaseCount) = setup()
    val uri = proxy.baseUri / "proxy"

    releaseCount.set(0) *>
      client().expect[String](uri).map { body =>
        assertEquals(body, "hello from backend")
      } *>
      IO.sleep(500.millis) *>
      releaseCount.get.map { count =>
        assertEquals(count, 1, s"Expected 1 release but got $count")
      }
  }

  test("proxy releases backend connections across multiple sequential requests") {
    val (proxy, releaseCount) = setup()
    val uri = proxy.baseUri / "proxy"

    releaseCount.set(0) *>
      (1 to 5).toList.traverse_ { _ =>
        client().expect[String](uri).map { body =>
          assertEquals(body, "hello from backend")
        }
      } *>
      IO.sleep(500.millis) *>
      releaseCount.get.map { count =>
        assertEquals(count, 5, s"Expected 5 releases but got $count")
      }
  }

  /** Emulates multiple callers: GET a large response, read only the headers, then close the socket.
    * This is the scenario that causes connection leaks — the proxy starts streaming the body from
    * the backend but the downstream client disconnects before reading it.
    */
  test("proxy releases backend connection when client closes without reading body") {
    val (proxy, releaseCount) = setup()

    releaseCount.set(0) *>
      IO.blocking {
        val host = proxy.baseUri.host.get.renderString
        val port = proxy.baseUri.port.get

        // Open a raw TCP socket so we have full control over when it closes
        val socket = new Socket(host, port)
        try {
          socket.setSoTimeout(5000)
          val out = socket.getOutputStream
          val in = new BufferedReader(new InputStreamReader(socket.getInputStream))

          // Send HTTP request
          out.write(
            "GET /proxy-large HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".getBytes)
          out.flush()

          // Read only the status line to confirm the server started responding
          val statusLine = in.readLine()
          assert(
            statusLine != null && statusLine.contains("200"),
            s"Expected 200 but got: $statusLine")
        } finally
          // Close immediately without reading body — this is what the Go test does
          socket.close()
      } *>
      // Wait for the server to detect the closed connection and finalize the body stream
      IO.sleep(2.seconds) *>
      releaseCount.get.map { count =>
        assertEquals(count, 1, s"Expected 1 release but got $count — body finalizer did not fire")
      }
  }

  /** Like the above test, but do it repeatedly so we can be more sure it'll always happen
    */
  test("proxy releases all backend connections under repeated unconsumed-body requests") {
    val (proxy, releaseCount) = setup()

    val iterations = 100

    releaseCount.set(0) *>
      (1 to iterations).toList.parTraverse { _ =>
        IO.blocking {
          val host = proxy.baseUri.host.get.renderString
          val port = proxy.baseUri.port.get
          val socket = new Socket(host, port)
          try {
            socket.setSoTimeout(5000)
            val out = socket.getOutputStream
            val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
            out.write(
              "GET /proxy-large HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".getBytes)
            out.flush()
            val statusLine = in.readLine()
            assert(
              statusLine != null && statusLine.contains("200"),
              s"Expected 200 but got: $statusLine")
          } finally
            socket.close()
        }
      } *>
      // Ensure they can all finish
      IO.sleep(5.seconds) *>
      releaseCount.get.map { count =>
        assertEquals(
          count,
          iterations,
          s"Expected $iterations releases but got $count — body finalizers not firing on early close")
      }
  }
}
