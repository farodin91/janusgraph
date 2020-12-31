// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop.gremlin.server.handler;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import java.util.Base64;
import java.util.Map;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.junit.jupiter.api.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

public class HttpHMACAuthenticationHandlerTest {

    @Test
    public void testChannelReadBasicAuthNoAuthHeader() {
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final FullHttpRequest msg = mock(FullHttpRequest.class);
        final HttpHeaders headers = mock(HttpHeaders.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final ChannelFuture cf = mock(ChannelFuture.class);

        when(msg.getMethod()).thenReturn(HttpMethod.POST);
        when(msg.headers()).thenReturn(headers);
        when(headers.get("Authorization")).thenReturn(null);
        when(ctx.writeAndFlush(eqHttpStatus(UNAUTHORIZED))).thenReturn(cf);
        when(cf.addListener(ChannelFutureListener.CLOSE)).thenReturn(null);
        when(msg.release()).thenReturn(false);

        HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);

        handler.channelRead(ctx, msg);

        verify(msg, times(1)).getMethod();
        verify(msg, times(1)).headers();
        verify(msg, times(1)).release();
    }

    @Test
    public void testChannelReadBasicAuthIncorrectScheme() {
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final FullHttpRequest msg = mock(FullHttpRequest.class);
        final HttpHeaders headers = mock(HttpHeaders.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final ChannelFuture cf = mock(ChannelFuture.class);

        when(msg.getMethod()).thenReturn(HttpMethod.POST);
        when(msg.headers()).thenReturn(headers);
        when(headers.get("Authorization")).thenReturn("bogus");
        when(ctx.writeAndFlush(eqHttpStatus(UNAUTHORIZED))).thenReturn(cf);
        when(cf.addListener(ChannelFutureListener.CLOSE)).thenReturn(null);
        when(msg.release()).thenReturn(false);

        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);

        handler.channelRead(ctx, msg);

        verify(msg, times(1)).getMethod();
        verify(msg, times(1)).headers();
        verify(msg, times(1)).release();
    }

    @Test
    public void testChannelReadBasicAuth() throws Exception {
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final FullHttpRequest msg = mock(FullHttpRequest.class);
        final HttpHeaders headers = mock(HttpHeaders.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final String encodedUserNameAndPass = Base64.getEncoder().encodeToString("user:pass".getBytes());
        when(msg.getMethod()).thenReturn(HttpMethod.POST);
        when(msg.headers()).thenReturn(headers);
        when(msg.getUri()).thenReturn("/");
        when(headers.get(eq("Authorization"))).thenReturn("Basic " + encodedUserNameAndPass);
        when(ctx.fireChannelRead(isA(FullHttpRequest.class))).thenReturn(ctx);
        when(authenticator.authenticate(anyMap())).thenReturn(new AuthenticatedUser("foo"));

        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);

        handler.channelRead(ctx, msg);

        verify(msg, times(1)).getMethod();
        verify(msg, times(1)).headers();
        verify(headers, times(1)).get(any());
        verify(ctx, times(1)).fireChannelRead(any());
        verify(authenticator, times(1)).authenticate(any());
    }

    @Test
    public void testChannelReadGetAuthToken() throws Exception {
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final FullHttpRequest msg = mock(FullHttpRequest.class);
        final HttpHeaders headers = mock(HttpHeaders.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final ChannelFuture cf = mock(ChannelFuture.class);
        final String encodedUserNameAndPass = Base64.getEncoder().encodeToString("user:pass".getBytes());
        when(msg.getMethod()).thenReturn(HttpMethod.GET);
        when(msg.headers()).thenReturn(headers);
        when(msg.getUri()).thenReturn("/session");
        when(headers.get("Authorization")).thenReturn("Basic " + encodedUserNameAndPass);
        when(authenticator.authenticate(anyMap())).thenReturn(new AuthenticatedUser("foo"));
        when(ctx.writeAndFlush(eqHttpStatus(OK))).thenReturn(cf);
        when(cf.addListener(ChannelFutureListener.CLOSE)).thenReturn(null);
        when(msg.release()).thenReturn(false);
        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);

        handler.channelRead(ctx, msg);

        verify(msg, times(1)).getMethod();
        verify(msg, times(1)).headers();
        verify(msg, times(1)).release();
        ArgumentCaptor<Map<String, String>> credMapCaptor = ArgumentCaptor.forClass(Map.class);

        verify(authenticator).authenticate(credMapCaptor.capture());

        assertNotNull(credMapCaptor.getValue().get(HttpHMACAuthenticationHandler.PROPERTY_GENERATE_TOKEN));
    }

    @Test
    public void testChannelReadTokenAuth() throws Exception {
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final FullHttpRequest msg = mock(FullHttpRequest.class);
        final HttpHeaders headers = mock(HttpHeaders.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final String encodedToken = Base64.getEncoder().encodeToString("askdjhf823asdlkfsasd".getBytes());
        when(msg.getMethod()).thenReturn(HttpMethod.GET);
        when(msg.headers()).thenReturn(headers);
        when(msg.getUri()).thenReturn("/");
        when(headers.get(eq("Authorization"))).thenReturn("Token " + encodedToken);
        when(ctx.fireChannelRead(isA(FullHttpRequest.class))).thenReturn(ctx);
        when(authenticator.authenticate(anyMap())).thenReturn(new AuthenticatedUser("foo"));
        final HttpHMACAuthenticationHandler handler = new HttpHMACAuthenticationHandler(authenticator);

        handler.channelRead(ctx, msg);

        verify(msg, times(1)).getMethod();
        verify(msg, times(1)).headers();
        verify(msg, times(1)).getUri();;
        verify(headers, times(1)).get(any());
        verify(ctx, times(1)).fireChannelRead(any());
        verify(authenticator, times(1)).authenticate(any());
    }


    private static DefaultFullHttpResponse eqHttpStatus(HttpResponseStatus status) {
        return argThat(new HttpResponseStatusMatcher(status));
    }

    public static class HttpResponseStatusMatcher implements ArgumentMatcher<DefaultFullHttpResponse> {

        private final HttpResponseStatus left;

        public HttpResponseStatusMatcher(HttpResponseStatus left) {
            this.left = left;
        }

        @Override
        public boolean matches(DefaultFullHttpResponse right) {
            return left.equals((right).getStatus());
        }
    }
}
