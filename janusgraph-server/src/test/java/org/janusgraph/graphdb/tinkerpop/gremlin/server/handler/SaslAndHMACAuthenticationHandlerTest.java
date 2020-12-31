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

import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_AUTHENTICATOR;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.HMACAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.SaslAndHMACAuthenticator;
import org.junit.jupiter.api.Test;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;

public class SaslAndHMACAuthenticationHandlerTest {

    @Test
    public void testHttpChannelReadWhenAuthenticatorHasNotBeenAdded() throws Exception {
        final HMACAuthenticator hmacAuth = mock(HMACAuthenticator.class);
        final SaslAndHMACAuthenticator authenticator = mock(SaslAndHMACAuthenticator.class);
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final ChannelPipeline pipeline = mock(ChannelPipeline.class);
        final HttpMessage msg = mock(HttpMessage.class);
        final HttpHeaders headers = mock(HttpHeaders.class);

        when(authenticator.getHMACAuthenticator()).thenReturn(hmacAuth);
        when(authenticator.getSimpleAuthenticator()).thenReturn(mock(JanusGraphSimpleAuthenticator.class));
        when(ctx.pipeline()).thenReturn(pipeline);
        when(pipeline.get("hmac_authenticator")).thenReturn(null);
        when(pipeline.addAfter(eq(PIPELINE_AUTHENTICATOR), eq("hmac_authenticator"), isA(ChannelHandler.class))).thenReturn(null);
        when(msg.headers()).thenReturn(headers);
        when(headers.get(anyString())).thenReturn(null);
        when(ctx.fireChannelRead(eq(msg))).thenReturn(ctx);

        final SaslAndHMACAuthenticationHandler handler = new SaslAndHMACAuthenticationHandler(authenticator, null);
        handler.channelRead(ctx, msg);

        verify(ctx, times(2)).pipeline();
        verify(msg, times(2)).headers();
        verify(headers, times(2)).get(isA(String.class));
    }

    @Test
    public void testHttpChannelReadWhenAuthenticatorHasBeenAdded() throws Exception {
        final SaslAndHMACAuthenticator authenticator = mock(SaslAndHMACAuthenticator.class);
        final HMACAuthenticator hmacAuth = mock(HMACAuthenticator.class);
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final ChannelHandler mockHandler = mock(ChannelHandler.class);
        final ChannelPipeline pipeline = mock(ChannelPipeline.class);
        final HttpMessage msg = mock(HttpMessage.class);
        final HttpHeaders headers = mock(HttpHeaders.class);

        when(authenticator.getHMACAuthenticator()).thenReturn(hmacAuth);
        when(authenticator.getSimpleAuthenticator()).thenReturn(mock(JanusGraphSimpleAuthenticator.class));
        when(ctx.pipeline()).thenReturn(pipeline);
        when(pipeline.get("hmac_authenticator")).thenReturn(mockHandler);
        when(msg.headers()).thenReturn(headers);
        when(headers.get(anyString())).thenReturn(null);
        when(ctx.fireChannelRead(eq(msg))).thenReturn(ctx);

        final SaslAndHMACAuthenticationHandler handler = new SaslAndHMACAuthenticationHandler(authenticator, null);
        handler.channelRead(ctx, msg);

        verify(ctx, times(1)).pipeline();
        verify(pipeline, times(1)).get(anyString());
        verify(msg, times(2)).headers();
        verify(headers, times(2)).get(isA(String.class));
        verify(ctx, times(1)).fireChannelRead(any());
    }

}
