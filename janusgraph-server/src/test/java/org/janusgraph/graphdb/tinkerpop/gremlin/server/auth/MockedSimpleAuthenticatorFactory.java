// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.janusgraph.core.JanusGraph;

import java.util.Map;

import static org.mockito.Mockito.*;

public class MockedSimpleAuthenticatorFactory implements MockedJanusGraphAuthenticatorFactory {

    public JanusGraphAbstractAuthenticator createInitializedAuthenticator(final Map<String, Object> config,
                                                                          final JanusGraph graph) {
        final JanusGraphSimpleAuthenticator authenticator = spy(JanusGraphSimpleAuthenticator.class);
        final SimpleAuthenticator sa = mock(SimpleAuthenticator.class);
        doReturn(graph).when(authenticator).openGraph(anyString());
        doReturn(sa).when(authenticator).createSimpleAuthenticator();
        sa.setup(config);
        return authenticator;
    }
}
