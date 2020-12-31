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
package org.janusgraph.graphdb.transaction;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.ThresholdBasedIndexSelectionStrategy;
import org.junit.jupiter.api.Test;

import org.janusgraph.core.RelationType;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.idmanagement.IDManager;

public class StandardJanusGraphTxTest {

    @Test
    public void testGetOrCreatePropertyKey() {
        StandardJanusGraphTx tx = createTxWithMockedInternals();
        tx.getOrCreatePropertyKey("Foo", "Bar");
        Exception e = null;
        try {
            tx.getOrCreatePropertyKey("Baz", "Quuz");
        } catch (IllegalArgumentException ex) {
            e = ex;
        }
        tx.getOrCreatePropertyKey("Qux", "Quux");
        assertNotNull(e, "getOrCreatePropertyKey should throw an Exception when the relationType is not a PropertyKey");
        //verifyAll();
    }


    private StandardJanusGraphTx createTxWithMockedInternals() {
        StandardJanusGraph mockGraph = mock(StandardJanusGraph.class);
        TransactionConfiguration txConfig = mock(TransactionConfiguration.class);
        GraphDatabaseConfiguration gdbConfig = mock(GraphDatabaseConfiguration.class);
        TimestampProvider tsProvider = mock(TimestampProvider.class);
        Serializer mockSerializer = mock(Serializer.class);
        EdgeSerializer mockEdgeSerializer = mock(EdgeSerializer.class);
        IndexSerializer mockIndexSerializer = mock(IndexSerializer.class);
        RelationType relationType = mock(RelationType.class);
        IDManager idManager = mock(IDManager.class);
        PropertyKey propertyKey = mock(PropertyKey.class);
        DefaultSchemaMaker defaultSchemaMaker = mock(DefaultSchemaMaker.class);
        IndexSelectionStrategy indexSelectionStrategy = mock(ThresholdBasedIndexSelectionStrategy.class);

        when(mockGraph.getConfiguration()).thenReturn(gdbConfig);
        when(mockGraph.isOpen()).thenReturn(true);
        when(mockGraph.getDataSerializer()).thenReturn(mockSerializer);
        when(mockGraph.getEdgeSerializer()).thenReturn(mockEdgeSerializer);
        when(mockGraph.getIndexSerializer()).thenReturn(mockIndexSerializer);
        when(mockGraph.getIDManager()).thenReturn(idManager);
        when(mockGraph.getIndexSelector()).thenReturn(indexSelectionStrategy);

        when(gdbConfig.getTimestampProvider()).thenReturn(tsProvider);

        when(txConfig.isSingleThreaded()).thenReturn(true);
        when(txConfig.hasPreloadedData()).thenReturn(false);
        when(txConfig.hasVerifyExternalVertexExistence()).thenReturn(false);
        when(txConfig.hasVerifyInternalVertexExistence()).thenReturn(false);
        when(txConfig.getVertexCacheSize()).thenReturn(6);
        when(txConfig.isReadOnly()).thenReturn(true);
        when(txConfig.getDirtyVertexSize()).thenReturn(2);
        when(txConfig.getIndexCacheWeight()).thenReturn(2L);
        when(txConfig.getGroupName()).thenReturn(null);
        when(txConfig.getAutoSchemaMaker()).thenReturn(defaultSchemaMaker);

        when(defaultSchemaMaker.makePropertyKey(isA(PropertyKeyMaker.class), notNull())).thenReturn(propertyKey);

        when(relationType.isPropertyKey()).thenReturn(false);

        when(propertyKey.isPropertyKey()).thenReturn(true);

        //replayAll();

        StandardJanusGraphTx partialMock = spy( new StandardJanusGraphTx(mockGraph, txConfig));

        doReturn(null).when(partialMock).getRelationType("Foo");
        doReturn(propertyKey).when(partialMock).getRelationType("Qux");
        doReturn(relationType).when(partialMock).getRelationType("Baz");

        return partialMock;

    }
}
