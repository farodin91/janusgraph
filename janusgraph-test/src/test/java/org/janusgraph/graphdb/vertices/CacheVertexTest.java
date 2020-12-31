// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.graphdb.vertices;

import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.datastructures.Retriever;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CacheVertexTest {

    @Test
    public void testLoadRelationsWithNullSuperSetValue() {
        final SliceQuery mockSliceQuery = mock(SliceQuery.class);
        final Retriever mockRetriever = mock(Retriever.class);

        when(mockSliceQuery.subsumes(any(SliceQuery.class))).thenReturn(true);
        when(mockRetriever.get(any(SliceQuery.class))).thenReturn(null);

        final CacheVertex cacheVertex = spy(new CacheVertex(mock(StandardJanusGraphTx.class), 0l, (byte) 0));

        doReturn(false).when(cacheVertex).isNew();

        cacheVertex.addToQueryCache(mockSliceQuery, null);
        cacheVertex.loadRelations(mock(SliceQuery.class), mockRetriever);

        verify(mockRetriever, times(1)).get(any());
    }

}
