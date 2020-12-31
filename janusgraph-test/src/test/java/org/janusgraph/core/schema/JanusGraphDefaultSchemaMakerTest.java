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

package org.janusgraph.core.schema;

import org.junit.jupiter.api.Test;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.mockito.verification.VerificationMode;

import java.util.Date;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class JanusGraphDefaultSchemaMakerTest {

  @Test
  public void testMakePropertyKey() {
      PropertyKeyMaker pkm = mockPropertyKeyMaker();
      DefaultSchemaMaker schemaMaker = JanusGraphDefaultSchemaMaker.INSTANCE;
      byte b = 100;
      short s = 10000;
      schemaMaker.makePropertyKey(pkm, "Foo");
      schemaMaker.makePropertyKey(pkm, 'f');
      schemaMaker.makePropertyKey(pkm, true);
      schemaMaker.makePropertyKey(pkm, b);
      schemaMaker.makePropertyKey(pkm, s);
      schemaMaker.makePropertyKey(pkm, 100);
      schemaMaker.makePropertyKey(pkm, 100L);
      schemaMaker.makePropertyKey(pkm, 100.0f);
      schemaMaker.makePropertyKey(pkm, 1.23e2);
      schemaMaker.makePropertyKey(pkm, new Date());
      schemaMaker.makePropertyKey(pkm, Geoshape.point(42.3601f, 71.0589f));
      schemaMaker.makePropertyKey(pkm, UUID.randomUUID());
      schemaMaker.makePropertyKey(pkm, new Object());

      verify(pkm, atLeastOnce()).cardinalityIsSet();
  }

  private PropertyKeyMaker mockPropertyKeyMaker() {
      PropertyKeyMaker propertyKeyMaker = mock(PropertyKeyMaker.class);
      PropertyKey pk = mock(PropertyKey.class);
      when(propertyKeyMaker.make()).thenReturn(pk);
      when(propertyKeyMaker.getName()).thenReturn("Quux");
      when(propertyKeyMaker.cardinality(Cardinality.SINGLE)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.cardinalityIsSet()).thenReturn(false);
      when(propertyKeyMaker.dataType(String.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Character.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Boolean.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Byte.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Short.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Integer.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Long.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Float.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Double.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Date.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Geoshape.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(UUID.class)).thenReturn(propertyKeyMaker);
      when(propertyKeyMaker.dataType(Object.class)).thenReturn(propertyKeyMaker);
      return propertyKeyMaker;
  }

}
