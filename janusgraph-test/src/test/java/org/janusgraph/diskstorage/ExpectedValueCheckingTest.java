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

package org.janusgraph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerProvider;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.*;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.*;

/**
 * Test transaction handling in {@link ExpectedValueCheckingStore} and related
 * classes, particularly with respect to consistency levels offered by the
 * underlying store.
 */
@ExtendWith(MockitoExtension.class)
public class ExpectedValueCheckingTest {

    private ExpectedValueCheckingStoreManager expectManager;
    private KeyColumnValueStoreManager backingManager;
    private StoreTransaction consistentTx;
    private StoreTransaction inconsistentTx;
    private StoreTransaction expectTx;
    private Locker backingLocker;
    private KeyColumnValueStore backingStore;
    private KeyColumnValueStore expectStore;

    @Captor
    private ArgumentCaptor<BaseTransactionConfig> txConfigCapture;
    private InOrder inOrder;

    private static final String STORE_NAME = "ExpectTestStore";
    private static final String LOCK_SUFFIX = "_expecttest";
    private static final String LOCKER_NAME = STORE_NAME + LOCK_SUFFIX;

    private static final StaticBuffer DATA_KEY = BufferUtil.getIntBuffer(1);
    private static final StaticBuffer DATA_COL = BufferUtil.getIntBuffer(2);
    private static final StaticBuffer DATA_VAL = BufferUtil.getIntBuffer(4);

    private static final StaticBuffer LOCK_KEY = BufferUtil.getIntBuffer(32);
    private static final StaticBuffer LOCK_COL = BufferUtil.getIntBuffer(64);
    private static final StaticBuffer LOCK_VAL = BufferUtil.getIntBuffer(128);

    @BeforeEach
    public void setupMocks() throws BackendException {
        // Setup some config mocks and objects
        backingManager = mock(KeyColumnValueStoreManager.class);
        LockerProvider lockerProvider = mock(LockerProvider.class);
        ModifiableConfiguration globalConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        ModifiableConfiguration localConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        ModifiableConfiguration defaultConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        // Set some properties on the configs, just so that global/local/default can be easily distinguished
        globalConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "global");
        localConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "local");
        defaultConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "default");
        BaseTransactionConfig defaultTxConfig = new StandardBaseTransactionConfig.Builder().customOptions(defaultConfig).timestampProvider(TimestampProviders.MICRO).build();
        StoreFeatures backingFeatures = new StandardStoreFeatures.Builder().keyConsistent(globalConfig, localConfig).build();


        // Setup behavior specification starts below this line


        // 1. Construct manager
        // The EVCSManager ctor retrieves the backing store's features and stores it in an instance field
        doReturn(backingFeatures).when(backingManager).getFeatures();

        // 2. Begin transaction
        // EVCTx begins two transactions on the backingManager: one with globalConfig and one with localConfig
        // The capture is used in the @After method to check the config
        inconsistentTx = mock(StoreTransaction.class);
        consistentTx = mock(StoreTransaction.class);
        when(backingManager.beginTransaction(txConfigCapture.capture())).thenReturn(inconsistentTx).thenReturn(consistentTx);

        // 3. Open a database
        backingLocker = mock(Locker.class);
        backingStore = mock(KeyColumnValueStore.class);
        when(backingManager.openDatabase(STORE_NAME)).thenReturn(backingStore);
        when(backingStore.getName()).thenReturn(STORE_NAME);
        when(lockerProvider.getLocker(LOCKER_NAME)).thenReturn(backingLocker);

        // 1. Construct manager
        expectManager = new ExpectedValueCheckingStoreManager(backingManager, LOCK_SUFFIX, lockerProvider, Duration.ofSeconds(1L));
        // 2. Begin transaction
        expectTx = expectManager.beginTransaction(defaultTxConfig);
        // 3. Open a database

        expectStore = expectManager.openDatabase(STORE_NAME);

        inOrder = Mockito.inOrder(backingManager, inconsistentTx, consistentTx, backingStore, backingLocker, lockerProvider);
        then(backingManager).should(inOrder).getFeatures();
        then(backingManager).should(inOrder, times(2)).beginTransaction(any());
        then(backingManager).should(inOrder).openDatabase(STORE_NAME);
        then(backingStore).should(inOrder).getName();
        then(lockerProvider).should(inOrder).getLocker(LOCKER_NAME);

    }

    @AfterEach
    public void verifyMocks() {
        // Check capture created in the @Before method
        List<BaseTransactionConfig> transactionConfigurations = txConfigCapture.getAllValues();
        assertEquals(2, transactionConfigurations.size());
        // First backing store transaction should use default tx config
        assertEquals("default", transactionConfigurations.get(0).getCustomOption(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID));
        // Second backing store transaction should use global strong consistency config
        assertEquals("global", transactionConfigurations.get(1).getCustomOption(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID));
        // The order in which these transactions are opened isn't really significant;
        // testing them in order is kind of over-specifying the implementation's behavior.
        // Could probably relax the ordering selectively here with some thought, but
    }

    @Test
    public void testMutateWithLockUsesConsistentTx() throws BackendException {
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();
        final KeyColumn kc = new KeyColumn(LOCK_KEY, LOCK_COL);

        // 1. Acquire a lock
        backingLocker.writeLock(kc, consistentTx);

        // 2. Run a mutation
        // N.B. mutation coordinates do not overlap with the lock, but consistentTx should be used anyway
        // 2.1. Check locks & expected values before mutating data
        backingLocker.checkLocks(consistentTx);
        StaticBuffer nextBuf = BufferUtil.nextBiggerBuffer(kc.getColumn());
        KeySliceQuery expectedValueQuery = new KeySliceQuery(kc.getKey(), kc.getColumn(), nextBuf);
        doReturn(StaticArrayEntryList.of(StaticArrayEntry.of(LOCK_COL, LOCK_VAL))).when(backingStore).getSlice(expectedValueQuery, consistentTx);

        // 2.2. Mutate data
        backingStore.mutate(DATA_KEY, adds, deletions, consistentTx); // writes by txs with locks must use strong consistency

        // 1. Lock acquisition
        expectStore.acquireLock(LOCK_KEY, LOCK_COL, LOCK_VAL, expectTx);
        // 2. Mutate
        expectStore.mutate(DATA_KEY, adds, deletions, expectTx);
        then(backingStore).should(inOrder).getSlice(expectedValueQuery, consistentTx);
    }

    @Test
    public void testMutateWithoutLockUsesInconsistentTx() throws BackendException {
        // Run a mutation
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();
        backingStore.mutate(DATA_KEY, adds, deletions, inconsistentTx); // consistency level is unconstrained w/o locks

        expectStore.mutate(DATA_KEY, adds, deletions, expectTx);
    }

    @Test
    public void testMutateManyWithLockUsesConsistentTx() throws BackendException {
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();

        Map<String, Map<StaticBuffer, KCVMutation>> mutations =
            ImmutableMap.of(STORE_NAME,
                ImmutableMap.of(DATA_KEY, new KCVMutation(adds, deletions)));
        final KeyColumn kc = new KeyColumn(LOCK_KEY, LOCK_COL);

        // Acquire a lock
        backingLocker.writeLock(kc, consistentTx);

        // 2. Run mutateMany
        // 2.1. Check locks & expected values before mutating data
        backingLocker.checkLocks(consistentTx);
        StaticBuffer nextBuf = BufferUtil.nextBiggerBuffer(kc.getColumn());
        KeySliceQuery expectedValueQuery = new KeySliceQuery(kc.getKey(), kc.getColumn(), nextBuf);
        when(backingStore.getSlice(expectedValueQuery, consistentTx)) // expected value read must use strong consistency
            .thenReturn(StaticArrayEntryList.of(StaticArrayEntry.of(LOCK_COL, LOCK_VAL)));
        // 2.2. Run mutateMany on backing manager to modify data
        backingManager.mutateMany(mutations, consistentTx); // writes by txs with locks must use strong consistency

        // Lock acquisition
        expectStore.acquireLock(LOCK_KEY, LOCK_COL, LOCK_VAL, expectTx);
        // Mutate
        expectManager.mutateMany(mutations, expectTx);
        then(backingStore).should(inOrder).getSlice(expectedValueQuery, consistentTx);
    }

    @Test
    public void testMutateManyWithoutLockUsesInconsistentTx() throws BackendException {
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();

        Map<String, Map<StaticBuffer, KCVMutation>> mutations =
            ImmutableMap.of(STORE_NAME,
                ImmutableMap.of(DATA_KEY, new KCVMutation(adds, deletions)));

        // Run mutateMany
        backingManager.mutateMany(mutations, inconsistentTx); // consistency level is unconstrained w/o locks

        // Run mutateMany
        expectManager.mutateMany(mutations, expectTx);
    }
}
