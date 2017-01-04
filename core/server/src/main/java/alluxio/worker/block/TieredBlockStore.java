/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.collections.Pair;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.resource.LockResource;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.FileUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.evictor.ARCEvictor;
import alluxio.worker.block.evictor.BlockTransferInfo;
import alluxio.worker.block.evictor.EvictionPlan;
import alluxio.worker.block.evictor.Evictor;
import alluxio.worker.block.evictor.EvictorType;
import alluxio.worker.block.evictor.LIRSEvictor;
import alluxio.worker.block.evictor.LRFUEvictor;
import alluxio.worker.block.evictor.LRUEvictor;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.block.io.LocalFileBlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents an object store that manages all the blocks in the local tiered storage.
 * This store exposes simple public APIs to operate blocks. Inside this store, it creates an
 * Allocator to decide where to put a new block, an Evictor to decide where to evict a stale block,
 * a BlockMetadataManager to maintain the status of the tiered storage, and a LockManager to
 * coordinate read/write on the same block.
 * <p>
 * This class is thread-safe, using the following lock hierarchy to ensure thread-safety:
 * <ul>
 * <li>Any block-level operation (e.g., read, move or remove) on an existing block must acquire a
 * block lock for this block via {@link TieredBlockStore#mLockManager}. This block lock is a
 * read/write lock, guarding both the metadata operations and the following I/O on this block. It
 * coordinates different threads (clients) when accessing the same block concurrently.</li>
 * <li>Any metadata operation (read or write) must go through {@link TieredBlockStore#mMetaManager}
 * and guarded by {@link TieredBlockStore#mMetadataLock}. This is also a read/write lock and
 * coordinates different threads (clients) when accessing the shared data structure for metadata.
 * </li>
 * <li>Method {@link #createBlockMeta} does not acquire the block lock, because it only creates a
 * temp block which is only visible to its writer before committed (thus no concurrent access).</li>
 * <li>Eviction is done in {@link #freeSpaceInternal} and it is on the basis of best effort. For
 * operations that may trigger this eviction (e.g., move, create, requestSpace), retry is used</li>
 * </ul>
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public class TieredBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MAX_RETRIES =
      Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_RETRY);

  protected final BlockMetadataManager mMetaManager;
  protected final BlockLockManager mLockManager;
  protected final Allocator mAllocator;
  protected Evictor mEvictor;
  protected EvictorType mEvictorType;

  protected final List<BlockStoreEventListener> mBlockStoreEventListeners = new ArrayList<>();

  /** A set of pinned inodes fetched from the master. */
  private final Set<Long> mPinnedInodes = new HashSet<>();

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();

  /** ReadLock provided by {@link #mMetadataLock} to guard metadata read operations. */
  protected final Lock mMetadataReadLock = mMetadataLock.readLock();

  /** WriteLock provided by {@link #mMetadataLock} to guard metadata write operations. */
  protected final Lock mMetadataWriteLock = mMetadataLock.writeLock();

  /** Association between storage tier aliases and ordinals. */
  private final StorageTierAssoc mStorageTierAssoc;

  private final Map<EvictorType, TieredBlockStore> mSimulateBlockStore = new HashMap<>(4);

  private boolean mSimulate = false;

  protected Map<String, Long> mAliasReadBytes = new HashMap<>();
  protected Map<String, Long> mAliasWriteBytes = new HashMap<>();
  private final ExecutorService mEvictorCheck =
      Executors.newFixedThreadPool(1, ThreadFactoryUtils.build("Check Evictor", true));

  /**
   * Creates a new instance of {@link TieredBlockStore}.
   */
  public TieredBlockStore() {
    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
    mLockManager = new BlockLockManager();
    mEvictorType = Configuration.getEnum(PropertyKey.WORKER_EVICTOR_TYPE, EvictorType.class);
    createSimulateBlockStores();
    BlockMetadataManagerView initManagerView = new BlockMetadataManagerView(mMetaManager,
        Collections.<Long>emptySet(), Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.create(initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    mEvictor = Evictor.Factory.create(initManagerView, mAllocator);
    mSimulateBlockStore.put(mEvictorType, this);
    if (mEvictor instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mEvictor);
    }

    mStorageTierAssoc = new WorkerStorageTierAssoc();
    for (int i = 0; i < mStorageTierAssoc.size(); i++) {
      mAliasReadBytes.put(mStorageTierAssoc.getAlias(i), 0L);
      mAliasWriteBytes.put(mStorageTierAssoc.getAlias(i), 0L);
    }
    mEvictorCheck.submit(new EvictorCheckingThread());
  }

  /**
   * Create an instance of {@link TieredBlockStore}.
   *
   * @param metaManager block metadata manager
   * @param pinnedInodes set of pined nodes in memory
   * @param simulate true or false
   */
  public TieredBlockStore(BlockMetadataManager metaManager, Set<Long> pinnedInodes,
      boolean simulate) {
    mMetaManager = new BlockMetadataManager(metaManager);
    mLockManager = new BlockLockManager();
    mSimulate = simulate;
    BlockMetadataManagerView initManagerView =
        new BlockMetadataManagerView(mMetaManager, pinnedInodes, Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.create(initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }
    mStorageTierAssoc = new WorkerStorageTierAssoc();
    for (int i = 0; i < mStorageTierAssoc.size(); i++) {
      mAliasReadBytes.put(mStorageTierAssoc.getAlias(i), 0L);
      mAliasWriteBytes.put(mStorageTierAssoc.getAlias(i), 0L);
    }
  }

  /**
   * Create a {@link ShadowBlockStore} for each kind of evictor.
   */
  private void createSimulateBlockStores() {
    if (mEvictorType != EvictorType.LRU) {
      mSimulateBlockStore.put(EvictorType.LRU,
          new ShadowBlockStore(mMetaManager, Collections.<Long>emptySet(), mEvictorType));
    }
    if (mEvictorType != EvictorType.LRFU) {
      mSimulateBlockStore.put(EvictorType.LRFU,
          new ShadowBlockStore(mMetaManager, Collections.<Long>emptySet(), mEvictorType));
    }
    if (mEvictorType != EvictorType.LIRS) {
      mSimulateBlockStore.put(EvictorType.LIRS,
          new ShadowBlockStore(mMetaManager, Collections.<Long>emptySet(), mEvictorType));
    }
    if (mEvictorType != EvictorType.ARC) {
      mSimulateBlockStore.put(EvictorType.ARC,
          new ShadowBlockStore(mMetaManager, Collections.<Long>emptySet(), mEvictorType));
    }
  }

  /**
   * Create evictor according to type of evictor.
   *
   * @param view instance of {@link BlockMetadataManagerView}
   * @param allocator instance of {@link Allocator}
   * @param type type of evictor
   * @return an evictor
   */
  public Evictor createEvictor(BlockMetadataManagerView view, Allocator allocator,
      EvictorType type) {
    switch (type) {
      case LRU:
        return new LRUEvictor(view, allocator);
      case LRFU:
        return new LRFUEvictor(view, allocator);
      case LIRS:
        return new LIRSEvictor(view, allocator);
      case ARC:
        return new ARCEvictor(view, allocator);
      default:
        return null;
    }
  }

  /**
   * Reset {@link ShadowBlockStore} and {@link Evictor} with a fixed interval.
   */
  public void resetEvictor() {
    long readBytes = 0L;
    for (int i = 0; i < mStorageTierAssoc.size(); i++) {
      readBytes += mAliasReadBytes.get(mStorageTierAssoc.getAlias(i));
    }
    // Don't reset evictor if the total read bytes smaller than twice of memory capacity.
    if (readBytes < mMetaManager.getTier(mStorageTierAssoc.getAlias(0)).getCapacityBytes() * 2) {
      return;
    }
    EvictorType candidateEvictorType = null;
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      boolean equal = true;
      for (int i = mStorageTierAssoc.size() - 1; i >= 0; i--) {
        long bytes = Long.MAX_VALUE;
        for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
            mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
          Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
          EvictorType type = entry.getKey();
          long sBytes = entry.getValue().getAliasWriteBytes(mStorageTierAssoc.getAlias(i));
          if (bytes != Long.MAX_VALUE && bytes != sBytes) {
            equal = false;
          }
          if (sBytes < bytes) {
            bytes = sBytes;
            candidateEvictorType = type;
          }
        }
        if (!equal) {
          break;
        }
      }
      if (equal) {
        for (int i = 0; i < mStorageTierAssoc.size(); i++) {
          long bytes = -1;
          for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
              mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
            Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
            EvictorType type = entry.getKey();
            long sBytes = entry.getValue().getAliasReadBytes(mStorageTierAssoc.getAlias(i));
            if (bytes != -1 && bytes != sBytes) {
              equal = false;
            }
            if (sBytes > bytes) {
              bytes = sBytes;
              candidateEvictorType = type;
            }
          }
          if (!equal) {
            break;
          }
        }
      }
      if (equal) {
        return;
      }
      if (candidateEvictorType != mEvictorType || candidateEvictorType == null) {
        LOG.info("Switch evictor type from {} to {}.", mEvictorType, candidateEvictorType);
        mEvictor = createEvictor(getUpdatedView(), mAllocator, mEvictorType);
      }
      for (int i = 0; i < mStorageTierAssoc.size(); i++) {
        mAliasReadBytes.put(mStorageTierAssoc.getAlias(i), 0L);
        mAliasWriteBytes.put(mStorageTierAssoc.getAlias(i), 0L);
      }
      createSimulateBlockStores();
    }
  }

  /**
   * Get write bytes in the fixed interval according to alias.
   *
   * @param alias alias of tier
   * @return bytes written
   */
  public long getAliasWriteBytes(String alias) {
    return mAliasWriteBytes.get(alias);
  }

  /**
   * Get read bytes in the fixed interval according to alias.
   *
   * @param alias alias of tier
   * @return bytes read
   */
  public long getAliasReadBytes(String alias) {
    return mAliasReadBytes.get(alias);
  }

  @Override
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
    boolean hasBlock;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      hasBlock = mMetaManager.hasBlockMeta(blockId);
    }
    if (hasBlock) {
      return lockId;
    }
    mLockManager.unlockBlock(lockId);
    throw new BlockDoesNotExistException(
        ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_SESSION, blockId, sessionId);
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mLockManager.unlockBlock(lockId);
  }

  @Override
  public void unlockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mLockManager.unlockBlock(sessionId, blockId);
  }

  @Override
  public BlockWriter getBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException, IOException {
    // NOTE: a temp block is supposed to only be visible by its own writer, unnecessary to acquire
    // block lock here since no sharing
    // TODO(bin): Handle the case where multiple writers compete for the same block.
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      return new LocalFileBlockWriter(tempBlockMeta.getPath());
    }
  }

  @Override
  public BlockReader getBlockReader(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mLockManager.validateLock(sessionId, blockId, lockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
      return new LocalFileBlockReader(blockMeta.getPath());
    }
  }

  @Override
  public TempBlockMeta createBlockMeta(long sessionId, long blockId, BlockStoreLocation location,
      long initialBlockSize)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().createBlockMeta(sessionId, blockId, location, initialBlockSize);
        }
      }
    }
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      TempBlockMeta tempBlockMeta =
          createBlockMetaInternal(sessionId, blockId, location, initialBlockSize, true);
      if (tempBlockMeta != null) {
        return tempBlockMeta;
      }
      if (i < MAX_RETRIES) {
        // Failed to create a temp block, so trigger Evictor to make some space.
        // NOTE: a successful {@link freeSpaceInternal} here does not ensure the subsequent
        // allocation also successful, because these two operations are not atomic.
        freeSpaceInternal(sessionId, initialBlockSize, location);
      }
    }
    // TODO(bin): We are probably seeing a rare transient failure, maybe define and throw some
    // other types of exception to indicate this case.
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION,
        initialBlockSize, MAX_RETRIES, blockId);
  }

  // TODO(bin): Make this method to return a snapshot.
  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getBlockMeta(blockId);
    }
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    mLockManager.validateLock(sessionId, blockId, lockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getBlockMeta(blockId);
    }
  }

  @Override
  public void commitBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().commitBlock(sessionId, blockId);
        }
      }
    }
    BlockStoreLocation loc = commitBlockInternal(sessionId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onCommitBlock(sessionId, blockId, loc);
      }
    }
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().abortBlock(sessionId, blockId);
        }
      }
    }
    abortBlockInternal(sessionId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAbortBlock(sessionId, blockId);
      }
    }
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().requestSpace(sessionId, blockId, additionalBytes);
        }
      }
    }
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      Pair<Boolean, BlockStoreLocation> requestResult =
          requestSpaceInternal(blockId, additionalBytes);
      if (requestResult.getFirst()) {
        return;
      }
      if (i < MAX_RETRIES) {
        freeSpaceInternal(sessionId, additionalBytes, requestResult.getSecond());
      }
    }
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION,
        additionalBytes, MAX_RETRIES, blockId);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    moveBlock(sessionId, blockId, BlockStoreLocation.anyTier(), newLocation);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().moveBlock(sessionId, blockId, oldLocation, newLocation);
        }
      }
    }
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      MoveBlockResult moveResult = moveBlockInternal(sessionId, blockId, oldLocation, newLocation);
      if (moveResult.getSuccess()) {
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onMoveBlockByClient(sessionId, blockId, moveResult.getSrcLocation(),
                moveResult.getDstLocation());
          }
        }
        return;
      }
      if (i < MAX_RETRIES) {
        freeSpaceInternal(sessionId, moveResult.getBlockSize(), newLocation);
      }
    }
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE, newLocation,
        blockId, MAX_RETRIES);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    removeBlock(sessionId, blockId, BlockStoreLocation.anyTier());
  }

  @Override
  public void removeBlock(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().removeBlock(sessionId, blockId, location);
        }
      }
    }
    removeBlockInternal(sessionId, blockId, location);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onRemoveBlockByClient(sessionId, blockId);
      }
    }
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().accessBlock(sessionId, blockId);
        }
      }
    }
    boolean hasBlock;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      hasBlock = mMetaManager.hasBlockMeta(blockId);
      if (hasBlock) {
        BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
        long originReadBytes =
            mAliasReadBytes.get(blockMeta.getParentDir().getParentTier().getTierAlias());
        mAliasReadBytes.put(blockMeta.getParentDir().getParentTier().getTierAlias(),
            originReadBytes + blockMeta.getBlockSize());
      }
    }
    if (!hasBlock) {
      throw new BlockDoesNotExistException(ExceptionMessage.NO_BLOCK_ID_FOUND, blockId);
    }
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAccessBlock(sessionId, blockId);
      }
    }
  }

  @Override
  public void freeSpace(long sessionId, long availableBytes, BlockStoreLocation location)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().freeSpace(sessionId, availableBytes, location);
        }
      }
    }
    // TODO(bin): Consider whether to retry here.
    freeSpaceInternal(sessionId, availableBytes, location);
  }

  @Override
  public void cleanupSession(long sessionId) {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().cleanupSession(sessionId);
        }
      }
    }
    // Release all locks the session is holding.
    mLockManager.cleanupSession(sessionId);

    // Collect a list of temp blocks the given session owns and abort all of them with best effort
    List<TempBlockMeta> tempBlocksToRemove;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      tempBlocksToRemove = mMetaManager.getSessionTempBlocks(sessionId);
    }
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      try {
        abortBlockInternal(sessionId, tempBlockMeta.getBlockId());
      } catch (Exception e) {
        LOG.error("Failed to cleanup tempBlock {} due to {}", tempBlockMeta.getBlockId(),
            e.getMessage());
      }
    }
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.hasBlockMeta(blockId);
    }
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    BlockStoreMeta storeMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      storeMeta = mMetaManager.getBlockStoreMeta();
    }
    return storeMeta;
  }

  @Override
  public BlockStoreMeta getBlockStoreMetaFull() {
    BlockStoreMeta storeMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      storeMeta = mMetaManager.getBlockStoreMetaFull();
    }
    return storeMeta;
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    synchronized (mBlockStoreEventListeners) {
      mBlockStoreEventListeners.add(listener);
    }
  }

  /**
   * Checks if a block id is available for a new temp block. This method must be enclosed by
   * {@link #mMetadataLock}.
   *
   * @param blockId the id of block
   * @throws BlockAlreadyExistsException if block id already exists
   */
  private void checkTempBlockIdAvailable(long blockId) throws BlockAlreadyExistsException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new BlockAlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);
    }
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new BlockAlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED, blockId);
    }
  }

  /**
   * Checks if block id is a temporary block and owned by session id. This method must be enclosed
   * by {@link #mMetadataLock}.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   */
  protected void checkTempBlockOwnedBySession(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException {
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new BlockAlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED, blockId);
    }
    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    long ownerSessionId = tempBlockMeta.getSessionId();
    if (ownerSessionId != sessionId) {
      throw new InvalidWorkerStateException(ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_SESSION,
          blockId, ownerSessionId, sessionId);
    }
  }

  /**
   * Aborts a temp block.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   * @throws IOException if I/O errors occur when deleting the block file
   */
  protected void abortBlockInternal(long sessionId, long blockId) throws BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      String path;
      TempBlockMeta tempBlockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        checkTempBlockOwnedBySession(sessionId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
        path = tempBlockMeta.getPath();
      }

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      Files.delete(Paths.get(path));

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        mMetaManager.abortTempBlockMeta(tempBlockMeta);
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // We shall never reach here
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Commits a temp block.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @return destination location to move the block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   * @throws IOException if I/O errors occur when deleting the block file
   */
  protected BlockStoreLocation commitBlockInternal(long sessionId, long blockId)
      throws BlockAlreadyExistsException, InvalidWorkerStateException, BlockDoesNotExistException,
      IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      // When committing TempBlockMeta, the final BlockMeta calculates the block size according to
      // the actual file size of this TempBlockMeta. Therefore, commitTempBlockMeta must happen
      // after moving actual block file to its committed path.
      BlockStoreLocation loc;
      String srcPath;
      String dstPath;
      TempBlockMeta tempBlockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        checkTempBlockOwnedBySession(sessionId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
        srcPath = tempBlockMeta.getPath();
        dstPath = tempBlockMeta.getCommitPath();
        loc = tempBlockMeta.getBlockLocation();
        long originWriteBytes = mAliasWriteBytes.get(loc.tierAlias());
        mAliasWriteBytes.put(loc.tierAlias(), originWriteBytes + tempBlockMeta.getBlockSize());
      }
      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.move(srcPath, dstPath);

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        mMetaManager.commitTempBlockMeta(tempBlockMeta);
      } catch (BlockAlreadyExistsException | BlockDoesNotExistException
          | WorkerOutOfSpaceException e) {
        throw Throwables.propagate(e); // we shall never reach here
      }
      return loc;
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Creates a temp block meta only if allocator finds available space. This method will not trigger
   * any eviction.
   *
   * @param sessionId session Id
   * @param blockId block Id
   * @param location location to create the block
   * @param initialBlockSize initial block size in bytes
   * @param newBlock true if this temp block is created for a new block
   * @return a temp block created if successful, or null if allocation failed (instead of throwing
   *         {@link WorkerOutOfSpaceException} because allocation failure could be an expected case)
   * @throws BlockAlreadyExistsException if there is already a block with the same block id
   */
  protected TempBlockMeta createBlockMetaInternal(long sessionId, long blockId,
      BlockStoreLocation location, long initialBlockSize, boolean newBlock)
      throws BlockAlreadyExistsException {
    // NOTE: a temp block is supposed to be visible for its own writer, unnecessary to acquire
    // block lock here since no sharing
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      if (newBlock) {
        checkTempBlockIdAvailable(blockId);
      }
      StorageDirView dirView =
          mAllocator.allocateBlockWithView(sessionId, initialBlockSize, location, getUpdatedView());
      if (dirView == null) {
        // Allocator fails to find a proper place for this new block.
        return null;
      }
      // TODO(carson): Add tempBlock to corresponding storageDir and remove the use of
      // StorageDirView.createTempBlockMeta.
      TempBlockMeta tempBlock = dirView.createTempBlockMeta(sessionId, blockId, initialBlockSize);
      try {
        // Add allocated temp block to metadata manager. This should never fail if allocator
        // correctly assigns a StorageDir.
        mMetaManager.addTempBlockMeta(tempBlock);
      } catch (WorkerOutOfSpaceException | BlockAlreadyExistsException e) {
        // If we reach here, allocator is not working properly
        LOG.error("Unexpected failure: {} bytes allocated at {} by allocator, "
            + "but addTempBlockMeta failed", initialBlockSize, location);
        throw Throwables.propagate(e);
      }
      return tempBlock;
    }
  }

  /**
   * Increases the temp block size only if this temp block's parent dir has enough available space.
   *
   * @param blockId block Id
   * @param additionalBytes additional bytes to request for this block
   * @return a pair of boolean and {@link BlockStoreLocation}. The boolean indicates if the
   *         operation succeeds and the {@link BlockStoreLocation} denotes where to free more space
   *         if it fails.
   * @throws BlockDoesNotExistException if this block is not found
   */
  private Pair<Boolean, BlockStoreLocation> requestSpaceInternal(long blockId, long additionalBytes)
      throws BlockDoesNotExistException {
    // NOTE: a temp block is supposed to be visible for its own writer, unnecessary to acquire
    // block lock here since no sharing
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      if (tempBlockMeta.getParentDir().getAvailableBytes() < additionalBytes) {
        return new Pair<>(false, tempBlockMeta.getBlockLocation());
      }
      // Increase the size of this temp block
      try {
        mMetaManager.resizeTempBlockMeta(tempBlockMeta,
            tempBlockMeta.getBlockSize() + additionalBytes);
      } catch (InvalidWorkerStateException e) {
        throw Throwables.propagate(e); // we shall never reach here
      }
      return new Pair<>(true, null);
    }
  }

  /**
   * Tries to get an eviction plan to free a certain amount of space in the given location, and
   * carries out this plan with the best effort.
   *
   * @param sessionId the session Id
   * @param availableBytes amount of space in bytes to free
   * @param location location of space
   * @throws WorkerOutOfSpaceException if it is impossible to achieve the free requirement
   * @throws IOException if I/O errors occur when removing or moving block files
   */
  protected void freeSpaceInternal(long sessionId, long availableBytes, BlockStoreLocation location)
      throws WorkerOutOfSpaceException, IOException {
    EvictionPlan plan;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      plan = mEvictor.freeSpaceWithView(availableBytes, location, getUpdatedView());
      // Absent plan means failed to evict enough space.
      if (plan == null) {
        throw new WorkerOutOfSpaceException(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE);
      }
    }

    // 1. remove blocks to make room.
    for (Pair<Long, BlockStoreLocation> blockInfo : plan.toEvict()) {
      try {
        removeBlockInternal(sessionId, blockInfo.getFirst(), blockInfo.getSecond());
      } catch (InvalidWorkerStateException e) {
        // Evictor is not working properly
        LOG.error("Failed to evict blockId {}, this is temp block", blockInfo.getFirst());
        continue;
      } catch (BlockDoesNotExistException e) {
        LOG.info("Failed to evict blockId {}, it could be already deleted", blockInfo.getFirst());
        continue;
      }
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onRemoveBlockByWorker(sessionId, blockInfo.getFirst());
        }
      }
    }
    // 2. transfer blocks among tiers.
    // 2.1. group blocks move plan by the destination tier.
    Map<String, Set<BlockTransferInfo>> blocksGroupedByDestTier = new HashMap<>();
    for (BlockTransferInfo entry : plan.toMove()) {
      String alias = entry.getDstLocation().tierAlias();
      if (!blocksGroupedByDestTier.containsKey(alias)) {
        blocksGroupedByDestTier.put(alias, new HashSet<BlockTransferInfo>());
      }
      blocksGroupedByDestTier.get(alias).add(entry);
    }
    // 2.2. move blocks in the order of their dst tiers, from bottom to top
    for (int tierOrdinal = mStorageTierAssoc.size() - 1; tierOrdinal >= 0; --tierOrdinal) {
      Set<BlockTransferInfo> toMove =
          blocksGroupedByDestTier.get(mStorageTierAssoc.getAlias(tierOrdinal));
      if (toMove == null) {
        toMove = new HashSet<>();
      }
      for (BlockTransferInfo entry : toMove) {
        long blockId = entry.getBlockId();
        BlockStoreLocation oldLocation = entry.getSrcLocation();
        BlockStoreLocation newLocation = entry.getDstLocation();
        MoveBlockResult moveResult;
        try {
          moveResult = moveBlockInternal(sessionId, blockId, oldLocation, newLocation);
        } catch (InvalidWorkerStateException e) {
          // Evictor is not working properly
          LOG.error("Failed to evict blockId {}, this is temp block", blockId);
          continue;
        } catch (BlockAlreadyExistsException e) {
          continue;
        } catch (BlockDoesNotExistException e) {
          LOG.info("Failed to move blockId {}, it could be already deleted", blockId);
          continue;
        }
        if (moveResult.getSuccess()) {
          synchronized (mBlockStoreEventListeners) {
            for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
              listener.onMoveBlockByWorker(sessionId, blockId, moveResult.getSrcLocation(),
                  newLocation);
            }
          }
        }
      }
    }
  }

  /**
   * Gets the most updated view with most recent information on pinned inodes, and currently locked
   * blocks.
   *
   * @return {@link BlockMetadataManagerView}, an updated view with most recent information
   */
  protected BlockMetadataManagerView getUpdatedView() {
    // TODO(calvin): Update the view object instead of creating new one every time.
    synchronized (mPinnedInodes) {
      return new BlockMetadataManagerView(mMetaManager, mPinnedInodes,
          mLockManager.getLockedBlocks());
    }
  }

  /**
   * Moves a block to new location only if allocator finds available space in newLocation. This
   * method will not trigger any eviction. Returns {@link MoveBlockResult}.
   *
   * @param sessionId session Id
   * @param blockId block Id
   * @param oldLocation the source location of the block
   * @param newLocation new location to move this block
   * @return the resulting information about the move operation
   * @throws BlockDoesNotExistException if block is not found
   * @throws BlockAlreadyExistsException if a block with same Id already exists in new location
   * @throws InvalidWorkerStateException if the block to move is a temp block
   * @throws IOException if I/O errors occur when moving block file
   */
  protected MoveBlockResult moveBlockInternal(long sessionId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      long blockSize;
      String srcFilePath;
      String dstFilePath;
      BlockMeta srcBlockMeta;
      BlockStoreLocation srcLocation;
      BlockStoreLocation dstLocation;

      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId);
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        blockSize = srcBlockMeta.getBlockSize();
      }

      if (!srcLocation.belongsTo(oldLocation)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            oldLocation);
      }
      TempBlockMeta dstTempBlock =
          createBlockMetaInternal(sessionId, blockId, newLocation, blockSize, false);
      if (dstTempBlock == null) {
        return new MoveBlockResult(false, blockSize, null, null);
      }

      // When `newLocation` is some specific location, the `newLocation` and the `dstLocation` are
      // just the same; while for `newLocation` with a wildcard significance, the `dstLocation`
      // is a specific one with specific tier and dir which belongs to newLocation.
      dstLocation = dstTempBlock.getBlockLocation();

      // When the dstLocation belongs to srcLocation, simply abort the tempBlockMeta just created
      // internally from the newLocation and return success with specific block location.
      if (dstLocation.belongsTo(srcLocation)) {
        mMetaManager.abortTempBlockMeta(dstTempBlock);
        return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
      }
      dstFilePath = dstTempBlock.getCommitPath();

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.move(srcFilePath, dstFilePath);

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        // If this metadata update fails, we panic for now.
        // TODO(bin): Implement rollback scheme to recover from IO failures.
        mMetaManager.moveBlockMeta(srcBlockMeta, dstTempBlock);
        long originWriteBytes = mAliasWriteBytes.get(dstLocation.tierAlias());
        long originReadBytes = mAliasReadBytes.get(srcLocation.tierAlias());
        mAliasWriteBytes.put(dstLocation.tierAlias(), originWriteBytes + blockSize);
        mAliasReadBytes.put(srcLocation.tierAlias(), originReadBytes + blockSize);
      } catch (BlockAlreadyExistsException | BlockDoesNotExistException
          | WorkerOutOfSpaceException e) {
        // WorkerOutOfSpaceException is only possible if session id gets cleaned between
        // createBlockMetaInternal and moveBlockMeta.
        throw Throwables.propagate(e); // we shall never reach here
      }

      return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Removes a block.
   *
   * @param sessionId session Id
   * @param blockId block Id
   * @param location the source location of the block
   * @throws InvalidWorkerStateException if the block to remove is a temp block
   * @throws BlockDoesNotExistException if this block can not be found
   * @throws IOException if I/O errors occur when removing this block file
   */
  protected void removeBlockInternal(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      String filePath;
      BlockMeta blockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK, blockId);
        }
        blockMeta = mMetaManager.getBlockMeta(blockId);
        filePath = blockMeta.getPath();
      }

      if (!blockMeta.getBlockLocation().belongsTo(location)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            location);
      }
      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      Files.delete(Paths.get(filePath));

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        mMetaManager.removeBlockMeta(blockMeta);
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // we shall never reach here
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Updates the pinned blocks.
   *
   * @param inodes a set of ids inodes that are pinned
   */
  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    if (!mSimulate) {
      for (Iterator<Map.Entry<EvictorType, TieredBlockStore>> it =
          mSimulateBlockStore.entrySet().iterator(); it.hasNext();) {
        Map.Entry<EvictorType, TieredBlockStore> entry = it.next();
        if (entry.getKey() != mEvictorType) {
          entry.getValue().updatePinnedInodes(inodes);
        }
      }
    }
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }

  /**
   * A wrapper on necessary info after a move block operation.
   */
  protected static class MoveBlockResult {
    /** Whether this move operation succeeds. */
    private final boolean mSuccess;
    /** Size of this block in bytes. */
    private final long mBlockSize;
    /** Source location of this block to move. */
    private final BlockStoreLocation mSrcLocation;
    /** Destination location of this block to move. */
    private final BlockStoreLocation mDstLocation;

    /**
     * Creates a new instance of {@link MoveBlockResult}.
     *
     * @param success success indication
     * @param blockSize block size
     * @param srcLocation source location
     * @param dstLocation destination location
     */
    MoveBlockResult(boolean success, long blockSize, BlockStoreLocation srcLocation,
        BlockStoreLocation dstLocation) {
      mSuccess = success;
      mBlockSize = blockSize;
      mSrcLocation = srcLocation;
      mDstLocation = dstLocation;
    }

    /**
     * @return the success indicator
     */
    boolean getSuccess() {
      return mSuccess;
    }

    /**
     * @return the block size
     */
    long getBlockSize() {
      return mBlockSize;
    }

    /**
     * @return the source location
     */
    BlockStoreLocation getSrcLocation() {
      return mSrcLocation;
    }

    /**
     * @return the destination location
     */
    BlockStoreLocation getDstLocation() {
      return mDstLocation;
    }
  }

  /**
   * Reset evictor in a fixed interval.
   */
  class EvictorCheckingThread implements Runnable {
    /**
     * Create a new instance of {@link EvictorCheckingThread}.
     */
    public EvictorCheckingThread() {}

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        resetEvictor();
      }
    }
  }
}
