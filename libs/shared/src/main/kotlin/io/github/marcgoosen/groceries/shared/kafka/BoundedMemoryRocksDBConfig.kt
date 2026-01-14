package io.github.marcgoosen.groceries.shared.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.Cache
import org.rocksdb.LRUCache
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteBufferManager

/**
 * Setting some custom RocksDB configuration based on
 * https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html#rocksdb
 * to limit the memory usage.
 *
 * See also https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
 *
 * In general, we try to not use persistent state stores (so no rocksdb)
 */
private val logger = KotlinLogging.logger {}

class BoundedMemoryRocksDBConfig : RocksDBConfigSetter {
    override fun setConfig(storeName: String, options: Options, configs: Map<String, Any>) {
        if (cache == null || writeBufferManager == null) initCacheOnce(configs)
        val tableConfig = options.tableFormatConfig() as BlockBasedTableConfig
        tableConfig.setBlockCache(cache)
        configs[ROCKSDB_BLOCK_SIZE_KB]?.parseAsLong()?.apply {
            logger.info { "Using BLOCK_SIZE=$this Kb" }
            tableConfig.setBlockSize(this * KB_FACTOR)
        }

        // These options are recommended to be set when bounding the total memory
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true)
        tableConfig.setPinTopLevelIndexAndFilter(true)

        options.setTableFormatConfig(tableConfig)
        options.setWriteBufferManager(writeBufferManager)
        configs[ROCKSDB_N_MEMTABLES]?.parseAsInt()?.apply {
            logger.info { "Using N_MEMTABLE=$this" }
            options.setMaxWriteBufferNumber(this)
        }
        configs[ROCKSDB_MEMTABLE_SIZE_MB]?.parseAsLong()?.apply {
            logger.info { "Using MEMTABLE_SIZE=$this Mb" }
            options.setWriteBufferSize(this * MB_FACTOR)
        }

        // Enable compression (optional). Compression can decrease the required storage
        // and increase the CPU usage of the machine. For CompressionType values, see
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/6.4.6/org/rocksdb/CompressionType.html.
        // options.setCompressionType(CompressionType.LZ4_COMPRESSION) Disabling this for now because we want less cpu usage.
    }

    override fun close(storeName: String, options: Options) {
        logger.info { "close($storeName)" }
        // Cache and WriteBufferManager should not be closed here, as the same objects are shared by every store instance.
    }

    @Synchronized
    fun initCacheOnce(configs: Map<String, Any>) {
        if (cache != null && writeBufferManager != null) {
            logger.warn { "RocksDB: cache and write buffer manager are already initialized, this shoudl not happen" }
            // already initialized
            return
        }
        logger.info { "RocksDB: Initializing cache and write buffer manager" }
        val offHeapMb = configs[ROCKSDB_TOTAL_OFF_HEAP_SIZE_MB]?.parseAsLong() ?: 16
        val totalMemTableMemMb = configs[ROCKSDB_TOTAL_MEMTABLE_MB]?.parseAsLong() ?: 16
        logger.info { "Using TOTAL_OFF_HEAP_MEMORY=$offHeapMb Mb" }
        logger.info { "USing TOTAL_MEMTABLE_MEMORY=$totalMemTableMemMb Mb" }
        if (cache == null) {
            cache = LRUCache(offHeapMb * MB_FACTOR)
        }
        if (writeBufferManager == null) writeBufferManager = WriteBufferManager(totalMemTableMemMb * MB_FACTOR, cache)
    }

    companion object {
        const val ROCKSDB_TOTAL_OFF_HEAP_SIZE_MB = "rocksdb.total_offheap_size_mb"
        const val ROCKSDB_TOTAL_MEMTABLE_MB = "rocksdb.total_memtable_mb"
        const val ROCKSDB_BLOCK_SIZE_KB = "rocksdb.block_size_kb"
        const val ROCKSDB_N_MEMTABLES = "rocksdb.n_memtables"
        const val ROCKSDB_MEMTABLE_SIZE_MB = "rocksdb.memtable_size_mb"

        private const val BYTE_FACTOR: Long = 1
        private const val KB_FACTOR = 1024 * BYTE_FACTOR
        private const val MB_FACTOR = 1024 * KB_FACTOR
        private var cache: Cache? = null
        private var writeBufferManager: WriteBufferManager? = null

        init {
            RocksDB.loadLibrary()
        }
    }

    fun Any.parseAsLong(): Long? = toString().takeIf { it.isNotEmpty() }?.toLong()
    fun Any.parseAsInt(): Int? = toString().takeIf { it.isNotEmpty() }?.toInt()
}
