package io.github.marcgoosen.groceries.shared.kafka

import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.Options

class BoundedMemoryRocksDBConfigTest {
    private val options = mockk<Options>(relaxed = true)
    private val blockBasedTableConfig = mockk<BlockBasedTableConfig>(relaxed = true)
    private val configs = mapOf(
        "rocksdb.config.setter" to BoundedMemoryRocksDBConfig::class.qualifiedName!!,
        "rocksdb.total_offheap_size_mb" to "16",
        "rocksdb.total_memtable_mb" to "16",
        "rocksdb.block_size_kb" to "16",
        "rocksdb.n_memtables" to "2",
        "rocksdb.memtable_size_mb" to "64",
    )

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        every { options.tableFormatConfig() } returns blockBasedTableConfig
    }

    @AfterEach
    fun tearDown() {
//        confirmVerified(counter)
    }

    @Test
    fun `It should create a BoundedMemoryRocksDBConfig`() {
        val boundedMemoryRocksDBConfig = BoundedMemoryRocksDBConfig()
        boundedMemoryRocksDBConfig.setConfig("test", options, configs)
    }
}
