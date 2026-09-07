package io.github.marcgoosen.groceries.shared.kafka

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.Options

private const val KB: Long = 1024
private const val MB = 1024 * KB

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
    fun onSetup() {
        clearAllMocks()
        every { options.tableFormatConfig() } returns blockBasedTableConfig
    }

    @Test
    fun `It should size the block cache from the configured block size`() {
        // Given
        val blockSize = slot<Long>()
        every { blockBasedTableConfig.setBlockSize(capture(blockSize)) } returns blockBasedTableConfig

        // When
        BoundedMemoryRocksDBConfig().setConfig("store", options, configs)

        assertThat(blockSize.captured).isEqualTo(16 * KB)
    }

    @Test
    fun `It should size the memtables from the configured memtable size and count`() {
        // Given
        val writeBufferSize = slot<Long>()
        val maxWriteBufferNumber = slot<Int>()
        every { options.setWriteBufferSize(capture(writeBufferSize)) } returns options
        every { options.setMaxWriteBufferNumber(capture(maxWriteBufferNumber)) } returns options

        // When
        BoundedMemoryRocksDBConfig().setConfig("store", options, configs)

        assertThat(writeBufferSize.captured).isEqualTo(64 * MB)
        assertThat(maxWriteBufferNumber.captured).isEqualTo(2)
    }

    @Test
    fun `It should pin the index and filter blocks so the cache stays bounded`() {
        // Given
        // When
        BoundedMemoryRocksDBConfig().setConfig("store", options, configs)

        verify {
            blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true)
            blockBasedTableConfig.setPinTopLevelIndexAndFilter(true)
            options.setWriteBufferManager(any())
        }
    }

    @Test
    fun `It should leave the defaults alone when no sizes are configured`() {
        // Given
        val onlySetter = mapOf("rocksdb.config.setter" to BoundedMemoryRocksDBConfig::class.qualifiedName!!)

        // When
        BoundedMemoryRocksDBConfig().setConfig("store", options, onlySetter)

        verify(exactly = 0) {
            blockBasedTableConfig.setBlockSize(any<Long>())
            options.setWriteBufferSize(any())
            options.setMaxWriteBufferNumber(any())
        }
    }
}
