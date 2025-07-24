// some compilers require this to access UINT64_MAX
#define __STDC_LIMIT_MACROS
#include <stdint.h>

#include <stdio.h>


// larger values here reduce RAM overhead per record slightly
// and may speed up lookup in over-full tables, but might slow
// down lookup in less full tables.
#define LINEARDB3_RECORDS_PER_BUCKET 8



typedef struct {
        // index of another FingerprintBucket in the overflow array,
        // or 0 if no overflow (0 overflow bucket never used)
        uint32_t overflowIndex;

        // record number in the data file 在.db文件里的记录编号
        // val并非数据而是类似fseek的偏移量
        uint32_t fileIndex[ LINEARDB3_RECORDS_PER_BUCKET ];

        // fingerprint mini-hash, mod the largest possible table size in the 32-bit space.
        // We can mod this with our current table size to find the current bin number
        // 指纹迷你哈希, 对当前表的扩容前当前最大表容量, 对key的低32位进行取模
        // (rehashing without actually rehashing the full key 重新哈希但不是重新哈希整个key)
        uint32_t fingerprints[ LINEARDB3_RECORDS_PER_BUCKET ];
    } LINEARDB3_FingerprintBucket;



#define LINEARDB3_BUCKETS_PER_PAGE 4096

typedef struct {
        LINEARDB3_FingerprintBucket buckets[ LINEARDB3_BUCKETS_PER_PAGE ];
    } LINEARDB3_BucketPage;



typedef struct {
        // 使用中的桶数量 (bucket是不用new的, 只需要new页, 因此是逻辑使用的桶)
        uint32_t numBuckets;
        
        // number of allocated pages 已malloc的页数量
        uint32_t numPages;
        
        // number of slots in pages pointer array beyond numPages, there are NULL pointers
        // 页数组长度, 包括null元素
        uint32_t pageAreaSize;
        LINEARDB3_BucketPage **pages;

        uint32_t firstEmptyBucket;

    } LINEARDB3_PageManager;
    
    

enum LastFileOp{ opRead, opWrite };


typedef struct {
        // load above this causes table to expand incrementally 扩容因子 0.5
        double maxLoad;
        
        // number of inserted records in database 表记录数
        uint32_t numRecords;
        

        // for linear hashing table expansion number of slots in base table 
        // 对于线性哈希表扩展基础插槽的数量 (桶的数量, 包括未使用的)
        // 仅在扩容期间作为旧容量
        uint32_t hashTableSizeA;

        // number of slots in expanded table when this reaches hashTableSizeA * 2
        // hash table is done with a full round of expansion
        // and hashTableSizeA is set to hashTableSizeB at that point
        // 当增长到hashTableSizeA * 2时，完成了一轮扩容, 此时sizeA赋值sizeB
        // 扩容期间动态增加
        uint32_t hashTableSizeB;
        

        unsigned int keySize;
        unsigned int valueSize;

        FILE *file;

        // for deciding when fseek is needed between reads and writes 
        // 记录上一个操作类型是读还是写, 决定读写时是否需要fseek?
        LastFileOp lastOp;

        // equal to the largest possible 32-bit table size, given our current table size.
        // used as mod for computing 32-bit hash fingerprints
        // 大于当前表容量的最小2的幂, 也就是扩容前当前最大表容量, 将murmur2哈希转为迷你哈希指纹
        // 是当前哈希表大小 hashTableSizeA 的2 的幂次，直到达到 32 位整数上限
        // hashTableSizeA * 2^n <= uint32_max
        uint32_t fingerprintMod;
        

        unsigned int recordSizeBytes; // key+val字节数

        uint8_t *recordBuffer; // 数据缓冲区

        unsigned int maxOverflowDepth; // 最大溢出深度?


        
        // sized to hashTableSizeB buckets 容量为sizeB
        LINEARDB3_PageManager *hashTable;

        LINEARDB3_PageManager *overflowBuckets; // 溢出桶页数组
        

    } LINEARDB3;









/**
 * Set maximum table load for all subsequent callst to LINEARDB3_open.
 *
 * Defaults to 0.5.
 *
 * When this load is surpassed, hash table expansion occurs.
 *
 * Lower values waste more RAM on table space but result in slightly higher performance.
 *
 * Note that a given DB remembers what maxLoad was set when it was opened,
 * and ignores future calls to setMaxLoad.
 *
 * However, the max load is NOT written into the database file format.
 *
 * Thus, data can be loaded into a table with a different load by setting
 * a differen maxLoad before re-opening the file.
 */
void LINEARDB3_setMaxLoad( double inMaxLoad );




/**
 * Open database
 * 
 * The three _size parameters must be specified if the database could be created or re-created. 
 * Otherwise an error will occur. If the database already exists, 
 * these parameters are ignored and are read from the database. 
 * You can check the struture afterwords to see what they were.
 *
 * @param db Database struct
 * @param path Path to data file.
 * @param inMode is ignored, and always opened in RW-create mode (left for compatibility with KISSDB api)
 * @param inHashTableStartSize Size of hash table in entries
 *   This is the starting size of the table, which will grow as the table
 *   becomes full.  If less than 2, will be automatically raised to 2.
 * @param key_size Size of keys in bytes
 * @param value_size Size of values in bytes
 * @return 0 on success, nonzero on error
 */
int LINEARDB3_open(
    LINEARDB3 *inDB,
    const char *inPath,
    int inMode,
    unsigned int inHashTableStartSize,
    unsigned int inKeySize,
    unsigned int inValueSize );



/**
 * Close database
 *
 * @param db Database struct
 */
void LINEARDB3_close( LINEARDB3 *inDB );



/**
 * Get an entry
 * 读取一个entry
 *
 * @param db Database struct
 * @param key Key (key_size bytes)
 * @param vbuf Value buffer (value_size bytes capacity)
 * @return -1 on I/O error, 0 on success, 1 on not found
 */
int LINEARDB3_get( LINEARDB3 *inDB, const void *inKey, void *outValue );



/**
 * Put an entry (overwriting it if it already exists)
 * In the already-exists case the size of the database file does not change.
 * 写入一个entry (如果已经存在则覆盖, 文件大小不变)
 *
 * @param db Database struct
 * @param key Key (key_size bytes)
 * @param value Value (value_size bytes)
 * @return -1 on I/O error, 0 on success
 */
int LINEARDB3_put( LINEARDB3 *inDB, const void *inKey, const void *inValue );



/**
 * Cursor used for iterating over all entries in database
 * 游标
 */
typedef struct {
        LINEARDB3 *db;
        uint32_t nextRecordIndex;
} LINEARDB3_Iterator;



/**
 * Initialize an iterator
 * 初始化Iteraor
 *
 * @param db Database struct
 * @param i Iterator to initialize
 */
void LINEARDB3_Iterator_init( LINEARDB3 *inDB, LINEARDB3_Iterator *inDBi );



/**
 * Get the next entry
 * 获取下一个entry
 *
 * The order of entries returned by iterator is undefined. It depends on how keys hash.
 * 迭代器的返回顺序是未定义的。它取决于键的哈希值。
 *
 * @param Database iterator
 * @param kbuf Buffer to fill with next key (key_size bytes)
 * @param vbuf Buffer to fill with next value (value_size bytes)
 * @return 0 if there are no more entries, negative on error, 
 *         positive if an kbuf/vbuf have been filled
 */
int LINEARDB3_Iterator_next( LINEARDB3_Iterator *inDBi, void *outKey, void *outValue );







/**
 * More advanced functions below.
 * 以下是高级函数
 *
 * These can be ignored for most usages, which just need open, close, get, put, and iterators.
 */





/**
 * Total number of cells in table, 
 * including those added through incremental expansion due to Linear Hashing algorithm.
 * 获取当前数据库中的插槽数，包括由于Linear Hashing算法扩容的空间
 */
unsigned int LINEARDB3_getCurrentSize( LINEARDB3 *inDB );


/**
 * Number of records that have been inserted in the database.
 * 获取插入的记录数
 */
unsigned int LINEARDB3_getNumRecords( LINEARDB3 *inDB );




/**
 * Gets optimal starting table size for a given load and number of records.
 * 基于负载因子和记录数，获取最佳的初始表大小
 * Return value can be used for inHashTableStartSize in LINEARDB3_open.
 * 返回值可用于LINEARDB3_open中的inHashTableStartSize字段
 */
uint32_t LINEARDB3_getPerfectTableSize( double inMaxLoad, uint32_t inNumRecords );



/**
 * Gets the optimal starting table size, based on an existing inDB, to house inNewNumRecords.  
 * Pays attention to inDB's set maxLoad. 
 * 基于现有的inDB，获取最优的起始表大小，来保存新的记录
 * 注意当前的负载因子
 * 
 * This is useful when iterating through one DB to insert items into a new, smaller DB.
 * Return value can be used for inHashTableStartSize in LINEARDB3_open.
 * 当迭代一个DB，将项插入一个新的较小的DB时，这很有用。
 * 返回值可用于LINEARDB3_open中的inHashTableStartSize字段
 */
unsigned int LINEARDB3_getShrinkSize( LINEARDB3 *inDB, unsigned int inNewNumRecords );
