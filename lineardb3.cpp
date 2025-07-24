#define _FILE_OFFSET_BITS 64

#include "lineardb3.h"

#include <string.h>
#include <stdlib.h>
#include <math.h>

#ifdef _WIN32
#define fseeko fseeko64
#define ftello ftello64
#endif

// #define uint8_t unsigned char
// #define uint32_t unsigned int
// #define uint64_t unsigned long long



// shorten names for internal code
#define FingerprintBucket LINEARDB3_FingerprintBucket
#define BucketPage LINEARDB3_BucketPage
#define PageManager LINEARDB3_PageManager

#define BUCKETS_PER_PAGE LINEARDB3_BUCKETS_PER_PAGE
#define RECORDS_PER_BUCKET LINEARDB3_RECORDS_PER_BUCKET


// 默认负载因子0.5
static double maxLoadForOpenCalls = 0.5;


void LINEARDB3_setMaxLoad( double inMaxLoad ) {
    maxLoadForOpenCalls = inMaxLoad;
    }




#include "murmurhash2_64.cpp"

/*
// djb2 hash function
static uint64_t djb2( const void *inB, unsigned int inLen ) {
    uint64_t hash = 5381;
    for( unsigned int i=0; i<inLen; i++ ) {
        hash = ((hash << 5) + hash) + (uint64_t)(((const uint8_t *)inB)[i]);
        }
    return hash;
    }
*/

// function used here must have the following signature:
// static uint64_t LINEARDB3_hash( const void *inB, unsigned int inLen );
// murmur2 seems to have equal performance on real world data
// and it just feels safer than djb2, which must have done well on test
// data for a weird reason
#define LINEARDB3_hash(inB, inLen) MurmurHash64( inB, inLen, 0xb9115a39 )

// djb2 is resulting in way fewer collisions in test data
//#define LINEARDB3_hash(inB, inLen) djb2( inB, inLen )


/*
// computes 8-bit hashing using different method from LINEARDB3_hash
static uint8_t byteHash( const void *inB, unsigned int inLen ) {
    // use different seed
    uint64_t bigHash = MurmurHash64( inB, inLen, 0x202a025d );
    
    // xor all 8 bytes together
    uint8_t smallHash = bigHash & 0xFF;
    
    smallHash = smallHash ^ ( ( bigHash >> 8 ) & 0xFF );
    smallHash = smallHash ^ ( ( bigHash >> 16 ) & 0xFF );
    smallHash = smallHash ^ ( ( bigHash >> 24 ) & 0xFF );
    smallHash = smallHash ^ ( ( bigHash >> 32 ) & 0xFF );
    smallHash = smallHash ^ ( ( bigHash >> 40 ) & 0xFF );
    smallHash = smallHash ^ ( ( bigHash >> 48 ) & 0xFF );
    smallHash = smallHash ^ ( ( bigHash >> 56 ) & 0xFF );
    
    return smallHash;
    }
*/


/*
// computes 16-bit hashing using different method from LINEARDB3_hash
// hash value is > 0  (so 0 can be used to mark empty cells)
static uint16_t shortHash( const void *inB, unsigned int inLen ) {
    // use different seed
    uint64_t bigHash = MurmurHash64( inB, inLen, 0x202a025d );
    
    // xor all 2-byte chunks together
    uint16_t smallHash = bigHash & 0xFFFF;
    
    smallHash = smallHash ^ ( ( bigHash >> 16 ) & 0xFFFF );
    smallHash = smallHash ^ ( ( bigHash >> 32 ) & 0xFFFF );
    smallHash = smallHash ^ ( ( bigHash >> 48 ) & 0xFFFF );
    
    if( smallHash == 0 ) {
        smallHash = 1;
        }

    return smallHash;
    }
*/






// prototypes for page manager, listed separately for readability

static void initPageManager( PageManager *inPM, uint32_t inNumStartingBuckets );


static void freePageManager( PageManager *inPM );


// returns pointer to newly created bucket
// 分配一个新桶
static FingerprintBucket *addBucket( PageManager *inPM );

// no bounds checking
// 获取页数组的第n个桶,没有边界检查
static FingerprintBucket *getBucket( PageManager *inPM, uint32_t inBucketIndex );

// always skips bucket at index 0. assuming that this call is used for overflowBuckets only
// where index 0 is used to mark buckets with no further overflow
// 总是跳过页的第0个桶，假设这个调用被用于overflowBuckets，第0个桶被用来标记溢出的桶
static uint32_t getFirstEmptyBucketIndex( PageManager *inPM );

// 标记桶为空
static void markBucketEmpty( PageManager *inPM, uint32_t inBucketIndex );





// 初始化页管理器
static void initPageManager( PageManager *inPM, uint32_t inNumStartingBuckets ) {
    inPM->numPages = 1 + inNumStartingBuckets / BUCKETS_PER_PAGE; // malloc的页数量

    inPM->pageAreaSize = 2 * inPM->numPages; // 页数组长度, 包括null元素
    
    inPM->pages = new BucketPage*[ inPM->pageAreaSize ]; // 创建页数组
    
    for( uint32_t i=0; i<inPM->pageAreaSize; i++ ) {
        inPM->pages[i] = NULL;
        }
    
    
    for( uint32_t i=0; i<inPM->numPages; i++ ) { // 给页数组malloc页
        inPM->pages[i] = new BucketPage;
        
        memset( inPM->pages[i], 0, sizeof( BucketPage ) );
        }
    
    inPM->numBuckets = inNumStartingBuckets; // 初始桶数量

    inPM->firstEmptyBucket = 0; // 页数组中第一个空桶的索引
    }


// 析构页管理器
static void freePageManager( PageManager *inPM ) {
    for( uint32_t i=0; i<inPM->numPages; i++ ) {
        delete inPM->pages[i];
        }
    delete [] inPM->pages;

    inPM->pageAreaSize = 0;
    inPM->numPages = 0;
    inPM->numBuckets = 0;
    }


// 给页管理器添加一个桶 (bucket是不用new的, 只需要new页, 因此增加逻辑使用的桶)
static FingerprintBucket *addBucket( PageManager *inPM ) {
    if( inPM->numPages * BUCKETS_PER_PAGE == inPM->numBuckets ) { // 最后一页的桶满了
        // need to allocate a new page 需要malloc一个新的页

        // first make sure there's room 页数组满了
        if( inPM->numPages == inPM->pageAreaSize ) {
    
            // enlarge page area 扩容页数组
            
            uint32_t oldSize = inPM->pageAreaSize;
            
            BucketPage **oldArea = inPM->pages;
            
            
            // double it 新的页数组大小是旧的两倍
            inPM->pageAreaSize = 2 * oldSize;
            
            inPM->pages = new BucketPage*[ inPM->pageAreaSize ];
            
            // NULL just the new slots
            for( uint32_t i=oldSize; i<inPM->pageAreaSize; i++ ) {
                inPM->pages[i] = NULL;
                }
            // 新旧指针内存迁移
            memcpy( inPM->pages, oldArea, oldSize * sizeof( BucketPage* ) );
            
            delete [] oldArea;
            }
        
        // stick new page at end 页数组没满, 找到下一个槽位, malloc一个新页
        inPM->pages[ inPM->numPages ] = new BucketPage;
        
        memset( inPM->pages[ inPM->numPages ], 0, sizeof( BucketPage ) );
        
        inPM->numPages++;
        }


    // room exists, return the empty bucket at end
    // 返回第一个可用的桶
    FingerprintBucket *newBucket = getBucket( inPM, inPM->numBuckets );
    
    inPM->numBuckets++; // 使用中的桶数量++
    
    return newBucket;
    }


// 获取页数组的第n个桶,没有边界检查
static FingerprintBucket *getBucket( PageManager *inPM, uint32_t inBucketIndex ) {
    uint32_t pageNumber = inBucketIndex / BUCKETS_PER_PAGE; // 页号
    uint32_t bucketNumber = inBucketIndex % BUCKETS_PER_PAGE; // 桶号
    
    return &( inPM->pages[ pageNumber ]->buckets[ bucketNumber ] );
    }


// 获取页数组中第一个空桶的索引
static uint32_t getFirstEmptyBucketIndex( PageManager *inPM ) {

    // 第一个空桶的页号和桶号
    uint32_t firstPage = inPM->firstEmptyBucket / BUCKETS_PER_PAGE;
    uint32_t firstBucket = inPM->firstEmptyBucket % BUCKETS_PER_PAGE;

    for( uint32_t p=firstPage; p<inPM->numPages; p++ ) {
        
        BucketPage *page = inPM->pages[p];

        for( int b=firstBucket; b<BUCKETS_PER_PAGE; b++ ) { // 从这个空桶开始往后遍历

            // 指纹为0说明是空槽, 第0个指纹为0说明是空桶
            if( page->buckets[b].fingerprints[0] == 0 ) {
                uint32_t index = p * BUCKETS_PER_PAGE + b; // 当前桶索引
                
                // [不理解] 旧的空桶已经被占, 找到新的空桶后为什么要Buck+1? 可能是往后续写方便使用
                if( index >= inPM->numBuckets ) {
                    // off end of official list of buckets that we know about
                    // keep track of this
                    inPM->numBuckets ++;
                    }

                // ignore index 0 页的第0个桶是溢出标记
                if( index != 0 ) {    
                    // remember for next time
                    // to reduce how many we have to loop through next time
                    // 更新第一个空桶的索引并返回
                    inPM->firstEmptyBucket = index;
                    
                    return index;
                    }
                
                }
            }

        // ignore first bucket index after we pass first page
        // [不理解] 不还是从下一页的第0个桶开始?
        firstBucket = 0;
        }
    
    // none empty in existing pages. create new one off end
    // malloc的页里面已经没有空桶了，创建一个新的页
    uint32_t newIndex = inPM->numBuckets;
    addBucket( inPM );

    inPM->firstEmptyBucket = newIndex; // 更新第一个空桶的位置
    
    return newIndex;
    }


// 将某桶标记为第一个空桶 (记录桶索引)
static void markBucketEmpty( PageManager *inPM, uint32_t inBucketIndex ) {
    if( inBucketIndex < inPM->firstEmptyBucket ) {
        inPM->firstEmptyBucket = inBucketIndex;
        }
    }






















// 魔数
static const char *magicString = "Ld2";

// Ld2 magic characters plus
// two 32-bit ints
// 数据库文件头大小 3+4+4 = 11
#define LINEARDB3_HEADER_SIZE 11





// returns 0 on success, -1 on error 
// 写入文件头
static int writeHeader( LINEARDB3 *inDB ) {
    if( fseeko( inDB->file, 0, SEEK_SET ) ) { // 设置文件指针位置到开头
        return -1;
        }

    int numWritten;
    
    // src pointer, len, block nums, dest pointer
    numWritten = fwrite( magicString, strlen( magicString ), 1, inDB->file );
    if( numWritten != 1 ) {
        return -1;
        }


    uint32_t val32; // key大小

    val32 = inDB->keySize;
    
    numWritten = fwrite( &val32, sizeof(uint32_t), 1, inDB->file );
    if( numWritten != 1 ) {
        return -1;
        }


    val32 = inDB->valueSize; // val大小
    
    numWritten = fwrite( &val32, sizeof(uint32_t), 1, inDB->file );
    if( numWritten != 1 ) {
        return -1;
        }

    return 0;
    }


// 获取记录大小 (key+val)
static int getRecordSizeBytes( int inKeySize, int inValueSize ) {
    return inKeySize + inValueSize;
    }

// 重新计算指纹模数
static void recomputeFingerprintMod( LINEARDB3 *inDB ) {
    inDB->fingerprintMod = inDB->hashTableSizeA;
    
    while( true ) {
        
        uint32_t newMod = inDB->fingerprintMod * 2;
        
        if( newMod <= inDB->fingerprintMod ) { // 当newMod再翻倍变小了, 说明溢出
            // reached 32-bit limit
            return;
            }
        else {
            inDB->fingerprintMod = newMod;
            }
        }
    }



// 获取理想的表大小
uint32_t LINEARDB3_getPerfectTableSize( double inMaxLoad, uint32_t inNumRecords ) {
    // 最小槽位数
    uint32_t minTableRecords = (uint32_t)ceil( inNumRecords / inMaxLoad );
    // 最小桶数
    uint32_t minTableBuckets =(uint32_t)ceil( (double)minTableRecords / 
                                              (double)RECORDS_PER_BUCKET );

    // even if file contains no inserted records, use 2 buckets minimum
    // 如果没有插入的记录, 使用2个桶
    if( minTableBuckets < 2 ) {
        minTableBuckets = 2;
        }
    return minTableBuckets;
    }



// if inIgnoreDataFile (which only applies if inPut is true), we completely
// ignore the data file and don't touch it, updating the RAM hash table only,
// and assuming all unique values on collision
// 获取或插入一个键值对
int LINEARDB3_getOrPut( 
    LINEARDB3 *inDB, 
    const void *inKey, 
    void *inOutValue,
    // 为真时表示允许插入新键
    char inPut, 
    // 为真时（仅在 inPut 为真时生效），表示忽略数据文件，
    // 仅更新内存哈希表，并在冲突时假设所有值都唯一。
    char inIgnoreDataFile = false 
);



// 打开数据库文件
int LINEARDB3_open(
    LINEARDB3 *inDB,
    const char *inPath, // 数据库文件路径
    int inMode, // 兼容性参数, 无效
    unsigned int inHashTableStartSize, // 初始桶数量
    unsigned int inKeySize,
    unsigned int inValueSize ) {
    
    inDB->recordBuffer = NULL; // 记录缓冲区
    inDB->maxOverflowDepth = 0; // 最大溢出深度 (溢出桶链表长度?)

    inDB->numRecords = 0; // 记录数
    
    inDB->maxLoad = maxLoadForOpenCalls; // 负载因子
    

    inDB->file = fopen( inPath, "r+b" ); // 打开已存在的文件
    
    if( inDB->file == NULL ) {
        // doesn't exist yet
        inDB->file = fopen( inPath, "w+b" ); // 创建一个新文件
        }
    
    if( inDB->file == NULL ) {
        return 1;
        }
    
    if( inHashTableStartSize < 2 ) {
        inHashTableStartSize = 2;
        }
    
    inDB->hashTableSizeA = inHashTableStartSize;
    inDB->hashTableSizeB = inHashTableStartSize;
    
    inDB->keySize = inKeySize;
    inDB->valueSize = inValueSize;
    
    inDB->recordSizeBytes = getRecordSizeBytes( inKeySize, inValueSize );
    
    inDB->recordBuffer = new uint8_t[ inDB->recordSizeBytes ];


    recomputeFingerprintMod( inDB );

    inDB->hashTable = new PageManager;
    inDB->overflowBuckets = new PageManager;
    

    // does the file already contain a header? seek to the end to find out file size

    if( fseeko( inDB->file, 0, SEEK_END ) ) { // fseek异常
        return 1;
        }
    
    
    
    if( ftello( inDB->file ) < LINEARDB3_HEADER_SIZE ) {
        // file that doesn't even contain the header
        // write fresh header and hash table, rewrite header
        // 没有文件头
    
        if( writeHeader( inDB ) != 0 ) {
            return 1;
            }
        
        inDB->lastOp = opWrite;
        
        initPageManager( inDB->hashTable, inDB->hashTableSizeA );
        initPageManager( inDB->overflowBuckets, 2 );
    } else {
        // read header 读取文件头
        if( fseeko( inDB->file, 0, SEEK_SET ) ) {
            return 1;
            }
        
        
        int numRead;
        
        char magicBuffer[ 4 ];
        
        numRead = fread( magicBuffer, 3, 1, inDB->file );

        if( numRead != 1 ) {
            return 1;
            }

        magicBuffer[3] = '\0';
        
        if( strcmp( magicBuffer, magicString ) != 0 ) { // 魔数不匹配
            printf( "lineardb3 magic string '%s' not found at start of  "
                    "file header in %s\n", magicString, inPath );
            return 1;
            }
        

        uint32_t val32;
        
        numRead = fread( &val32, sizeof(uint32_t), 1, inDB->file );
        
        if( numRead != 1 ) {
            return 1;
            }
        
        if( val32 != inKeySize ) { // key大小不匹配
            printf( "Requested lineardb3 key size of %u does not match "
                    "size of %u in file header in %s\n", inKeySize, val32,
                    inPath );
            return 1;
            }
        


        numRead = fread( &val32, sizeof(uint32_t), 1, inDB->file );
        
        if( numRead != 1 ) {
            return 1;
            }
        
        if( val32 != inValueSize ) { // val大小不匹配
            printf( "Requested lineardb3 value size of %u does not match "
                    "size of %u in file header in %s\n", inValueSize, val32,
                    inPath );
            return 1;
            }
        

        // got here, header matches 到此为止, 文件头匹配


        // make sure hash table exists in file
        if( fseeko( inDB->file, 0, SEEK_END ) ) {
            return 1;
            }
 
        uint64_t fileSize = ftello( inDB->file );


        uint64_t numRecordsInFile = 
            ( fileSize - LINEARDB3_HEADER_SIZE ) / inDB->recordSizeBytes; // 其实只要除不尽就行
        
        uint64_t expectedSize =
            inDB->recordSizeBytes * numRecordsInFile + LINEARDB3_HEADER_SIZE;
        
        // 根据记录大小反推文件大小
        if( expectedSize != fileSize ) {
            
            printf( "Requested lineardb3 file %s does not contain a "
                    "whole number of %d-byte records.  "
                    "Assuming final record is garbage and truncating it.\n", 
                    inPath, inDB->recordSizeBytes );
        
            char tempPath[200];
            sprintf( tempPath, "%.190s%s", inPath, ".trunc" );

            FILE *tempFile = fopen( tempPath, "wb" );
            
            if( tempFile == NULL ) {
                printf( "Failed to open temp file %s for truncation\n",
                        tempPath );
                return 1;
                }
            
            if( fseeko( inDB->file, 0, SEEK_SET ) ) {
                return 1;
                }
            
            unsigned char headerBuffer[ LINEARDB3_HEADER_SIZE ];
            
            int numRead = fread( headerBuffer, LINEARDB3_HEADER_SIZE, 1, inDB->file );
            
            if( numRead != 1 ) {
                printf( "Failed to read header from lineardb3 file %s\n",
                        inPath );
                return 1;
                }
            int numWritten = fwrite( headerBuffer, LINEARDB3_HEADER_SIZE, 1, tempFile );
            
            if( numWritten != 1 ) {
                printf( "Failed to write header to temp lineardb3 "
                        "truncation file %s\n", tempPath );
                return 1;
                }
                

            for( uint64_t i=0; i<numRecordsInFile; i++ ) {
                numRead = fread( inDB->recordBuffer, inDB->recordSizeBytes, 1, inDB->file );
            
                if( numRead != 1 ) {
                    printf( "Failed to read record from lineardb3 file %s\n", inPath );
                    return 1;
                    }
                
                numWritten = fwrite( inDB->recordBuffer, inDB->recordSizeBytes, 1, tempFile );
            
                if( numWritten != 1 ) {
                    printf( "Failed to record to temp lineardb3 "
                            "truncation file %s\n", tempPath );
                    return 1;
                    }
                }

            fclose( inDB->file );
            fclose( tempFile );
            
            if( rename( tempPath, inPath ) != 0 ) {
                printf( "Failed overwrite lineardb3 file %s with "
                        "truncation file %s\n", inPath, tempPath );
                return 1;
                }

            inDB->file = fopen( inPath, "r+b" );

            if( inDB->file == NULL ) {
                printf( "Failed to re-open lineardb3 file %s after "
                        "trunctation\n", inPath );
                return 1;
                }
            }
        
        
        // now populate hash table 更新哈希表

        uint32_t minTableBuckets = 
            LINEARDB3_getPerfectTableSize( inDB->maxLoad,
                                           numRecordsInFile );

        
        inDB->hashTableSizeA = minTableBuckets;
        inDB->hashTableSizeB = minTableBuckets;
        
        
        recomputeFingerprintMod( inDB );

        initPageManager( inDB->hashTable, inDB->hashTableSizeA );
        initPageManager( inDB->overflowBuckets, 2 );


        if( fseeko( inDB->file, LINEARDB3_HEADER_SIZE, SEEK_SET ) ) {
            return 1;
            }

        // 遍历所有记录, 构建哈希表
        for( uint64_t i=0; i<numRecordsInFile; i++ ) {
            int numRead = fread( inDB->recordBuffer, 
                                 inDB->recordSizeBytes, 1, inDB->file );
            
            if( numRead != 1 ) {
                printf( "Failed to read record from lineardb3 file\n" );
                return 1;
                }

            // put only in RAM part of table
            // note that this assumes that each key in the file is unique
            // (it should be, because we generated the file on a previous run)
            int result = 
                LINEARDB3_getOrPut( inDB,
                                    &( inDB->recordBuffer[0] ),
                                    &( inDB->recordBuffer[inDB->keySize] ),
                                    // 插入
                                    true, 
                                    // ignore data file, update ram only
                                    // don't even verify keys in data file
                                    // this preserves our fread position 保留了文件指针位置
                                    // 仅插入RAM
                                    true );
            if( result != 0 ) {
                printf( "Putting lineardb3 record in RAM hash table failed\n" );
                return 1;
                }
            }
        
        inDB->lastOp = opRead;
        }
    

    


    return 0;
    }



// 关闭数据库
void LINEARDB3_close( LINEARDB3 *inDB ) {
    if( inDB->recordBuffer != NULL ) {
        delete [] inDB->recordBuffer;
        inDB->recordBuffer = NULL;
        }    


    freePageManager( inDB->hashTable );
    freePageManager( inDB->overflowBuckets );
    
    delete inDB->hashTable;
    delete inDB->overflowBuckets;
    

    if( inDB->file != NULL ) {
        fclose( inDB->file );
        inDB->file = NULL;
        }
    }







// key比较
inline char keyComp( int inKeySize, const void *inKeyA, const void *inKeyB ) {
    uint8_t *a = (uint8_t*)inKeyA;
    uint8_t *b = (uint8_t*)inKeyB;
    
    for( int i=0; i<inKeySize; i++ ) {
        if( a[i] != b[i] ) {
            return false;
            }
        }
    return true;
    }




static uint64_t getBinNumber( LINEARDB3 *inDB, uint32_t inFingerprint );



typedef struct {
        FingerprintBucket *nextBucket;
        int nextRecord;
    } BucketIterator;

    


// Repeated calls to this function will insert a series of records
// into an empty bucket and any necessary overflow chain buckets
// 重复调用此函数将插入一系列记录到一个空桶和任何必要的溢出链桶中
// Assumes bucket/record pointed to by iterator is empty and at end of
// chain (assumes this is a fresh series of inserts using an iterator
// that started with an empty bucket).
// 假设迭代器指向的桶/记录为空，且位于链表的末尾
// (假设这是一系列新的插入操作，且迭代器从空桶开始. 说明往同一个哈希槽写数据?)
// 
// Updates iterator
// 迭代器插入桶 (往同一个哈希槽)
static void insertIntoBucket( LINEARDB3 *inDB,
                              BucketIterator *inBucketIterator,
                              uint32_t inFingerprint,
                              uint32_t inFileIndex ) {
    
    if( inBucketIterator->nextRecord == RECORDS_PER_BUCKET ) { // 要插入的记录超出桶大小
        // 分配溢出桶, 找到溢出页数组的第一个空桶的索引
        inBucketIterator->nextBucket->overflowIndex = getFirstEmptyBucketIndex( inDB->overflowBuckets );
        
        inBucketIterator->nextRecord = 0; // 从溢出桶的第0个开始放
        inBucketIterator->nextBucket = // 通过桶索引拿到溢出桶
            getBucket( inDB->overflowBuckets, inBucketIterator->nextBucket->overflowIndex );
    }

    // 指纹
    inBucketIterator->nextBucket-> fingerprints[ inBucketIterator->nextRecord ] = inFingerprint;
    // 文件索引
    inBucketIterator->nextBucket-> fileIndex[ inBucketIterator->nextRecord ] = inFileIndex;
    // 迭代器指向下一条记录
    inBucketIterator->nextRecord++;
}




/* uses method described here:
 * https://en.wikipedia.org/wiki/Linear_hashing
 *
 * 线性哈希（Linear Hashing） 的核心特性。
 * 
 * 线性哈希的设计目标是：
 * 避免一次性扩容整个哈希表
 * 每次扩容只 rehash 一个桶
 * 新桶加入哈希表末尾 addBucket() 添加新桶到哈希表最后
 * 旧桶内容 rehash 后分到旧桶或新桶	使用 getBinNumber() 重新计算指纹对应的桶号
 * 支持溢出桶链表 rehash 会递归处理所有溢出桶
 * 扩容节奏可控	每次只处理一个桶，避免性能抖动
 * 
 * 所以每次扩容只处理一个桶的数据，这样可以：
 * 避免一次性大量 rehash 的开销
 * 保持插入和查询操作的平滑性能
 *
 * returns 0 on success, -1 on failure
 *
 * This call may expand the table by more than one cell, 
 * until the table is big enough that it's at or below the maxLoad
 * 扩容直到负载<=负载因子 (每次扩容bucket+1)
 */
static int expandTable( LINEARDB3 *inDB ) {

    // printf("Expanding table: current load %.2f%% (%u records, %u buckets)\n",
    //    (double)inDB->numRecords / (inDB->hashTableSizeB * RECORDS_PER_BUCKET) * 100,
    //    inDB->numRecords, inDB->hashTableSizeB);
    
    // expand table one cell at a time until we are back at or below maxLoad
    // 每次扩容一桶, 直到满足负载因子
    while( 
        (double)( inDB->numRecords ) / (double)( inDB->hashTableSizeB * RECORDS_PER_BUCKET ) // 负载因子公式
        >
        inDB->maxLoad
    ) {
        uint32_t oldSplitPoint = inDB->hashTableSizeB - inDB->hashTableSizeA;


        // we only need to redistribute records from this bucket
        FingerprintBucket *oldBucket = getBucket( inDB->hashTable, oldSplitPoint );

        // add one bucket to end of of table
        FingerprintBucket *newBucket = addBucket( inDB->hashTable );
        if (!newBucket) { // 内存分配失败
            printf("Error: Failed to allocate new bucket during table expansion.\n");
            return -1;
        }

        uint32_t oldBucketIndex = oldSplitPoint;
        uint32_t newBucketIndex = inDB->hashTableSizeB;

        inDB->hashTableSizeB ++;


        BucketIterator oldIter = { oldBucket, 0 };
        BucketIterator newIter = { newBucket, 0 };
        

        // re-hash all cells at old split point,
        // all the way down any overflow chain that's there
        // 对旧桶的每个记录进行重新哈希, 及其溢出桶
        
        FingerprintBucket *nextOldBucket = oldBucket;
        
        while( nextOldBucket != NULL ) {
            // make a working copy 
            FingerprintBucket tempBucket;
            memcpy( &tempBucket, nextOldBucket, sizeof( FingerprintBucket ) );
            
            // clear the real bucket to make room for new inserts
            // this clears the overflow index as well
            // 清空旧桶, 腾出空间
            memset( nextOldBucket, 0, sizeof( FingerprintBucket ) );
            

            for( int r=0; r<RECORDS_PER_BUCKET; r++ ) {
                uint32_t fingerprint = tempBucket.fingerprints[ r ];
                
                if( fingerprint == 0 ) { // 指纹为0是空槽
                    // stop at first empty spot hit
                    // remaining records empty too
                    break;
                    }
                // 文件索引
                uint32_t fileIndex = tempBucket.fileIndex[ r ];
                // 新桶号
                uint64_t newBinNum = getBinNumber( inDB, fingerprint );
                
                BucketIterator *insertIterator;
                
                if( newBinNum == oldBucketIndex ) {
                    insertIterator = &oldIter;
                }
                else if( newBinNum == newBucketIndex ) {
                    insertIterator = &newIter;
                }
                else {
                    // rehash doesn't hit either bucket? should never happen 指纹重哈希后不属于旧桶或新桶 (正常不会触发)
                    printf("Error: Rehashed bin number %llu does not match old (%u) or new (%u) bucket index.\n",
                           (unsigned long long)newBinNum, oldBucketIndex, newBucketIndex);
                    return -1;
                }
                // 将记录插入新桶
                insertIntoBucket( inDB, insertIterator, fingerprint, fileIndex );
            }
            
            if( tempBucket.overflowIndex != 0 ) { // 有溢出桶
                // process next in overflow chain, reinserting those records next
                nextOldBucket = getBucket( inDB->overflowBuckets, tempBucket.overflowIndex );
                if (!nextOldBucket) { // 获取溢出桶失败 (越界或无效索引)
                    printf("Error: Failed to get overflow bucket at index %u during expansion.\n", 
                        tempBucket.overflowIndex);
                    return -1;
                }
                // we're going to clear it in next iteration of loop, so report it as empty
                // 下轮循环会清空它, 所以标记其为首个空闲桶
                markBucketEmpty( inDB->overflowBuckets, tempBucket.overflowIndex );
            }
            else { // 链表末尾
                // end of chain
                nextOldBucket = NULL;
            }
        }

        
        if( inDB->hashTableSizeB == inDB->hashTableSizeA * 2 ) {
            // full round of expansion is done. 完成一轮完整的扩容 (不一定触发?)
            inDB->hashTableSizeA = inDB->hashTableSizeB;  
        }
    }
    
        
    // printf("Finished expanding table. Final size: %u\n", inDB->hashTableSizeB);

    return 0;
}



// 基于64位哈希值获取桶号 (通过指纹也是等效的)
static uint64_t getBinNumberFromHash( LINEARDB3 *inDB, uint64_t inHashVal ) {

    uint64_t binNumberA = inHashVal % (uint64_t)( inDB->hashTableSizeA );
    
    uint64_t binNumberB = binNumberA;
    


    unsigned int splitPoint = inDB->hashTableSizeB - inDB->hashTableSizeA;
    
    
    if( binNumberA < splitPoint ) {
        // points before split can be mod'ed with double base table size

        // binNumberB will always fit in hashTableSizeB, the expanded table
        binNumberB = inHashVal % (uint64_t)( inDB->hashTableSizeA * 2 );
    }
    return binNumberB;
}


// 基于指纹获取桶号
static uint64_t getBinNumber( LINEARDB3 *inDB, uint32_t inFingerprint ) {
    // fingerprint is such that it will always land in the same bin as full hash val
    // 指纹能够像64位哈希值计算得到同一个桶号
    return getBinNumberFromHash( inDB, inFingerprint );
}


// 基于key获取桶号
static uint64_t getBinNumber( LINEARDB3 *inDB, const void *inKey, uint32_t *outFingerprint ) {
    // murmurhash2计算key的64位哈希值
    uint64_t hashVal = LINEARDB3_hash( inKey, inDB->keySize );
    // 哈希值取模得到指纹
    *outFingerprint = hashVal % inDB->fingerprintMod;

    if( *outFingerprint == 0 ) {
        // forbid straight 0 as fingerprint value
        // we use 0-fingerprints as not-present flags
        // for the rare values that land on 0, we need to make sure
        // main hash changes along with fingerprint
        // 指纹为0, 极罕见的情况, 指纹0用来表示空槽
        
        if( hashVal < UINT64_MAX ) {
            hashVal++;
        } else {
            hashVal--;
        }
        
        *outFingerprint = hashVal % inDB->fingerprintMod;
    }
    
    
    return getBinNumberFromHash( inDB, hashVal );
}




// Consider getting/putting from inBucket at inRecIndex
// 考虑在inRecindex上从inbucket获取/放置
// 在指定的哈希桶（bucket）中的指定记录位置（record index）上尝试进行获取或插入/更新操作
// 用于实现哈希表中指纹 匹配查找、插入新记录 或 更新已有记录。
//
// returns 0 if handled and done
// returns -1 on error
// returns 1 if guaranteed not found
// 返回1（如果找不到的话）
// returns 2 if bucket full and not found 
// 返回2，如果桶满了，找不到 (碰撞)
static int LINEARDB3_considerFingerprintBucket( 
    LINEARDB3 *inDB,
    const void *inKey,
    void *inOutValue, // 输入/输出值（put 时写入，get 时读取）
    uint32_t inFingerprint,
    char inPut, // 0 for get, 1 for put
    char inIgnoreDataFile, // 仅更新RAM
    FingerprintBucket *inBucket, // 要检查的桶
    int inRecIndex // 桶中的记录索引 0-7
) {
    
    int i = inRecIndex;
    
    uint32_t binFP = inBucket->fingerprints[ i ]; // 当前桶指纹
        
    char emptyRec = false;
        
    if( binFP == 0 ) { // 桶内记录为空
        emptyRec = true;
            
        if( inPut ) {
            // set fingerprint and file pos for insert
            binFP = inFingerprint;
            inBucket->fingerprints[ i ] = inFingerprint;
                
            // will go at end of file
            inBucket->fileIndex[ i ] = inDB->numRecords;
                

            inDB->numRecords++;

            if( inIgnoreDataFile ) {
                // finished non-file insert
                return 0;
            }
        } else {
            return 1;
        }
    }

    if( binFP == inFingerprint ) { // 指纹匹配
        // hit

        if( inIgnoreDataFile ) {
            // treat any fingerprint match as a collision
            // assume all unique data if we're ignoring the data file
            // [不理解] 都认为是碰撞?
            return 2;
        }
        
        // 文件偏移量(字节) = 文件头大小 + 文件索引 * 记录大小
        // [MARK] (uint64_t)inBucket->fileIndex[i] * (uint64_t)inDB->recordSizeBytes;
        uint64_t filePosRec = 
            LINEARDB3_HEADER_SIZE + (uint64_t)inBucket->fileIndex[ i ] * (uint64_t)inDB->recordSizeBytes;
            
        if( !emptyRec ) { // 桶内记录非空 (检查key是否匹配, 哈希匹配不代表完全一致)
            
            // read key to make sure it actually matches
            // 即使指纹匹配, 也要拿到原始key做比较
            
            // never seek unless we have to 非必要不做fseek (off_t 是 int64_t)
            if( inDB->lastOp == opWrite || ftello( inDB->file ) != (off_t)filePosRec ) {

                if( fseeko( inDB->file, filePosRec, SEEK_SET ) ) {
                    return -1;
                }
            }
            
            int numRead = fread( inDB->recordBuffer, inDB->keySize, 1, inDB->file );
            inDB->lastOp = opRead;
    
            if( numRead != 1 ) {
                return -1;
            }
            if( ! keyComp( inDB->keySize, inDB->recordBuffer, inKey ) ) {
                // false match on non-empty rec because of fingerprint collision
                // 指纹相同但是key不同, 是哈希碰撞
                return 2;
            }
        }

            
        if( inPut ) { // 写入操作
            if( emptyRec ) {

                // don't seek unless we have to. if we're doing a series of fresh inserts,
                // the file pos is already waiting at the end of the file for us
                // 如果当前是连续的插入, 那么指针位置是正好的
                if( inDB->lastOp == opRead || ftello( inDB->file ) != (off_t)filePosRec ) {
                    
                    // no seeking done yet, go to end of file
                    if( fseeko( inDB->file, 0, SEEK_END ) ) {
                        return -1;
                    }
                    // make sure it matches where we've documented that the record should go
                    if( ftello( inDB->file ) != (off_t)filePosRec ) {
                        return -1;
                    }
                }
                
                // 写入key
                int numWritten = fwrite( inKey, inDB->keySize, 1, inDB->file );
                inDB->lastOp = opWrite;
                
                if( numWritten != 1 ) {
                    return -1;
                }
            }
            
            // else already seeked and read key of non-empty record ready to write value

            // still need to seek here after reading before writing according to fopen docs
            fseeko( inDB->file, 0, SEEK_CUR );
            
            // 写入value
            int numWritten = fwrite( inOutValue, inDB->valueSize, 1, inDB->file );
            inDB->lastOp = opWrite;
    
            if( numWritten != 1 ) {
                return -1;
            }
            
            // successful put    
            return 0;
        }
        else { // 读取操作
            // we don't need to seek here, we know (!emptyRec), 
            // so we already seeked and read the key above, ready to read value now
            // 因为我们已经读取了key, 所以可以继续读取value
            int numRead = fread( inOutValue, inDB->valueSize, 1, inDB->file );
            inDB->lastOp = opRead;
            
            if( numRead != 1 ) {
                return -1;
            }
            return 0;
        }
    }
    
    // rec full but didn't match 键不匹配 (碰撞)
    return 2;
}




// 获取或写入
int LINEARDB3_getOrPut( 
    LINEARDB3 *inDB, 
    const void *inKey, 
    void *inOutValue,
    char inPut, 
    char inIgnoreDataFile 
) {

    uint32_t fingerprint;

    uint64_t binNumber = getBinNumber( inDB, inKey, &fingerprint );

    
    unsigned int overflowDepth = 0;

    FingerprintBucket *thisBucket = getBucket( inDB->hashTable, binNumber );


    char skipToOverflow = false;

    if( inPut && inIgnoreDataFile ) {
        skipToOverflow = true;
    }
    
    if( !skipToOverflow || thisBucket->overflowIndex == 0 )
    for( int i=0; i<RECORDS_PER_BUCKET; i++ ) {

        int result = LINEARDB3_considerFingerprintBucket(
            inDB, inKey, inOutValue,
            fingerprint,
            inPut, inIgnoreDataFile,
            thisBucket, 
            i );
        
        if( result < 2 ) {
            return result;
        }
        // 2 means record didn't match, keep going
        // 2 说明碰撞了
    }

    
    uint32_t thisBucketIndex = 0;
    
    while( thisBucket->overflowIndex > 0 ) { // 查看溢出链上能不能找到
        // consider overflow
        overflowDepth++;

        if( overflowDepth > inDB->maxOverflowDepth ) {
            inDB->maxOverflowDepth = overflowDepth;
        }
        
        
        thisBucketIndex = thisBucket->overflowIndex;
        
        thisBucket = getBucket( inDB->overflowBuckets, thisBucketIndex );

        if( !skipToOverflow || thisBucket->overflowIndex == 0 )
        for( int i=0; i<RECORDS_PER_BUCKET; i++ ) {

            int result = LINEARDB3_considerFingerprintBucket(
                inDB, inKey, inOutValue,
                fingerprint,
                inPut, inIgnoreDataFile,
                thisBucket, 
                i );
        
            if( result < 2 ) {
                return result;
            }
            // 2 means record didn't match, keep going
            // 2 说明仍然碰撞了
        }
    }

    
    if( inPut && thisBucket->overflowIndex == 0 ) {

        // reached end of overflow chain without finding place to put value
        // need to make a new overflow bucket
        // 走完溢出链也没找到插入的位置, 需要创建一个溢出桶

        overflowDepth++;

        if( overflowDepth > inDB->maxOverflowDepth ) {
            inDB->maxOverflowDepth = overflowDepth;
        }
        

        thisBucket->overflowIndex = 
            getFirstEmptyBucketIndex( inDB->overflowBuckets );

        FingerprintBucket *newBucket = 
            getBucket( inDB->overflowBuckets, thisBucket->overflowIndex );
        newBucket->fingerprints[0] = fingerprint;

        // will go at end of file
        newBucket->fileIndex[0] = inDB->numRecords;
        
        
        inDB->numRecords++;
        
        if( ! inIgnoreDataFile ) {

            // [MARK]
            uint64_t filePosRec = 
                LINEARDB3_HEADER_SIZE + (uint64_t)newBucket->fileIndex[0] * (uint64_t)inDB->recordSizeBytes;

            // don't seek unless we have to
            if( inDB->lastOp == opRead || ftello( inDB->file ) != (off_t)filePosRec ) {
            
                // go to end of file
                if( fseeko( inDB->file, 0, SEEK_END ) ) {
                    return -1;
                }

                // make sure it matches where we've documented that the record should go
                if( ftello( inDB->file ) != (off_t)filePosRec ) {
                    return -1;
                }
            }
            
            // 写入key与value
            int numWritten = fwrite( inKey, inDB->keySize, 1, inDB->file );
            inDB->lastOp = opWrite;

            numWritten += fwrite( inOutValue, inDB->valueSize, 1, inDB->file );
                
            if( numWritten != 2 ) {
                return -1;
            }
            return 0;
        }
        

        return 0;
    }



    // not found
    return 1;
}



int LINEARDB3_get( LINEARDB3 *inDB, const void *inKey, void *outValue ) {
    return LINEARDB3_getOrPut( inDB, inKey, outValue, false, false );
}



int LINEARDB3_put( LINEARDB3 *inDB, const void *inKey, const void *inValue ) {
    int result = LINEARDB3_getOrPut( inDB, inKey, (void *)inValue, true, false );

    if( result == -1 ) {
        return result;
    }

    // 每次插入检查负载, 超出则立刻扩容
    if( inDB->numRecords > ( inDB->hashTableSizeB * RECORDS_PER_BUCKET ) * inDB->maxLoad ) {
        result = expandTable( inDB );
    }
    return result;
}



void LINEARDB3_Iterator_init( LINEARDB3 *inDB, LINEARDB3_Iterator *inDBi ) {
    inDBi->db = inDB;
    inDBi->nextRecordIndex = 0;
}




int LINEARDB3_Iterator_next( LINEARDB3_Iterator *inDBi, void *outKey, void *outValue ) {
    LINEARDB3 *db = inDBi->db;
    
    while( true ) {        
        
        if( inDBi->nextRecordIndex >= db->numRecords ) {
            return 0;
        }

        // fseek is needed here to make iterator safe to interleave with other calls
        
        // BUT, don't seek unless we have to
        // even seeking to current location has a performance hit
        // [MARK]
        uint64_t filePosRec = 
            LINEARDB3_HEADER_SIZE + (uint64_t)inDBi->nextRecordIndex * (uint64_t)db->recordSizeBytes;
        

        if( db->lastOp == opWrite || ftello( db->file ) != (off_t)filePosRec ) {
    
            if( fseeko( db->file, filePosRec, SEEK_SET ) ) {
                return -1;
            }
        }
        

        int numRead = fread( outKey, db->keySize, 1, db->file );
        db->lastOp = opRead;
        
        if( numRead != 1 ) {
            return -1;
        }
        
        numRead = fread( outValue, db->valueSize, 1, db->file );
        if( numRead != 1 ) {
            return -1;
        }
        
        inDBi->nextRecordIndex++;
        return 1;
    }
}




unsigned int LINEARDB3_getCurrentSize( LINEARDB3 *inDB ) {
    return inDB->hashTableSizeB;
}



unsigned int LINEARDB3_getNumRecords( LINEARDB3 *inDB ) {
    return inDB->numRecords;
}




unsigned int LINEARDB3_getShrinkSize( LINEARDB3 *inDB, unsigned int inNewNumRecords ) {

    // perfect size to insert this many with no table expansion
    
    uint32_t minTableBuckets = 
        LINEARDB3_getPerfectTableSize( inDB->maxLoad, inNewNumRecords );

    return minTableBuckets;
}
