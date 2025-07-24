#include "lineardb3.h"
#include "timer.cpp"
#include <cstring>
using namespace std;

#define uint32_t unsigned int
#define uint64_t unsigned long long
#define LINEARDB3_HEADER_SIZE 11

class Timer;
void floor_db_test();
void map_time_db_test();
void map_time_db_shrink();
void map_db_shrink();

int main(int argc, char *argv[]){
    // floor_db_test();
    // map_time_db_test();
    // map_time_db_shrink();

    if (argc < 2) {
        printf("Usage: %s <db_name>\n", argv[0]);
        return 0;
    }

    Timer t;
    
    if (strcmp(argv[1], "map.db") == 0) {
        map_db_shrink();
    } else if (strcmp(argv[1], "mapTime.db") == 0) {
        map_time_db_shrink();
    } else {
        printf("available db list: map.db, mapTime.db\n");
    }

    t.elapsed();
}

/**
 * 算法已核验
 * cnt: 178956972 -> 106944141
 */
void map_db_shrink() {

    printf( "Generating Shrinked database...\n" );

    char *dbPath = "map.db";
    char *dbPathShrink = "map_shrink.db";

    // key = x, y, s, b
    // val = oid
    // uint32_t key[4] = { 0x00000000, 0x00000000, 0x00000000, 0x00000000 };
    // uint32_t val[1] = { 0x00000000 };
    uint32_t recordSizeBytes = 20;
    uint32_t recordBuffer[5] = { 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000 };
    uint32_t recordBufferGet[5] = { 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000 };
    unsigned char headerBuffer[ LINEARDB3_HEADER_SIZE ];

    LINEARDB3 *db = new LINEARDB3();
    LINEARDB3_open(
        db,
        dbPath,
        0,
        8000,
        16,
        4
    );
    FILE *originFile = fopen( dbPath, "r+b" );
    if ( originFile == NULL ) {
        printf( "Error opening originFile\n" );
        return;
    }
    FILE *shrinkFile = fopen( dbPathShrink, "w+b" );
    if ( shrinkFile == NULL ) {
        printf( "Error opening shrinkFile\n" );
        return;
    }

    if( fseeko( originFile, 0, SEEK_END ) ) return;
    uint64_t fileSize = ftello( originFile );
    uint64_t numRecordsInFile = ( fileSize - LINEARDB3_HEADER_SIZE ) / recordSizeBytes;
    
    if( fseeko( originFile, 0, SEEK_SET ) ) return;
    int numRead = fread( headerBuffer, LINEARDB3_HEADER_SIZE, 1, originFile );
    if( numRead != 1 ) {
        printf( "Failed to read header from lineardb3 file\n");
        return;
    }

    int numWritten = fwrite( headerBuffer, LINEARDB3_HEADER_SIZE, 1, shrinkFile );
    if( numWritten != 1 ) {
        printf( "Failed to write header to temp lineardb3 truncation file\n" );
        return;
    }

    for( uint64_t i=0; i<numRecordsInFile; i++ ) {
        numRead = fread( recordBuffer, recordSizeBytes, 1, originFile );
        if( numRead != 1 ) {
            printf( "Failed to read record from lineardb3 file\n" );
            return;
        }

        if (recordBuffer[2] == 0 && recordBuffer[3] == 0) { // 主物品
            // 主物品非0
            if (recordBuffer[4] != 0) {
                numWritten = fwrite( recordBuffer, recordSizeBytes, 1, shrinkFile );
                if( numWritten != 1 ) {
                    printf( "Failed to record to temp lineardb3 truncation file\n" );
                    return;
                }
            }
        } else { // 子物品, 其他属性
            recordBufferGet[0] = recordBuffer[0];
            recordBufferGet[1] = recordBuffer[1];
            recordBufferGet[2] = 0;
            recordBufferGet[3] = 0;
            // 获取主物品 -1 on I/O error, 0 on success, 1 on not found
            int result = LINEARDB3_get( db, &recordBufferGet[0], &recordBufferGet[4] );
            // 存在主物品记录且主物品非0
            if (result == 0 && recordBufferGet[4] != 0) {
                numWritten = fwrite( recordBuffer, recordSizeBytes, 1, shrinkFile );
                if( numWritten != 1 ) {
                    printf( "Failed to record to temp lineardb3 truncation file\n" );
                    return;
                }
            }
        }
    }

    fclose( originFile );
    fclose( shrinkFile );
    LINEARDB3_close( db );
}

/**
 * 算法已核验
 * Time taken: 233374 ms
 * maxload is 0.500000
 * Database size: 44739243
 * numBuckets: 44739243
 * overflowBuckets: 955855
 * cnt: 72012831 / 178956972 -> 106944141
 */
void map_time_db_shrink() {

    printf( "Generating Shrinked database...\n" );

    uint32_t recordSizeBytes = 24;
    uint32_t recordBuffer[6] = { 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000 };
    unsigned char headerBuffer[ LINEARDB3_HEADER_SIZE ];

    FILE *originFile = fopen( "mapTime.db", "r+b" );
    if ( originFile == NULL ) {
        printf( "Error opening originFile\n" );
        return;
    }
    FILE *shrinkFile = fopen( "mapTime_shrink.db", "w+b" );
    if ( shrinkFile == NULL ) {
        printf( "Error opening shrinkFile\n" );
        return;
    }

    if( fseeko( originFile, 0, SEEK_END ) ) return;
    uint64_t fileSize = ftello( originFile );
    uint64_t numRecordsInFile = ( fileSize - LINEARDB3_HEADER_SIZE ) / recordSizeBytes;
    
    if( fseeko( originFile, 0, SEEK_SET ) ) return;
    int numRead = fread( headerBuffer, LINEARDB3_HEADER_SIZE, 1, originFile );
    if( numRead != 1 ) {
        printf( "Failed to read header from lineardb3 file\n");
        return;
    }

    int numWritten = fwrite( headerBuffer, LINEARDB3_HEADER_SIZE, 1, shrinkFile );
    if( numWritten != 1 ) {
        printf( "Failed to write header to temp lineardb3 truncation file\n" );
        return;
    }

    for( uint64_t i=0; i<numRecordsInFile; i++ ) {
        numRead = fread( recordBuffer, recordSizeBytes, 1, originFile );
        if( numRead != 1 ) {
            printf( "Failed to read record from lineardb3 file\n" );
            return;
        }

        if (recordBuffer[4] != 0 || recordBuffer[5] != 0) {
            numWritten = fwrite( recordBuffer, recordSizeBytes, 1, shrinkFile );
            if( numWritten != 1 ) {
                printf( "Failed to record to temp lineardb3 truncation file\n" );
                return;
            }
        }
    }

    fclose( originFile );
    fclose( shrinkFile );
}

void map_time_db_test() {
    LINEARDB3 *db = new LINEARDB3();

    printf( "Opening database...\n" );

    Timer t0;
    // 文件是小端序的, 但是输入和输出都是大端, 但是元素之间是小端的(反向)
    LINEARDB3_open(
        db,
        "../fsdownload/mapTime.db", // totalRecords: 178956971
        0,
        8000,
        16,
        8
    );
    t0.elapsed();

    printf("maxload is %f\n", db->maxLoad);
    printf( "Database size: %u\n", db->hashTableSizeB);
    printf( "numBuckets: %u\n", db->hashTable->numBuckets);
    printf( "overflowBuckets: %u\n", db->overflowBuckets->numBuckets);

    // key = x, y, s, b
    // val = time
    // uint32_t key[4] = { 0x00000000, 0x00000001, 0xffffbc91, 0x0000277a };
    uint32_t key[4] = { 0x0000284a, 0xffffbd6b, 0x00000001, 0x00000000 };
    uint32_t val[2] = { 0x00000000, 0x00000000 };

    // LINEARDB3_put( db, key, val );
    LINEARDB3_get( db, key, val );
    printf( "Key %08x, %08x, %08x, %08x\n", key[0], key[1], key[2], key[3]);
    printf( "Val %08x, %08x            \n", val[0], val[1]);

}

void floor_db_test() {
    LINEARDB3 *db = new LINEARDB3();

    printf( "Opening database...\n" );

    // 文件是小端序的, 但是输入和输出都是大端
    LINEARDB3_open(
        db,
        "floor.db",
        0,
        8000,
        8,
        4
    );

    printf("maxload is %f\n", db->maxLoad);
    printf( "Database size: %u\n", db->hashTableSizeB);
    printf( "numBuckets: %u\n", db->hashTable->numBuckets);
    printf( "overflowBuckets: %u\n", db->overflowBuckets->numBuckets);


    /////
    Timer t;

    // key = x, y
    // val = oid
    uint32_t key[2] = { 0x00083d6c, 0x00100003 };
    uint32_t val[1] = { 0x00002387 };

    // x=540012,y=3 原大小1393539
    LINEARDB3_put( db, key, val );
    LINEARDB3_get( db, key, val );
    printf( "Key %08x, %08x\n", key[0], key[1]);
    printf( "Val %08x      \n", val[0]);


    int cnt = 0;
    LINEARDB3_Iterator dbi;
    LINEARDB3_Iterator_init( db, &dbi );
    while( LINEARDB3_Iterator_next( &dbi, key, val ) ) {
        if (val == 0) cnt++;
    }
    printf("cnt: %d\n", cnt);


    /////
    t.elapsed();

}