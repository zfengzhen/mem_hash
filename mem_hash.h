#include <stdint.h>
#include <time.h>
#include <sys/mman.h>

namespace mem_hash {
//crc32 多项式
const uint32_t PLOY            = 0x04C11DB7;
//支持的最大阶数
const uint32_t MAX_BUCKET_SIZE = 200; 
//单一BLOCK节点的容量
const uint32_t BLOCK_DATA_SIZE = 512;
//每个key对应的value存储最大的BLOCK个数
const uint32_t MAX_BLOCK_NUM   = 20;
//LOG 日志缓冲区最大长度
const int32_t  MAX_LOG_LEN     = 4096;
//mlock开关
const int      OPEN_MLOCK      = 1;
const int      CLOSE_MLOCK     = 0;

//多阶HASH阶数、每阶的长度以及最大BLOCK的个数
struct head_info {
	uint32_t bucket_time;
	uint32_t bucket_len;
	uint32_t max_block;
};

//头部固定信息
struct mem_head  {
	uint32_t crc32_head_info;
	struct   head_info head_info_;
	int32_t  free_block_pos;
	uint32_t node_used;
	uint32_t block_used;
};

//NODE节点
struct mem_node  {
	uint64_t key;
	time_t   tval;
	uint32_t size;
	uint32_t crc32;
	int32_t  pos;
};

//BLOCK节点
struct mem_block {
	uint32_t flag;
	int32_t  pos;
	char     data[BLOCK_DATA_SIZE];
};

//结构保护区
struct mem_barrier {
	char     barrier[8];
};

class MemHash {
public:
	MemHash();
	~MemHash();
	//初始化函数，调用Set、Get、Del、Append之前必须初始化
	int Init(const char*    name,
		    time_t          data_store_time,
		    int             mlock_open_flag,
		    int             msync_freq, 
		    int             msync_flag, 
		    uint32_t        bucket_time,
		    uint32_t        bucket_len,
		    uint32_t        max_block);

	int Meta(const char*    name,
		    uint32_t&       bucket_time,
		    uint32_t&       bucket_len,
		    uint32_t&       max_block);

	int Set(uint64_t        key,
		    const char*     data,
		    int             len);

	int Get(uint64_t        key,
		    char*           data,
		    int             max_len,
		    int&            data_len);

	int IsExist(uint64_t    key);

	int Del    (uint64_t    key);

	int Append (uint64_t    key,
		    const char*     data,
		    int             len);

	//遍历key ， 传入key为0，重头开始遍历，否则继续上一次遍历
	int ForEachKey(uint64_t& key);
	void Stat(uint32_t& node_used_perct, uint32_t& block_used_perct);
	void MemSync(int flags = MS_ASYNC);
	
private:
	MemHash(MemHash &rhs);
	MemHash& operator=(MemHash& rhs);

	//初始化一块新的MemHash
	void InitNewMemHash(const char* name,
		  	   uint32_t  bucket_time,
		  	   uint32_t  bucket_len,
		  	   uint32_t  max_block);
	//初始化旧的MemHash
	void InitOldMemHash(int fd,
			   uint32_t  bucket_time,
			   uint32_t  bucket_len,
			   uint32_t  max_block);

	//Set中的Del操作
	void DelForInner(uint64_t    key);
	//初始化bucket数组
	void BucketInit(uint32_t  bucket_time,
		        uint32_t  bucket_len);
	//初始化max_node
	void NodeInit(); 
	//初始化max_block
	void BlockInit(uint32_t max_block);
	//初始化MemHash整体大小total_size
	void TotalSizeInit();
	//初始化mmap新文件的内存布局
	void MemInitNew();
	//初始化mmap旧文件的内存布局
	void MemInitOld();

	//检查结构保护区数据
	void CheckBarrier(char* barrier);
	//检查头部固定结构
	void CheckHead();
	//清除所有BLOCK节点的使用标记位
	void ClearBlockUsedFlag();
	//检查NODE节点和BLOCK节点的一致性
	void CheckNodeBlock();
	//恢复BLOCK节点
	void RecoverBlock();
	//根据key获取该key的node节点指针
	struct mem_node* GetNode(uint64_t key);
	//根据pos获取BLOCK节点的指针
	inline struct mem_block* GetBlock(int32_t pos);
	//根据SIZE获取要使用BLOCK的个数
	inline uint32_t GetNodeBlockUsed(uint32_t size);
	//根据SIZE获取要使用的BLOCK最后一个节点的偏移量
	inline uint32_t GetLastBlockUsed(uint32_t size);

	//-----primes相关
	//质数产生
	int GeneratePrimes(uint32_t* primes,
			   uint32_t  max,
			   uint32_t  num);
	//判断是否是质数
	int IsPrime(uint32_t value);

	//-----MemHash数据结构
	//质数数组
	uint32_t bucket[MAX_BUCKET_SIZE];
	//阶数
	uint32_t bucket_time;
	//每阶长度
	uint32_t bucket_len;
	//NODE节点的个数
	uint32_t max_node;
	//BLOCK节点的个数
	uint32_t max_block;
	//MemHash的大小
	size_t   total_size;
	//MemHash在内存中的mmap指针
	char  *mem_base;
	//HEAD区域开始指针
	struct mem_head* head_;
	//NODE区域开始指针
	struct mem_node* node_;
	//BLOCK区域开始指针
	struct mem_block* block_;
	//mlock控制开关
	int mlock_open_flag;
	//msync频率
	int msync_freq;
	//msync标志（异步或者同步）
	int msync_flag;
	//数据变更次数
	int data_change;
	//ForEachKey开始位置
	uint32_t foreach_key_pos;
	//超时机制（数据存在时间）
	time_t data_store_time;

	//-----crc32相关
	void Crc32CreateTable(uint32_t* table);
	uint32_t Crc32GetSummedPloys(uint32_t top_byte);
	uint32_t Crc32Compute(const char* data, int len);
	uint32_t Crc32Append(uint32_t crc32, const char* data, int len);
	uint32_t crc32_table[256];	

	//-----log相关
	int  log_fd;
	char log_buffer_[MAX_LOG_LEN]; 
	void Log_(const char *fmt, ...);
};

}
