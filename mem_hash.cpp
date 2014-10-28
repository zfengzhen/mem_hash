#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <stdarg.h>
#include "mem_hash.h"

namespace mem_hash {
#define LOG(fmt, args...) Log_("[%s][%d][%s] : " fmt, \
				__FILE__, \
				__LINE__, \
				__FUNCTION__, ##args);

#define GET_BLOCK_USED_FLAG(x) 	(x &  0x1)
#define SET_BLOCK_USED_FLAG(x) 	(x = x | 0x1)
#define CLR_BLOCK_USED_FLAG(x)	(x = x & ~0x1)

MemHash::MemHash()
{
	bucket_time     = 0;
	bucket_len      = 0;
	max_node        = 0;
	max_block       = 0;		
	total_size      = 0;
	mem_base        = NULL;
	head_           = NULL;
	node_           = NULL;
	block_          = NULL;
	foreach_key_pos = 0;
	mlock_open_flag = OPEN_MLOCK;
	msync_freq      = 0;
	msync_flag      = 0;
	data_change     = 0;
	data_store_time = 0;

	memset(bucket, 0, sizeof(uint32_t) * MAX_BUCKET_SIZE);
	//crc32
	Crc32CreateTable(crc32_table);
}

MemHash::~MemHash()
{
	int ret = 0;
	close(log_fd);
	ret = munmap(mem_base, total_size);
	if (ret == -1) {
		printf("MemHash::~MemHash munmap error[%d]. %s\n", 
				errno, strerror(errno));
	}
}

int MemHash::Init(const char* name,
		  time_t    data_store_time,
		  int       mlock_open_flag,
		  int       msync_freq,
		  int       msync_flag,
		  uint32_t  bucket_time,
		  uint32_t  bucket_len,
		  uint32_t  max_block)
{
	//打开日志文件
	log_fd = open("run.log", O_CREAT | O_RDWR | O_APPEND, 0666);
	if (log_fd == -1) {
		printf("MemHash::Init open log error[%d]. %s\n", 
				errno, strerror(errno));
		exit(-1);
	}

	//超时设置
	if (data_store_time <= 0)
		this->data_store_time = 0;
	else
		this->data_store_time = data_store_time;

	//mmap相关设置
	this->mlock_open_flag = mlock_open_flag;
	if (msync_freq > 0)
		this->msync_freq = msync_freq;
	else
		this->msync_freq = 0;

	if (msync_flag != MS_ASYNC || msync_flag != MS_SYNC)
		this->msync_flag = MS_ASYNC;
	else
		this->msync_flag = msync_flag;

	int fd = open(name, O_RDWR, 0666);
	if (fd == -1) 
		InitNewMemHash(name, bucket_time, bucket_len, max_block);
	else 
		InitOldMemHash(fd,   bucket_time, bucket_len, max_block);

	return 0;
}

void MemHash::InitNewMemHash(const char* name,
		  	    uint32_t  bucket_time,
		  	    uint32_t  bucket_len,
		  	    uint32_t  max_block)
{
	LOG("MemHash::InitNewMemHash  Create New MemHash.\n");
	int ret = 0;

	int fd = open(name, O_RDWR | O_CREAT, 0666);	

	if (fd == -1) {
		printf("MemHash::InitNewMemHash open error[%d]. %s\n", 
				errno, strerror(errno));
		exit(-1);
	}

	//初始化阶数、阶长度以及bucket数组
	BucketInit(bucket_time, bucket_len);
	//根据阶数、阶长度初始化max_node
	NodeInit();
	//初始化max_block
	BlockInit(max_block);
	//初始化total_size
	TotalSizeInit();

	ret = ftruncate(fd, total_size);
	if (ret == -1) {
		printf("MemHash::InitNewMemHash ftruncate error[%d]. %s\n",
				errno, strerror(errno));
		exit(-1);
	} 

	mem_base = (char *)mmap(NULL, total_size,
				PROT_READ | PROT_WRITE,
				MAP_SHARED, fd, 0); 
	if (mem_base == MAP_FAILED) {
		printf("MemHash::InitNewMemHash mmap error[%d]. %s\n",
				errno, strerror(errno));
		exit(-1);
	}

	if (mlock_open_flag == OPEN_MLOCK) {	
		ret = mlock(mem_base, total_size);
		if (ret == -1) {
			printf("MemHash::InitNewMemHash mlock error[%d]. %s\n",
					errno, strerror(errno));
			exit(-1);
		} 
	}

	MemInitNew();

	close(fd);
	return ;
}

int MemHash::Meta(const char* name,
		  uint32_t& bucket_time,
		  uint32_t& bucket_len,
		  uint32_t& max_block)
{
	int fd = open(name, O_RDWR, 0666);

	if (fd == -1) {
		printf("MemHash::Meta open error[%d]. %s\n", 
				errno, strerror(errno));
		return -1;
	}
	
	int ret = 0;
	struct mem_head tmp_head;
	ret = pread(fd , &tmp_head, 
			sizeof(struct mem_head),
			sizeof(struct mem_barrier));
	close(fd);

	if (ret == -1) {
		printf("MemHash::Meta pread error[%d]. %s\n", 
				errno, strerror(errno));
		return -2;
	}

	uint32_t crc32_check = 0;

	//crc32头部效验
	crc32_check = Crc32Compute((char *)&tmp_head.head_info_,
				   sizeof(tmp_head.head_info_));
	if (crc32_check != tmp_head.crc32_head_info) {
		printf("MemHash::Meta crc32 error.\n"); 
		return -3;
	}

	bucket_time = tmp_head.head_info_.bucket_time;
	bucket_len  = tmp_head.head_info_.bucket_len;
	max_block   = tmp_head.head_info_.max_block;

	return 0;
}

void MemHash::InitOldMemHash(int fd,
			    uint32_t bucket_time,
			    uint32_t bucket_len,
			    uint32_t max_block)
{
	LOG("MemHash::InitOldMemHash  Using Old MemHash.\n");
	int ret = 0;

	//初始化阶数、阶长度以及bucket数组
	BucketInit(bucket_time, bucket_len);
	//根据阶数、阶长度初始化max_node
	NodeInit();
	//初始化max_block
	BlockInit(max_block);
	//初始化total_size
	TotalSizeInit();

	struct stat tmp_stat;
	ret = fstat(fd, &tmp_stat);
	if (ret == -1) {
		printf("MemHash::InitOldMemHash fstat error[%d]. %s\n",
				errno, strerror(errno));
		exit(-1);
	} 
	
	//效验MemHash文件大小
	if ((uint32_t)tmp_stat.st_size != total_size) {
		printf("MemHash::InitOldMemHash error. \
			stat.st_size != total_size\n");
		exit(-1);
	}
	
	mem_base = (char *)mmap(NULL, total_size,
				PROT_READ | PROT_WRITE,
				MAP_SHARED, fd, 0); 
	if (mem_base == MAP_FAILED) {
		printf("MemHash::InitOldMemHash mmap error[%d]. %s\n",
				errno, strerror(errno));
		exit(-1);
	}

	if (mlock_open_flag == OPEN_MLOCK) {
		ret = mlock(mem_base, total_size);
		if (ret == -1) {
			printf("MemHash::InitOldMemHash mlock error[%d]. %s\n",
					errno, strerror(errno));
			exit(-1);
		} 
	}

	MemInitOld();
	close(fd);
	
	return ;
}

void MemHash::BucketInit(uint32_t  bucket_time,
	       		 uint32_t  bucket_len)
{
	if (bucket_time > MAX_BUCKET_SIZE) {
		printf("MemHash::BucketInit error. \
		        bucket_time > MAX_BUCKET_SIZE[%u] \n", 
			MAX_BUCKET_SIZE);
		exit(-1);
	}

	this->bucket_time = bucket_time;
	this->bucket_len  = bucket_len;

	int ret = 0;
	//产出质数，返回成功产生的质数个数
	ret = GeneratePrimes(bucket, bucket_len, bucket_time);
	
	if (ret != (int)bucket_time) {
		printf("MemHash::BucketInit error. "
		       "GeneratePrimes < bucket_time [%u] \n", 
			bucket_time);
		exit(-1);
	}

	return ;
}

void MemHash::NodeInit() 
{
	int node_count = 0;
	
	for (uint32_t i = 0; i < bucket_time; i++) {
		node_count += bucket[i];
	}

	this->max_node = node_count;	

	return ;
}

void MemHash::BlockInit(uint32_t max_block)
{
	this->max_block = max_block;

	return ;
}

void MemHash::TotalSizeInit()
{
	//---|barrier|head|barrier|node zone|barrier|block zone|barrier|---
	total_size = sizeof(struct mem_barrier) * 1         +
		     sizeof(struct mem_head)    * 1         +
		     sizeof(struct mem_barrier) * 1         +
		     sizeof(struct mem_node)    * max_node  +
		     sizeof(struct mem_barrier) * 1         +
		     sizeof(struct mem_block)   * max_block + 
		     sizeof(struct mem_barrier) * 1;         

	return ;
}

void MemHash::MemInitNew()
{
	//---|barrier|head|barrier|node zone|barrier|block zone|barrier|---
	char *p = mem_base;

	struct mem_barrier tmp_barrier;
	memcpy(tmp_barrier.barrier, "MEMHASHZ", 8);

	//barrier
	memcpy(p, &tmp_barrier, sizeof(struct mem_barrier));
	p += sizeof(struct mem_barrier);

	//head
	head_ = (struct mem_head *)p;	
	memset(head_, 0, sizeof(struct mem_head));
	head_->head_info_.bucket_time = bucket_time;
	head_->head_info_.bucket_len  = bucket_len;
	head_->head_info_.max_block   = max_block;
	//计算crc32用于下次使用时效验
	head_->crc32_head_info = Crc32Compute((char *)(&head_->head_info_),
					      sizeof(head_->head_info_));
	head_->free_block_pos   = 0;
	head_->node_used        = 0;
	head_->block_used       = 0;

	//barrier
	p += sizeof(struct mem_head);
	memcpy(p, &tmp_barrier, sizeof(struct mem_barrier));
	p += sizeof(struct mem_barrier);

	//node zone
	node_ = (struct mem_node *)p;
	struct mem_node *tmp_node = node_;
	for (uint32_t i = 0; i < max_node; i++) {
		memset(tmp_node, 0, sizeof(struct mem_node));
		tmp_node->pos = -1;
		tmp_node++;
	}		

	//barrier
	p += sizeof(struct mem_node) * max_node;
	memcpy(p, &tmp_barrier, sizeof(struct mem_barrier));
	p += sizeof(struct mem_barrier);

	//block zone
	block_ = (struct mem_block *)p;
	struct mem_block *tmp_block = block_;
	for (uint32_t i = 0; i < max_block; i++) {
		memset(tmp_block, 0, sizeof(struct mem_block));
		tmp_block->pos = i + 1;
		tmp_block++;
	}

	//barrier
	p += sizeof(struct mem_block) * max_block;
	memcpy(p, &tmp_barrier, sizeof(struct mem_barrier));

	return ;
}

void MemHash::MemInitOld()
{
	char *p = mem_base;

	//效验barrier
	CheckBarrier(p);
	p += sizeof(struct mem_barrier);

	//效验head
	head_ = (struct mem_head *)p;	
	CheckHead();
	p += sizeof(struct mem_head);

	//效验barrier
	CheckBarrier(p);
	p += sizeof(struct mem_barrier);

	node_ = (struct mem_node *)p;
	p += sizeof(struct mem_node) * max_node;

	//效验barrier
	CheckBarrier(p);
	p += sizeof(struct mem_barrier);

	block_ = (struct mem_block *)p;
	p += sizeof(struct mem_block) * max_block;

	//效验barrier
	CheckBarrier(p);

	//清除所有BLOCK使用标志位
	ClearBlockUsedFlag();
	//效验NODE和BLOCK节点
	CheckNodeBlock();
	//恢复BLOCK节点
	RecoverBlock();

	return ;
}

void MemHash::CheckBarrier(char* barrier)
{
	int ret = strncmp(barrier, "MEMHASHZ", 8);
	if (ret != 0) {
		printf("MemHash::CheckBarrier  error.\n"); 
		exit(-1);
	}
}

void MemHash::CheckHead()
{
	uint32_t crc32_check = 0;

	//crc32头部效验
	crc32_check = Crc32Compute((char *)&head_->head_info_,
				   sizeof(head_->head_info_));
	if (crc32_check != head_->crc32_head_info) {
		printf("MemHash::CheckHead  error.\n"); 
		exit(-1);
	}
}

void MemHash::ClearBlockUsedFlag()
{
	//初始化head中BLOCK节点使用情况
	head_->free_block_pos = -1;
	head_->node_used      =  0;
	head_->block_used     =  0;

	//重置BLOCK节点使用标志位
	struct mem_block *tmp_block = block_;
	for (uint32_t i = 0; i < max_block; i++) {
		tmp_block = GetBlock(i);
		CLR_BLOCK_USED_FLAG(tmp_block->flag);
	}
	
	return ;
}

void MemHash::CheckNodeBlock()
{
	struct mem_node *tmp_node = node_;
	struct mem_block *tmp_block = block_;

	for (uint32_t i = 0; i < max_node; i++) {
		tmp_node = node_ + i;
		//遍历所有的非空NODE节点
		if (tmp_node->key != 0) {
			//该节点使用的BLOCK节点的个数
			uint32_t nbu = GetNodeBlockUsed(tmp_node->size);
			//该节点使用的最后一个BLOCK节点的偏移量
			uint32_t lbu = GetLastBlockUsed(tmp_node->size);

			if (nbu > MAX_BLOCK_NUM) {
				tmp_node-> key = 0;
				tmp_node->crc32 = 0;
				tmp_node->tval = 0;
				tmp_node->size = 0;
				tmp_node->pos = -1;
				LOG("MemHash::CheckNode error. "
				    "node block_used > MAX_BLOCK_NUM[%lu]",
				    MAX_BLOCK_NUM);
				continue;
			}
			
			tmp_block = GetBlock(tmp_node->pos); 
			if (tmp_block == NULL) {
				tmp_node-> key = 0;
				tmp_node->crc32 = 0;
				tmp_node->tval = 0;
				tmp_node->size = 0;
				tmp_node->pos = -1;
				LOG("MemHash::CheckNode error. " 
				    "node.pos < 0 or " 
				    "node.pos >= max_block[%lu]",
				    max_block);
				continue;
			}

			uint32_t crc32buf = 0;
			//前n-1个BLOCK节点crc32叠加
			for (uint32_t j = 0; j < nbu - 1; j++) {
				crc32buf = Crc32Append(crc32buf,
						tmp_block->data,
						BLOCK_DATA_SIZE);
				tmp_block = GetBlock(tmp_block->pos);
				if (tmp_block == NULL) {
					tmp_node-> key = 0;
					tmp_node->crc32 = 0;
					tmp_node->tval = 0;
					tmp_node->size = 0;
					tmp_node->pos = -1;
					LOG("MemHash::CheckNode error. " 
					    "node.pos < 0 or " 
					    "node.pos >= max_block[%lu]",
					    max_block);
					break;
				}
			} 

			if (tmp_block == NULL)
				continue;
			
			if (tmp_block->pos != -1) {
				tmp_node-> key = 0;
				tmp_node->crc32 = 0;
				tmp_node->tval = 0;
				tmp_node->size = 0;
				tmp_node->pos = -1;
				LOG("MemHash::CheckNode error. "
				    "last block pos != -1");
				continue;
			}

			//最后一个BLOCK节点crc32叠加
			crc32buf = Crc32Append(crc32buf,
					tmp_block->data,
					lbu);
			
			//效验crc32
			if (crc32buf != tmp_node->crc32) {
				tmp_node-> key = 0;
				tmp_node->crc32 = 0;
				tmp_node->tval = 0;
				tmp_node->size = 0;
				tmp_node->pos = -1;
				LOG("MemHash::CheckNode error. " 
				    "node.crc32 check error.");
				continue;
			}

			//效验成功，将该NODE节点下的所有BLOCK节点标记为已使用
			tmp_block = GetBlock(tmp_node->pos); 
			for (uint32_t j = 0; j < nbu; j++) {
				SET_BLOCK_USED_FLAG(tmp_block->flag);
				head_->block_used++;
				tmp_block = GetBlock(tmp_block->pos);
			}
			
			head_->node_used++;
		}
		
	}
	
	LOG("[CheckNodeBlock][finish]");
	LOG("[STAT][free_block_pos(%d)]"
			"[node_used(%u)]"
			"[block_used(%u)]", 
			head_->free_block_pos,
			head_->node_used,
			head_->block_used);
	return ;
}

void MemHash::RecoverBlock()
{
	//根据BLOCK的标记位重建BLOCK空闲队列
	struct mem_block *pre_block = block_;
	struct mem_block *tmp_block = block_;

	//查找第一个空闲的BLOCK节点
	for (uint32_t i = 0; i < max_block; i++) {
		pre_block = GetBlock(i);		
		if (GET_BLOCK_USED_FLAG(pre_block->flag) != 1) {
			head_->free_block_pos = i;
			break;
		}
	}
	
	//重建空闲BLOCK队列
	for (uint32_t i = head_->free_block_pos + 1; i < max_block; i++) {
		tmp_block = GetBlock(i);		
		if (GET_BLOCK_USED_FLAG(tmp_block->flag) != 1) {
			pre_block->pos = i;
			pre_block = tmp_block;
		}
	}

	//最后一个空闲BLOCK节点指向POS置为-1
	pre_block->pos = -1;

	return ;
}

struct mem_node* MemHash::GetNode(uint64_t key)
{
	struct mem_node *tmp_node = node_;
	uint32_t base_pos = 0;

	for (uint32_t i = 0; i < bucket_time; i++) {
		if (i > 0) base_pos += bucket[i-1];

		tmp_node = node_ + base_pos + (key % bucket[i]);
		if (tmp_node->key == key) {
			return tmp_node;
		}
	}

	return NULL;
}

struct mem_block* MemHash::GetBlock(int32_t pos)
{
	if (pos >= 0 && pos < (int32_t)max_block)
		return block_ + pos;
	else
		return NULL;
}

int MemHash::Del(uint64_t key)
{
	struct mem_node *tmp_node = GetNode(key);
	if (tmp_node == NULL) { 
		LOG("[Del][%lu][failed] not find the key.", key);
		return -1;
	}

	//该节点使用的BLOCK节点的个数
	uint32_t nbu = GetNodeBlockUsed(tmp_node->size);
	struct mem_block *tmp_block = GetBlock(tmp_node->pos);
	tmp_node->key = 0;
	head_->node_used--;

	//处理前n-1个BLOCK节点
	for (uint32_t i = 0; i < nbu - 1; i++) {
		CLR_BLOCK_USED_FLAG(tmp_block->flag);	
		tmp_block = GetBlock(tmp_block->pos);
	}
	
	//处理最后一个BLOCK节点
	CLR_BLOCK_USED_FLAG(tmp_block->flag);	
	//增加BLOCK空闲队列
	tmp_block->pos = head_->free_block_pos;
	head_->free_block_pos = tmp_node->pos;
	head_->block_used -= nbu;
	
	//处理NODE节点
	tmp_node->crc32 = 0;
	tmp_node->tval = 0;
	tmp_node->size = 0;
	tmp_node->pos = -1;

	data_change++;
	if ((msync_freq != 0) && (data_change > msync_freq)) {
		data_change = 0;
		MemSync(msync_flag);
	}

	/*LOG("[Del][%lu][success]", key);
	LOG("[STAT][free_block_pos(%d)]"
			"[node_used(%u)]"
			"[block_used(%u)]", 
			head_->free_block_pos,
			head_->node_used,
			head_->block_used);*/

	return 0;
}

void MemHash::DelForInner(uint64_t key)
{
	struct mem_node *tmp_node = GetNode(key);
	if (tmp_node == NULL) { 
		return ;
	}

	//该节点使用的BLOCK节点的个数
	uint32_t nbu = GetNodeBlockUsed(tmp_node->size);
	struct mem_block *tmp_block = GetBlock(tmp_node->pos);
	tmp_node->key = 0;
	head_->node_used--;

	//处理前n-1个BLOCK节点
	for (uint32_t i = 0; i < nbu - 1; i++) {
		CLR_BLOCK_USED_FLAG(tmp_block->flag);	
		tmp_block = GetBlock(tmp_block->pos);
	}
	
	//处理最后一个BLOCK节点
	CLR_BLOCK_USED_FLAG(tmp_block->flag);	
	//增加BLOCK空闲队列
	tmp_block->pos = head_->free_block_pos;
	head_->free_block_pos = tmp_node->pos;
	head_->block_used -= nbu;
	
	//处理NODE节点
	tmp_node->crc32 = 0;
	tmp_node->tval = 0;
	tmp_node->size = 0;
	tmp_node->pos = -1;

}


int MemHash::Set(uint64_t key, const char* data, int len)
{
	//防止key为0的情况
	if (key == 0)
		return -100;

	const char *start_data = data;
	//该数据要使用的BLOCK节点的个数
	uint32_t nbu = GetNodeBlockUsed(len);
	//该数据要使用的最后一个BLOCK节点的偏移量
	uint32_t lbu = GetLastBlockUsed(len);
	
	if (nbu > MAX_BLOCK_NUM) { 	
		LOG("[Set][%lu][failed] blocks > MAX_BLOCK_NUM[%u]",
		     key, MAX_BLOCK_NUM);
		return -1;
	}

	if (nbu > max_block - head_->block_used) { 	
		LOG("[Set][%lu][failed] blocks > free blocks num [%u]",
		     key, max_block - head_->block_used);
		return -2;
	}
	
	struct mem_node *tmp_node = NULL;
	uint32_t base_pos = 0;

	//调用Del调用
	DelForInner(key);

	time_t cur_time = time(0);
	for (uint32_t i = 0; i < bucket_time; i++) {
		if (i > 0) base_pos += bucket[i-1];

		tmp_node = node_ + base_pos + (key % bucket[i]);

		//数据超时
		if (tmp_node->key != 0 && data_store_time != 0) {
			time_t interval = cur_time - tmp_node->tval;
			if (interval > data_store_time) {
				DelForInner(tmp_node->key);
			}
		}

		//查找空闲的NODE节点
		if (tmp_node->key == 0) {
			head_->node_used++;
			int32_t pre_free_pos = head_->free_block_pos;
			struct mem_block *tmp_block = GetBlock(pre_free_pos);
			//处理前n-1个BLOCK节点
			for (uint32_t j = 0; j < nbu - 1; j++) {
				head_->block_used++;
				SET_BLOCK_USED_FLAG(tmp_block->flag);
				memcpy(tmp_block->data, data, BLOCK_DATA_SIZE);
				data += BLOCK_DATA_SIZE;
				tmp_block = GetBlock(tmp_block->pos);
			}
			
			//处理最后一个BLOCK节点
			head_->block_used++;
			SET_BLOCK_USED_FLAG(tmp_block->flag);
			memcpy(tmp_block->data, data, lbu);
			head_->free_block_pos = tmp_block->pos;
			tmp_block->pos  = -1;

			tmp_node->pos   = pre_free_pos;
			tmp_node->crc32 = Crc32Compute(start_data, len);
			tmp_node->tval  = time(0);		
			tmp_node->size  = len;
			tmp_node->key   = key;

			data_change++;
			if ((msync_freq != 0) && (data_change > msync_freq)) {
				data_change = 0;
				MemSync(msync_flag);
			}

			/*LOG("[Set][%lu][success]", key);
			LOG("[STAT][free_block_pos(%d)]"
					"[node_used(%u)]"
					"[block_used(%u)]", 
					head_->free_block_pos,
					head_->node_used,
					head_->block_used);*/
		
			return 0;
		} 
	}

	LOG("[Set][%lu][failed] no empty node", key);

	return -3;
}


int MemHash::IsExist(uint64_t key)
{
	//防止key为0的情况
	if (key == 0)
		return -100;

	struct mem_node *tmp_node = GetNode(key);
	if (tmp_node == NULL) 
		return 0;

	//数据超时
	if (data_store_time != 0) { 
		time_t interval = time(0) - tmp_node->tval;
		if (interval > data_store_time) {
			DelForInner(key);
			return 0;
		}       
	} 

	return 1;
}

int MemHash::Get(uint64_t key, char* data, int max_len, int& data_len)
{
	//防止key为0的情况
	if (key == 0)
		return -100;

	struct mem_node *tmp_node = GetNode(key);
	if (tmp_node == NULL) { 
		LOG("[Get][%lu][failed] not find the key.", key);
		return -1;
	}

	if (tmp_node->size > (uint32_t)max_len) {
		LOG("[Get][%lu][failed] node.size > buffer len.", key);
		return -2;
	}

	//数据超时
	if (data_store_time != 0) {
		time_t interval = time(0) - tmp_node->tval;
		if (interval > data_store_time) {
			DelForInner(key);
			LOG("[Get][%lu][failed] interval[%lu] > data_store_time[%lu]",
					interval, data_store_time, key);
			return -3;
		}
	}

	//该节点使用的BLOCK节点的个数
	uint32_t nbu = GetNodeBlockUsed(tmp_node->size);
	//该节点使用的最后一个BLOCK节点的偏移量
	uint32_t lbu = GetLastBlockUsed(tmp_node->size);

	struct mem_block *tmp_block = GetBlock(tmp_node->pos);
	char *tmp_buf = data;
	//处理前n-1个BLOCK节点
	for (uint32_t j = 0; j < nbu - 1; j++) {
		memcpy(tmp_buf, tmp_block->data, BLOCK_DATA_SIZE);
		tmp_buf += BLOCK_DATA_SIZE;
		tmp_block = GetBlock(tmp_block->pos);
	}	

	//处理最后一个BLOCK节点
	memcpy(tmp_buf, tmp_block->data, lbu);
	data_len = tmp_node->size;

	/*LOG("[Get][%lu][success]", key);
	LOG("[STAT][free_block_pos(%d)]"
			"[node_used(%u)]"
			"[block_used(%u)]", 
			head_->free_block_pos,
			head_->node_used,
			head_->block_used);*/

	return 0;

}

int MemHash::Append(uint64_t key, const char* data, int len)
{
	//防止key为0的情况
	if (key == 0)
		return -100;

	const char *start_data = data;
	struct mem_node *tmp_node = GetNode(key);
	if (tmp_node == NULL) { 
		LOG("[Append][%lu] node not exist call [Set]", key);
		return  Set(key, data, len);
	}

	//数据超时
	time_t cur_time = time(0);
	if (data_store_time != 0) {
		time_t interval = cur_time - tmp_node->tval;
		if (interval > data_store_time) {
			DelForInner(key);
			return  Set(key, data, len);
		}
	}

	//append之后总共使用的BLOCK个数
	uint32_t total_nbu = GetNodeBlockUsed((tmp_node->size + len));
	
	if (total_nbu > MAX_BLOCK_NUM) { 	
		LOG("[Append][%lu][failed] blocks > MAX_BLOCK_NUM[%u]",
		     key, MAX_BLOCK_NUM);
		return -1;
	}

	//该节点现在使用的BLOCK个数
	uint32_t nbu = GetNodeBlockUsed(tmp_node->size);
	//该节点最后一个BLOCK偏移量
	uint32_t lbu = GetLastBlockUsed(tmp_node->size);

	//寻找最后一个BLOCK节点
	struct mem_block *tmp_block = GetBlock(tmp_node->pos);
	for (uint32_t i = 0; i < nbu - 1; i++) {
		tmp_block = GetBlock(tmp_block->pos);
	}
	struct mem_block *last_block = tmp_block;

	//新增数据在最后一个BLOCK节点可以容纳下
	if ((uint32_t)len <= BLOCK_DATA_SIZE - lbu) {
		memcpy(last_block->data + lbu, data, len);
		tmp_node->size += len;
		tmp_node->crc32 = Crc32Append(tmp_node->crc32,
					last_block->data + lbu,
					len);	

		/*LOG("[Append][%lu][success]", key);
		LOG("[STAT][free_block_pos(%d)]"
				"[node_used(%u)]"
				"[block_used(%u)]", 
				head_->free_block_pos,
				head_->node_used,
				head_->block_used);*/

		data_change++;
		if ((msync_freq != 0) && (data_change > msync_freq)) {
			data_change = 0;
			MemSync(msync_flag);
		}

		return 0;
	} else {
		//新增数据在最后一个BLOCK节点容纳不下了
		//需要新增BLOCK的数据量
		uint32_t left = len - (BLOCK_DATA_SIZE - lbu);
		//剩下的数据需要的BLOCK节点数目
		uint32_t left_nbu = GetNodeBlockUsed(left);
		//剩下的数据的最后一个BLOCK节点的偏移量
		uint32_t left_lbu = GetLastBlockUsed(left);
		
		if (left_nbu > max_block - head_->block_used) {
			LOG("[Append][%lu][failed] blocks > free blocks num [%u]",
			     key, max_block - head_->block_used);
			return -2;
		}

		//填充最后一个BLOCK节点
		memcpy(last_block->data + lbu, data, BLOCK_DATA_SIZE - lbu);
		data += BLOCK_DATA_SIZE - lbu;

		//处理剩余数据量的前n-1个BLOCK节点
		int32_t pre_free_pos = head_->free_block_pos;
		tmp_block = GetBlock(pre_free_pos);
		for (uint32_t j = 0; j < left_nbu - 1; j++) {
			head_->block_used++;
			SET_BLOCK_USED_FLAG(tmp_block->flag);
			memcpy(tmp_block->data, data, BLOCK_DATA_SIZE);
			data += BLOCK_DATA_SIZE;
			tmp_block = GetBlock(tmp_block->pos);
		}

		//处理剩余数据量的最后一个BLOCK节点
		head_->block_used++;
		SET_BLOCK_USED_FLAG(tmp_block->flag);
		memcpy(tmp_block->data, data, left_lbu);

		tmp_node->crc32 = Crc32Append(tmp_node->crc32,
				start_data,
				len);	
		head_->free_block_pos = tmp_block->pos;
		tmp_block->pos  = -1;
		last_block->pos = pre_free_pos;
		tmp_node->size += len;

		data_change++;
		if ((msync_freq != 0) && (data_change > msync_freq)) {
			data_change = 0;
			MemSync(msync_flag);
		}

		/*LOG("[Append][%lu][success]", key);
		LOG("[STAT][free_block_pos(%d)]"
				"[node_used(%u)]"
				"[block_used(%u)]", 
				head_->free_block_pos,
				head_->node_used,
				head_->block_used);*/

		return 0;
	}
}

int MemHash::ForEachKey(uint64_t& key)
{
	if (key == 0)
		foreach_key_pos = 0;
	
	struct mem_node *tmp_node = NULL;
	uint32_t i = 0;
	for (i = foreach_key_pos; i < max_node; i++) {
		tmp_node = node_ + i;
		//遍历所有的非空NODE节点
		if (tmp_node->key != 0) {
			key = tmp_node->key;
			foreach_key_pos = i + 1;
			return 1;
		}
	}
	
	foreach_key_pos = 0;

	return 0;
}

void MemHash::Stat(uint32_t& node_used, uint32_t& block_used)
{
	node_used  = head_->node_used * 100 / max_node;
	block_used = head_->block_used * 100 / max_block;
}

void MemHash::MemSync(int flags)
{
	msync(mem_base, total_size, flags);
}

inline uint32_t MemHash::GetNodeBlockUsed(uint32_t size)
{
	if (size % BLOCK_DATA_SIZE == 0)
		return size / BLOCK_DATA_SIZE;
	else
		return size / BLOCK_DATA_SIZE + 1;
}

inline uint32_t MemHash::GetLastBlockUsed(uint32_t size)
{
	if (size % BLOCK_DATA_SIZE == 0)
		return BLOCK_DATA_SIZE;
	else
		return size % BLOCK_DATA_SIZE;
}

int MemHash::GeneratePrimes(uint32_t* primes,
			    uint32_t  max,
		   	    uint32_t  num)
{
	uint32_t i = 0, j = 0;

	for (i = max; i > 1; i--) {
		if (IsPrime(i)) {
			primes[j] = i;
			j++;
		}

		if (j == num) return j;
	}

	return j;
}

int MemHash::IsPrime(uint32_t value)
{
	uint32_t square = (uint32_t)sqrt(value);
	uint32_t i;

	for(i = 2; i <= square; i++) {
		if(value % i == 0)
			return 0;
	}

	return 1;
}


void MemHash::Crc32CreateTable(uint32_t* crc32_table)
{
	for (uint32_t i = 0; i < 256; i++) {
		crc32_table[i] = Crc32GetSummedPloys(i);
	}       

	return; 
}

uint32_t MemHash::Crc32GetSummedPloys(uint32_t top_byte)
{
	uint32_t summed_ploys = top_byte << 24;

	for (int i = 0; i < 8; i++) {
		uint32_t top_bit = (summed_ploys >> 31) && 0x01;
		summed_ploys = summed_ploys << 1;

		if (top_bit == 0x01)
			summed_ploys = summed_ploys ^ PLOY; 
	}       

	return summed_ploys;
}

uint32_t MemHash::Crc32Compute(const char* data, int len)
{
	uint32_t reg = 0;

	for(int i = 0; i < len; i++) {
		reg = (reg << 8) ^ crc32_table[data[i] ^ ((reg >> 24) && 0xFF)];
	}       

	return reg;
}

uint32_t MemHash::Crc32Append(uint32_t crc32, const char* data, int len)
{
	uint32_t reg = crc32;

	for(int i = 0; i < len; i++) {
		reg = (reg << 8) ^ crc32_table[data[i] ^ ((reg >> 24) && 0xFF)];
	}       

	return reg;
}

void MemHash::Log_(const char* fmt, ...)
{
	time_t now = time(NULL);
	struct tm tmm;
	localtime_r(&now, &tmm);
	int buf_len = 0;
	
	buf_len += snprintf(log_buffer_ + buf_len, 32,
			   "[%04d-%02d-%02d %02d:%02d:%02d]", 
			   tmm.tm_year + 1900,
			   tmm.tm_mon + 1,
			   tmm.tm_mday,
			   tmm.tm_hour,
			   tmm.tm_min,
			   tmm.tm_sec);

	va_list ap;
	va_start(ap, fmt);
	buf_len += vsnprintf(log_buffer_ + buf_len,
			     MAX_LOG_LEN - buf_len - 1, fmt, ap);

	if (buf_len >= MAX_LOG_LEN - 1) 
		buf_len = MAX_LOG_LEN - 1;
	*(log_buffer_ + buf_len) = '\n';

	va_end(ap);

	write(log_fd, log_buffer_, buf_len + 1);
}

}//namespace mem_hash 
