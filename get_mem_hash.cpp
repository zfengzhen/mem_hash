#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "mem_hash.h"

using namespace fergus;

int main (int argc, char *argv[])
{
	if (argc != 3) { 
		printf("command format error.\n");
		return -1;
	}       
	char mem_hash_name[256];
        strncpy(mem_hash_name, argv[1], 256);

	uint64_t key = strtoul(argv[2], 0, 10);
	printf("key = %lu\n", key);

	int ret = 0;
	MemHash *mem = new MemHash();
	
	if (mem == NULL) {
		printf("new MemHash() error.\n");
		return -2;
	}

	uint32_t bucket_time;
	uint32_t bucket_len;
	uint32_t max_block;

	ret = mem->Meta(mem_hash_name, bucket_time, bucket_len, max_block);
	if (ret != 0) {
		printf("mem_hash_name not existed.\n");
		return -3;
	}
	printf("bucket_time = %u, bucket_len = %u, max_block = %u\n", bucket_time, bucket_len, max_block);

	mem->Init(mem_hash_name, MAP_PRIVATE, fergus::CLOSE_MLOCK, 10, bucket_time, bucket_len, max_block);

	char buf[10240];
	memset(buf, 0, 10240);
	int len = 0;
	ret = mem->Get(key, buf, 10240, len); 
	if (ret != 0) {
		printf("[Get][%lu] error.\n", key);
		return -3;
	}

	printf("buf : %s\n", buf);

	delete mem;
	return 0;
}
