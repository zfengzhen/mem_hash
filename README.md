![](https://github.com/zfengzhen/Blog/blob/master/img/mem_hash_arch.png)
### hash冲突解决方式：  
多阶hash，可以hash bucket_time次，如果都冲突了，操作失败。  
### 文件结构检查：   
1、检查barrier   
2、检查head，通过crc32检查head_info重要区域(该文件结构的bucket_time、bucket_len、max_block)  
3、检查node zone.通过对非0 key的node节点与其对应的block节点进行crc32完整性检查，判断数据是否是完整的。   
### 内存映射机制：   
采用mmap对文件映射到内存中，并采用mlock进行锁定   
### 内存落地机制：   
1、落地依赖于操作系统msync机制   
2、业务侧可以调用MemSync进行同步或者异步的落地（建议采用异步落地）    
3、初始化时，可以设定多少次数据写入时，程序自动调用MemSync进行异步的落地（设为0时，取消自动调用MemSync）   
### 数据过期机制：  
node节点记录数据最新修改时间，在初始化的时候，业务自定义数据过期时间，请求到达时根据当前时间判断数据是否过期（设为0时，取消数据过期机制）   
### 性能数据   
msync频率: 0 依赖操作系统落地  
<table>
    <tr>
        <td>[数据库大小]\[MS_ASYNC msync频率]</td>
        <td>0</td>
        <td>1</td>
        <td>100</td>
        <td>1000</td>
        <td>10000</td>
    </tr>
    <tr>
        <td>512B</td>
        <td>76923</td>
        <td>3333</td>
        <td>5524</td>
        <td>26315</td>
        <td>66666</td>
    </tr>
    <tr>
        <td>768B</td>
        <td>62500</td>
        <td>3289</td>
        <td>5434</td>
        <td>24390</td>
        <td>55555</td>
    </tr>
    <tr>
        <td>1024B</td>
        <td>55555</td>
        <td>3278</td>
        <td>5376</td>
        <td>21739</td>
        <td>50000</td>
    </tr>
    <tr>
        <td>1280B</td>
        <td>76923</td>
        <td>3076</td>
        <td>5291</td>
        <td>22222</td>
        <td>45454</td>
    </tr>
    <tr>
        <td>5120B</td>
        <td>18518</td>
        <td>2923</td>
        <td>4484</td>
        <td>12820</td>
        <td>18181</td>
    </tr>
    <tr>
        <td>5376B</td>
        <td>18181</td>
        <td>2915</td>
        <td>4366</td>
        <td>12500</td>
        <td>17241</td>
    </tr>
</table>
