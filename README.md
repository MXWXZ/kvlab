 # Zookeeper config
 基于openjdk11环境
 1. 首先创建zookeeper用户，下载zookeeper，解压后修改 `zoo.cfg` 文件，由于需要支持docker文件内容采用环境变量的方法，这样就能在创建docker时动态修改了。
 2. 主要参数为 `ZOO_SERVERS` ，格式是 `server.x=host:2888:3888` ，配置多个即可实现多节点
 3. 修改 `myid` 文件为zookeeper用户id
 4. 执行 `zkServer.sh` 启动zookeeper

docker compose中建立了3个zookeeper节点在不同docker中，host分别为 `zoo1` `zoo2` `zoo3`

# 通信协议
gRPC

# Server端
特性：
- 多节点服务端
- master节点自恢复
- 一致性hash

## 环境变量
- `zooaddr`: zookeeper地址
- `serverip`：指定ip，默认为第一个非自环地址
- `serverport`：RPC端口，默认1926

## Zookeeper结构
- `/server/lock`：server锁
- `/server/top`：最大服务端计数
- `/server/[num]`：服务端IP
  - 特例 `/server/0` 为master节点

## 注册机制
1. 拿server锁，创建ID值为 `/server/top` 值的节点，并使top值+1，若不存在master节点，切换本节点至master节点
2. slave节点监听 `/server/0`，不存在时抢server锁，先抢到的创建成为新的master节点，自恢复
3. master节点监听data节点修改，维护data节点状态

## 一致性hash
data节点管理采用一致性哈希算法，并且每个节点有2个虚拟节点用于提高平衡性，哈希算法采用crc32

# Data端
特性：
- 多节点Data端
- 

## 环境变量
- `zooaddr`: zookeeper地址
- `serverip`：指定ip，默认为第一个非自环地址
- `serverport`：RPC端口，默认1926
- `name`：节点名，必须唯一

## Zookeeper结构
- `/data/lock`：data锁

## 注册机制
1. 拿data锁，创建ID值为 `/data/[name]/top` 值的节点，并使top值+1