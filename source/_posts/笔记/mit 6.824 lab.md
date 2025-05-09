---
title: MIT 6.824 课程笔记

---

# 实验一：MapReduce

通过了除 crash test 的测试，基本的完成思路是：worker 不断 rpc 申请任务，直到 coordinator 的调度状态为 DONE。根据参数写回到的任务类型分别执行，doMapTask or doReduceTask 完成之后发送消息提醒任务已完成。coordinator 主要完成任务发布、分配 worker 请求任务、处理任务完成消息和 crash 检测功能。

crash 检测我开启新的线程，任务执行期间一直每隔一秒检查 taskHolder 所有任务当前完成的时间，如果出现超时，就会把任务状态重新打上 WAITING/MAPPED ，发布到任务通道。
❓ 程序测试出现的问题是：_检测到 crash 后，worker 没有继续请求任务，即没有从通道中认领任务_。也许是并发问题，我在 taskHolder 加上了锁，没用。可能是 data race 的问题，开了 -race 之后出现了很多 data race.可能把锁都改成 goroutine + chan 会解决并发的错误

流程如下：
![](/img/mit6.824/f959e287be2a34c022ea74bd409d0e4a.png)
理解 MapReduce 难点：

- reduce 需要等待所有的 map 任务完成，是为了数据聚合。分布式 worker 处理每个文件，使用 `ihash(key) % NReduce` 把每个 key 映射到`[0, nreduce]`范围内的中间文件中，即可以保证：**所有 worker 处理不同的文件遇到的相同 key，都会保存在相同 tmp-后缀文件中，这样可以正确统计词频。** 只需 reduce worker 选择相同尾缀的中间文件。
- coordinator 维护每个任务的状态，这是很有必要的。worker 根据 task 状态选择执行不同的函数，coordinator 依据任务状态和完成的数量决定当前的调度状态，以及出现了 crash，应该把任务状态修改重新投到通道中。状态包括：`WAITING MAP_READY MAPPING MAPPED REDUCE_READY REDUCING REDUCED`
- 并发需要加锁。

# 实验二

## 课堂笔记

### GFS

分布式目标是为了提升性能，同时机器过多可能会带来大量不定时出现的故障，因此需要有容错和监控系统。解决故障的方法是创建副本，那么又带来多个副本之间可能出现的不一致问题。

GFS 设计需要解决一些问题
❓ 文件怎么分散存储在多个服务器当中？
分割成 64MB 大小的 chunk 存储在不同机器下面，一个文件内容可能需要占用多个 chunk（gfs 设计准备 GB 级大小文件）。这个数字偏大，是为了减少 master 节点存储 chunk 信息，查询时尽量在少的 chunk 完成减少网络传输代价。

✅GFS 保证单点 master，原因是控制流和数据流分离，client 仅从 master 获取 chunk 的元数据信息，并不直接进行数据交互，这一点放在 chunkserver 与 client 中。另外，master 只持久化 file and chunk namespace、file 和 chunk id 的映射关系，至于 chunk 在哪个 server 当中并没有保存，因为可以和 chunkserver 通信得到一个 chunkserver 的所有 chunk 信息。

❓ 服务器发生故障怎么保证文件不锁坏不丢失？（**高可用问题**，在宕机之后仍能保证向外提供服务）
**_master 的高可用_**：master 和 元数据的高可用是一致的，采用主备复制的模式。master 更新/增加 namespace 或 改变映射关系首先写入日志，再将 wal 从 priamry master 发送到备用 master，直到备用 master 确认之后再更改主 master 的内存。而要识别 master 宕机需要共识算法。
**_chunk server 的高可用_**：通过 master 维持副本数量和副本之间的一致，并不遵循物理上 chunk server 主备一致性。GFS 设定只有三个 chunk 副本都保存了信息才算是写入完成，如果出现某个 chunkserver 宕机，还剩下两个副本可用。同时 master 可以在其他的 chunkserver 上新建副本，维持副本数目。

> 另外，master 需要将 chunk server 版本号保存起来，因为一旦 master 宕机，那么仅根据 chunk server 自己的版本号并不能确定哪个 server 是保存最新副本的安全 server

> [!note]- 为什么不选择所有 chunk server 中最大的版本号作为 master 确定的安全版本号？
> “当 Master 重启时，无论如何都需要与所有的 Chunk 服务器进行通信，因为 Master 需要确定哪个 Chunk 服务器存了哪个 Chunk。你可能会想到，Master 可以将所有 Chunk 服务器上的 Chunk 版本号汇总，找出里面的最大值作为最新的版本号。如果所有持有 Chunk 的服务器都响应了，那么这种方法是没有问题的。但是存在一种风险，当 Master 节点重启时，可能部分 Chunk 服务器离线或者失联或者自己也在重启，从而不能响应 Master 节点的请求。所以，Master 节点可能只能获取到持有旧副本的 Chunk 服务器的响应，而持有最新副本的 Chunk 服务器还没有完成重启，或者还是离线状态（这个时候 Master 能找到的 Chunk 最大版本明显不对）
>
> 因为 Master 从磁盘存储的数据知道 Chunk 对应的最新版本，Master 节点会整合具有最新版本 Chunk 的服务器。每个 Chunk 服务器会记住本地存储 Chunk 对应的版本号，当 Chunk 服务器向 Master 汇报时，就可以说，我有这个 Chunk 的这个版本。而 Master 节点就可以忽略哪些版本号与已知版本不匹配的 Chunk 服务器。
> ”

❓primary chunk 有什么用？
控制流的顺序由其决定，租约期限内决定其他副本写入 chunk 顺序，避免同时副本写入的顺序不对。依然还是控制流的主备关系，client 还是正常发送所有的数据给副本，只是主备关系决定了写入的顺序。

🤔GFS 读写流程
![](/img/mit6.824/ceec5785d9432faf3f9ebf3c565a1838.png)
流水线式数据传输可以节省大量的网络传输，不用再从客户端指定传输到主 chunkserver，那样的话主 chunkserver 需要给多个 replica 发送数据，网络带宽将成为性能瓶颈。**控制流分离，client 仍然需要向主 chunkserver 发送写入顺序信息**，等待所有副本接收到数据后，主 chunk server 决定顺序写入。
推荐使用追加的方式写入文件，也是 GFS 在文件写入可以接受 “重复写入数据，但是数据不能错误” 的想法。

🤔 那么一致性模型的要点是什么呢？
![](/img/mit6.824/51e6890f9b4daaf2696a633b7a0077e9.png)

{% note info %}
并发改写为什么更推荐用追加的方式？
如果是全局的改写，一次修改可能涉及到多个 chunk，GFS 并没有全局 chunk 的概念，只能保证相同 chunk 副本之间的写入是一致的（这是由数据流和控制流分离保证的，控制流决定了写入的顺序，在上面已经有介绍）；而在多个改写的 chunk 之间，并发的写操作对不同的 chunk 来自 S1 S2 S3 可能不同。**_不同 chunk 对改写的顺序不能保证相同_**
{% endnote %}  

{% note info %}
GFS 的快照保存技术
![](/img/mit6.824/ff47e22fe31d77dec98cc0a6f62fda87.png)
优点很明显：1. 减少内存占用空间，大部分 chunk 并不会修改，快照和原文件的 chunk 是公用的；2. 快速的快照机制，停止写入的时间很短暂，因为只需要将源数据拷贝给快照文件，增加引用计数即可


### GFS 一些课堂问题

> [!quote] **可不可以通过版本号来判断副本是否有之前追加的数据？**
> Robert 教授：所有的 Secondary 都有相同的版本号。版本号只会在 Master 指定一个新 Primary 时才会改变。通常只有在原 Primary 发生故障了，才会指定一个新的 Primary。所以，副本（参与写操作的 Primary 和 Secondary）都有相同的版本号，**你没法通过版本号来判断它们是否一样，或许它们就是不一样的（取决于数据追加成功与否）**。
>
> 这么做的理由是，当 Primary 回复“no”给客户端时，客户端知道写入失败了，之后客户端的 GFS 库会重新发起追加数据的请求，直到最后成功追加数据。成功了之后，追加的数据会在所有的副本中相同位置存在。在那之前，追加的数据只会在部分副本中存在。（注：挂掉的副本没有接收到数据，master 也可以通过 checksum 来判断）
> 总结：**版本号不是判断写入是否成功的工具，版本号是用来区分是否错过了换主，通过检查内容/校验等方式以及比较版本标记哪些 chunk 是陈旧的。** 😁 客户端重试，并且期望之后能正常工作。并不会立即解决错误的问题
> 如果 Secondary 服务器挂了，Master 节点可以发现并更新 Primary 和 Secondary 的集合，之后再增加版本号。但是这些都是之后才会发生

> [!quote] **如果 master 发现 primary 挂了怎么办？**
> Robert 教授：可以这么回答这个问题。在某个时间点，Master 指定了一个 Primary，之后 Master 会一直通过定期的 ping 来检查它是否还存活。因为如果它挂了，Master 需要选择一个新的 Primary。Master 发送了一些 ping 给 Primary，并且 Primary 没有回应，你可能会认为 Master 会在那个时间立刻指定一个新的 Primary。但事实是，这是一个错误的想法。为什么是一个错误的想法呢？因为可能是网络的原因导致 ping 没有成功，所以有可能 Primary 还活着，但是网络的原因导致 ping 失败了。❗ 但同时，Primary 还可以与客户端交互，如果 Master 为 Chunk 指定了一个新的 Primary，那么就会同时有两个 Primary 处理写请求，这两个 Primary 不知道彼此的存在，会分别处理不同的写请求，最终会导致有两个不同的数据拷贝。这被称为脑裂（split-brain）。
>
> 脑裂是一种非常重要的概念，我们会在之后的课程中再次介绍它（详见 6.1），它通常是由网络分区引起的。比如说，Master 无法与 Primary 通信，但是 Primary 又可以与客户端通信，这就是一种网络分区问题。网络故障是这类分布式存储系统中最难处理的问题之一。
>
> 所以，我们想要避免错误的为同一个 Chunk 指定两个 Primary 的可能性。Master 采取的方式是，**_当指定一个 Primary 时，为它分配一个租约，Primary 只在租约内有效。Master 和 Primary 都会知道并记住租约有多长，当租约过期了，Primary 会停止响应客户端请求，它会忽略或者拒绝客户端请求。_** 因此，如果 Master 不能与 Primary 通信，并且想要指定一个新的 Primary 时，Master 会等到前一个 Primary 的租约到期。这意味着，Master 什么也不会做，只是等待租约到期。租约到期之后，可以确保旧的 Primary 停止了它的角色，这时 Master 可以安全的指定一个新的 Primary 而不用担心出现这种可怕的脑裂的情况。

> [!quote] **为什么立即指定一个新的 Primary 是坏的设计？**
> 因为客户端会通过缓存提高效率，客户端会在短时间缓存 Primary 的身份信息（这样，客户端就不用每次都会向 Master 请求 Primary 信息）。即使没有缓存，也可能出现这种情况，客户端向 Master 节点查询 Primary 信息，Master 会将 Primary 信息返回，这条消息在网络中传播。之后 Master 如果发现 Primary 出现故障，并且立刻指定一个新的 Primary，同时向新的 Primary 发消息说，你是 Primary。Master 节点之后会向其他查询 Primary 的客户端返回这个新的 Primary。**而前一个 Primary 的查询还在传递过程中，前一个客户端收到的还是旧的 Primary 的信息。如果没有其他的更聪明的一些机制，前一个客户端是没办法知道收到的 Primary 已经过时了。** 如果前一个客户端执行写文件，那么就会与后来的客户端产生两个冲突的副本。

### VM-ft 课堂笔记

首先可以看[tanxinyu 的论文笔记博客](https://tanxinyu.work/vm-ft-thesis/) 对整个论文有大致的了解。Vmware-ft 论文主要为了解决两台主备物理机之间针对非确定事件的一致性同步问题，并对机器状态接受做了问题简化，只关注外部不确定事件输入（这里特别指网络数据包的到达）、定时器中断和怪异指令。他的底层思想仍然是复制状态机模型：**多个机器在相同的初始状态下，执行相同的输入，那么最后的状态一定是相同的**。需要解决的问题是，这些非确定的事件怎么依赖一种 **捕获——重放机制**以确定的顺序和位置记录下来，并精确的在备机上重放，在多台机器上达成同步的一致性状态。可以说，vm-ft是补充实际应用下，除确定性指令外实现复制状态机的解决方案。

A1：捕获—模拟中断—重放机制的工作原理
这里有两个场景，分别是***primary 的定时器中断和来自网卡的网络包数据中断***，处理略有不同：
- 定时器中断：在运行了 Primary 虚机的物理服务器上，有一个定时器，这个定时器会计时，生成定时器中断并发送给 VMM（虚拟机监控）。VMM 会**停止 Primary 虚机的指令执行，并记下当前的指令序号**，然后**在指令序号的位置插入伪造的模拟定时器中断**，并恢复 Primary 虚机的运行。之后，VMM 将指令序号和定时器中断再发送给 Backup 虚机。虽然 Backup 虚机的 VMM 也可以从自己的物理定时器接收中断，但是它并没有将这些物理定时器中断传递给 Backup 虚机的 guest 操作系统，而是直接忽略它们。**backup接收到来自primary的伪造log中断，送入操作系统中**。这样，就实现了从primary到backup的精确指令中断重放。
- 网卡中断：一个网络数据包到达了primary机器，网卡会将网络数据包拷贝给 VMM 的内存， VMM在接收到网卡中断后会停止primary机器。这时候他会同时 将primary机器上的指令、网络数据包发送给backup， backup拷贝数据包内容到内存，在相同的指令序号位置模拟一个网卡中断；网络数据包拷贝给primary内存，发送网卡中断。这就是Bounce-Buffer机制
可以看到，无论是定时器中断还是网络数据包，其本身的中断都是不确定的，vm-ft提出用这种指令序列 + 捕获中断 + 重放的方式精准在主备机器上实现同步。

A2：考虑容错机制，vm-ft怎么解决宕机的不一致？
问题背景：假设Primary生成了回复给客户端，但是之后立马崩溃了，或者因为网络不稳定，backup没有收到网络包。primary机向客户端回复后，backup成为主机却没有上次的记录。
解决这个问题方法可以是等待ack，即**直到Backup虚机确认收到了相应的Log条目，Primary虚机不允许生成任何输出**。这里的核心思想是，确保在客户端看到对于请求的响应时，Backup虚机一定也看到了对应的请求，或者说至少在Backup的VMM中缓存了这个请求。如果出现崩溃，那么主机不会收到ack，自然也不会向客户端返回响应。
然而，这样是非常消耗性能的。更通常的做法是，备机维护一个log缓冲区，收集主机的网络包并依次确认，而这似乎并没有遵循vm-ft的输出控制

A3：vm-ft 主从切换仍然需要单点控制
如果两个机器之间发生了网络故障，或者本身两台机器出现物理故障，在其中一个的视角，是：网络数据包不可达或者超时。这时候需要主从切换，向一个可信的第三方 test-and-set 求证。类似锁的功能，决定谁是主节点，用标志位0/1来标识

### Raft 算法 课堂笔记

A1：Raft算法是如何避免脑裂问题的？
从一个数学的角度，过半票决是怎么解决脑裂的？总结来看，脑裂问题主要需要解决单一性、一致性问题。
- 单一性：为了保证不同网络分区总共也只能有一台主服务器，那么就要求服务器的数量是奇数的，保证分区服务器数量不对称，从而达到要求：**在任何时候为了完成任何操作，你必须凑够过半的服务器来批准相应的操作** 。这样保证过半的前提下，奇数服务器确保一个leader
- 一致性：这更有趣，以选举为例，每一个操作对应的过半服务器，必然至少包含一个服务器存在于上一个操作的过半服务器中。也就是说，任意两组过半服务器，至少有一个服务器是重叠的。新leader依靠这个至少一台存在于旧leader的服务器，就能够知道旧 Leader 使用的任期号（term number）和任何旧 Leader 提交的操作。这是保证一致性的关键。

A2：Raft 作为一个共识算法，怎么和应用层接入？
Raft集群当中主要有两个接口一个是key-value接口，另一个是函数调用 Start 接口。key-value层用来转发客户端请求的接口，Start 函数主要执行日志复制的过程。需要注意的是，key-value 接口向 Raft 层发送请求的时候，并不是同步的，客户端可以不用等待一个请求被确认commit之后在发送下一个请求，大量的请求同时发送是常见的。直到 Start函数执行完毕后，Raft 层通过applyCh通道确认key-value已经成功执行，可以向客户端返回了。
![](/img/mit6.824/f60d24f78d7223a24d832118e73983a3.png)

A3：raft 集群当中的commit、取消、和过半哲学？
在 A1问题中，为了避免脑裂解释了过半是怎么保证一致性和单一性的。同时还有一些别的

## Require Point of Lab

### 共识算法

共识算法常被用来确保每一个节点上的状态机一定都会按相同的顺序执行相同的命令， 并且最终会处于相同的状态（复制状态机模型）。它允许其中的节点出现故障后，整个系统仍然能保持运作，也就是高可用特性。具体来说，**共识算法解决数据存储下的一致性问题，提供容错机制。**
这里不得不提到复制状态机：多个节点在相同的初始状态下，执行相同的输入，那么最后的状态一定是相同的。这就解决了副本之间的一致性问题。在 Raft 算法中，leader 通过发送 log entry 请求到所有的 follower 节点，节点按照日志记录依次执行，那么最后的状态就是一致的。

### Raft 算法

前置知识和概念：

- 节点的状态只会有 follower，candidate，leader。如果节点之间没有收到 leader 的心跳，那么就会进入 candidate 状态，向所有节点并发发送请求包和响应请求。经过一次或多次的选举，选出 leader。
- 时间按照任期进行划分，每次开启新的任期，term+1
- 通信 rpc 类型包括：请求投票（投票选举 leader）和追加条目（由 leader 发起，用来追加日志条目和心跳机制）
- 还是论文当中的核心用几个问题来拆分，分别是以下问题：

#### A1：Raft 是怎么解决多副本之间协调一致的问题？
**采用 leader 选举的方法，有多个节点当中的 leader 决定 client 数据写入其他节点的顺序（类似于上面的 GFS 的主 primary ，但不同的是 GFS 是 master 指定的，而 Raft 是节点之间自主选举产生，依靠 RPC 发送日志条目）。** follower 没有接收到心跳后，进入 candidate 状态，首先会把自己的任期 term+1，其次给自己投票，然后并发向所有的节点发送投票请求 RequestVote RPC。节点接收到投票请求，会给出一个响应，如果请求的 term >= 自己的 term，根据 ”先来先服务“ 的原则投出票。如此执行，直到有节点最先得到超过半数票，立即切换状态成为 leader，向所有其余节点发送心跳包，而其它节点收到心跳之后状态从 candidate 转换成 follower。至此，新的 leader 产生。
❓ 那么这样是不是会导致很多节点同时选举，票数分散？可能，但仍然要考虑每个节点意识到自己 ”可以成为 candidate“ 时机不同，网络的快慢，结点的分布都可能成为优势。

> _注：实际编码中用了 rand 函数，让每个节点每次选举时间是不同的_

### lab2B

#### A0：日志复制怎么保证一致性特性？
流程：

- **Leader 将日志条目追加到本地日志**，并通过 `nextIndex[]` 向各 Follower 发送。
- **Follower 接收到日志条目**，并通过 `matchIndex[]` 反馈成功复制的最大日志索引。
- **Leader 通过 `matchIndex[]` 判断日志条目是否被大多数节点复制**，如果满足法定人数，则更新 `commitIndex`，并通知所有节点提交该条目。*（peer在下一次的心跳或者append entry中leaderCommit 检查是否可以更新）*
- **Follower 更新 `commitIndex` 和 `appliedIndex`**，将已提交的日志条目应用到本地状态机。
  > [!quote] Raft 算法里面的一些结构体说明。
  >
  > - commitIndex ：表示当前已被**提交**的日志条目的索引，在 follower leader 中均有。更新是在 Leader 将日志条目复制到大多数节点之后，由 Leader 来更新。
  > - appliedIndex ：表示前已应用到本地状态机的日志条目的最大索引。leader 确定了 commit index 之后，确认超过半数就可以写入到本地状态机了，即更新。大多数情况是两者相等，如果 appliedIndex < commitIndex ，那么节点会继续应用直至相等。
  > - **_日志号和任期号同时配合才能确定一个日志_**， 避免 leader 没处理完挂掉之后，部分副本上有日志，而新选举的 leader 没有，但是 term 更大更权威，所以可以删掉那些过期的日志，毕竟也没有提交写入

#### A1：为什么需要 `rf.replicatorCond` ?
这是优化的部分，比较难懂。首先这是每一个 server 维护了条件变量的数组（每一个 server 对应一个条件变量 replicatorCond 数组，而不是一个条件变量），当 server 被 make 函数启动时，replicatorCond 是一直在循环中 wait 的，直到他成为 leader 并且检查到某一个 follower 没有跟上自己的最新日志，那么就会唤醒去执行 replicaOneround。

那么这个条件变量由谁来唤醒呢？是上层应用调用 `start`，进一步 `broadcastHearbeat()` 提醒：成为 leader 之后，复制日志的时候要给每一个 peer 提醒，`replicatorCond[peer].Siganl()` 函数派上用场。如何优化的，即使被唤醒后，可能某个 peer 的 `commitindex` 再重复发送日志的过程中并没有变化，即不满足` rf.commitindex[peer] < rf.getlastlog().index` 那么就没必要给这个 peer 也发送，节省了 RPC 通信

#### A2：日志复制的工作机制
上层应用调用 start() 对 leader 做了以下修改，后续的复制由此事件驱动：

- `append(rf.logs, newEntry)` 修改 logs 数组，从而 getlastlogIndex 也变化
- `rf.commitIndex, rf.nextIndex[rf.me] = newEntry.Index, newEntry.Index+1`
  整个过程的核心是 replicateOneRound 函数，它封装了三个核心操作：**生成请求 request--generateAppendEntryRequest；RPC 调用 follower 的 AppendEntries ；处理返回响应。** 上层调用 appendEntry 和 leader 心跳都会生成请求，这里生成 request 都用了一个函数统一，核心在于生成 entry 的时候 `copy(entries, rf.logs[nextlogIndex-firstIndex:])` 。如果是心跳包，那么 peer 节点的 nextlogIndex == leader.lastlog.index 所以 entry 空；如果是 append entry 那么 peer 节点的 nextlog.index 就会 < leader.lastlog.index 从而把每一个节点需要的日志都精准地复制下去。这样就实现了日志复制和心跳的统一。

接下来关注一下复制过程当中非常核心的几个状态：更新 leader/peer 的 commitIndex、提交到状态机、日志冲突回溯。
- leader 当客户端上层调用传递了一个指令，leader 就会把这个指令包装成一个日志添加到自己的日志数组当中，**立即更新 commitIndex**。这里可以理解成 leader 立刻收到了来自自己的日志复制请求并接受。
- peer 节点接收到 leader 的日志复制请求之后，检查合法性添加到日志数组，但这个时候并不会更新自己的 commit index，他会检查请求当中的 leader commit 如果 > 自己的 commit index 那么就会更新。所以可以说普通节点 commit index 的更新是要延迟于 leader 的。
- apply 状态： leader 节点在处理 peer 节点返回的响应**如果发现大多数节点都已经成功的复制了日志，并且自己经过了检查——即将应用的日志的任期 == 自己当前的任期**（_背后有场景_），那么就会提交到客户端上层应用。如果不满足条件就会保持 commitIndex 不动。成功唤醒应用线程，应用到状态机之后更新自己的 lastapplied。
- 普通节点同时还会另外开一个线程不断去检测自己的 commit index 和 last applied 两个指标如果一旦发现 commit index > last applied 那么就会更新并且提交于自己的状态机中
- 日志冲突回溯：这个问题是为了解决**leader 宕机导致的日志仅复制到了小部分节点上新的 leader 为了覆盖这部分旧的日志数据**， 就会在 peer 节点的 prevlogIndex prevlog.term 检查，直到找到 `request.prevlog.term == rf.log[index].term`。这个问题背后出现的场景很复杂，也是日志复制过程中的边缘场景。

#### A3：Raft的安全性保证
上面的介绍当中需要补充两点关于Raft对安全性保障的说明，首先leader节点在把日志应用到状态机的检查中，需要保证应用的log.term == leader.current.term。为什么要做这样的约束呢？ leader节点再检查到某个日志被大多数节点所复制为什么不能够直接向上层应用提交状态呢?
![](/img/mit6.824/436500e461cad813e3b489d729e0c6ad.png)
目的是为了***防止出现目前最新任期的日志主备切换，导致这种 log index相同而任期不同的日志覆盖的情况*** 。上图中如果S1 S2 S3都应用日志二，那么当时的leader就会像上层调用反应日志二已经被应用到了大多数节点的机器上。但是如果S5此时当选成了leader，就会凭借**索引相同但任期号更大的日志3**覆盖掉原来日志2节点，这样就出现了客户端和实际机器集群当中的不一致状态。所以，Raft要求leader节点要***新增一个自己的 append entry 才能够保护自己之前的日志能够提交。***

第二个安全性问题是：为什么leader单点向客户端提交状态是可行的。换言之，为什么leader向客户端返回的应用日志索引之前的所有日志，都已经确保可以提交了？
这是由 **request vote.lastlog.Index/term** 保证，每一个candidate都会***向其他所有的节点发送自己的最后日志的索引和任期号***。这就要求了，如果他能够当选leader，那么大多数的节点都认可他的最新日志。这也就避免了某一些candidate不符合大多数节点的日志状态，但是却当选leader的情况。也是raft中过半哲学能够维持的一致性体现。

#### A4：no-op 隐式提交大多数follower节点

❗ 记 debug Raft 实验 lab2B 的过程，花了大概两天白天的时间在尝试让他正常运行。最后问题都是非常细小，变量理解没有熟练用错导致的。
起：leader 发送 entry 或者心跳后，follower 接受之后逐渐出现 prevlogIndex 变小直至小于 0 的情况。
思：生成的报错是在 generateAppendEntry 提示 prevlogIndex < 0 非法，但是问题肯定在上一轮的 append 中。如果想要让 follower 的 nextindex 变小，考虑到 handleAppendEntry 在 success 的情况下是单调增的

```go
rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
rf.nextIndex[peer] = rf.matchIndex[peer] + 1
```

那么只会是 false 错误，即使是心跳也错（不合理），发现错误 match 的逻辑里面确实让 response.conflictIndex 变小，这是不对的。bug 可能就是出现在这：

1. 没有进入 handleResponse 的 success，而是 `rf.nextIndex[peer] = response.ConflictIndex + 1` 导致 nextIndex=0
2. append 里面 match 错误进入 conflictIndex 处理逻辑导致错误
   最终定位找到源头是 `if !rf.matchLog(request.PrevLogIndex, request.PrevLogTerm)` 写错

再起：成功能跑之后，遇到了 follower 没有成功复制/commit 的问题，观察测试输出

```log
2025/04/28 19:55:18 leader1 update commitIndex to 1
2025/04/28 19:55:18 peer0 nextlogIndex: 2
2025/04/28 19:55:18 peer2 nextlogIndex: 2
2025/04/28 19:55:18 leader1 send ApplyMsg to channel
    /home/whale/job/project/hw/824/src/Raft/config.go:441: server0 cmd: <nil>
    /home/whale/job/project/hw/824/src/Raft/config.go:441: server1 cmd: 100
    /home/whale/job/project/hw/824/src/Raft/config.go:441: server2 cmd: <nil>
    /home/whale/job/project/hw/824/src/Raft/config.go:530: 1/3servers commit, command: 100
```

思：前半段正确，leader 更新大多数节点的提交，peer.nextIndex 也前进了。后半段上层调用出现没有`follower.log[]`的问题，问题应该就是`peer.commitIndex` 没有推进，同时没推送到`rf.applyCh`通道中。最终找到是`advanceComitIndex`出现 if 判断问题。注意这里上层调用不仅仅是 leader 需要传递 applyMsg 到 channel，所有的 peer 更新了 commitIndex 都需要推送到 applyCh 中。
