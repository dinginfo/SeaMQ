# SeaMQ

<img src="/SeaMQ.jpg" alt="pic" style="max-width:100%;">



SeaMQ是一个高性能，高可靠，高安全，高可扩展的消息中间件，生产者发送到SeaMQ的消息保证送到消费者端

# 横向扩展
多master，多broker设计，master不保存消息，只负责Topic的创建，消息队列的分配，用户的创建，为用户分配主题。Broker cache消息，负责消息的读写操作，在不停止整个集群的情况下动态添加broker或减少broker，不影响系统使用，增加系统吞吐量。
    

# 数据高可靠

消息保存在HDFS文件系统，消息数据默认保存3份在不同的机器，本机一份，同机架机器保存一份，不同机架的机器保存一份，消息保存成功后才返回发送成功标志，当某个机器 down机或磁盘环掉，HDFS文件系统会把down机的数据自动复制到其它机器，确保数据万无一失。


# 消息堆积能力强
消息保存在HDFS文件系统，HDFS把多个机器的磁盘组成一个超大存储池，容量可以达到数十PB，能堆积万亿级别消息。消费者端down机或消费很慢不影响生产者发送消息。

# 高并发性
一个Topic可以有多个queue, SeaMQ把多个queue均衡分配给broker，消息生产者生产的消息被均衡发送到多个broker，提高发送消息并发量。


# 高可用
SeaMQ集群有多个broker，当其中一个down机后，SeaMQ会把此机的queue分配给其它broker，提高broker的高可用性。

# 保证消息顺序，不重复
同一个queue的消息保证顺序，消息生产者发送的消息不重复

# 多租户设计
系统开始就考虑多租户设计，一个租户对应一个Domain,每个用户只能发送或消费指定的topic，不属于自己的topic不能发送消息和接收消息，保证topic的数据安全
