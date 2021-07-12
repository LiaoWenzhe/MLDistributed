# distributed_ML


各种分布式模式下的分布式机器学习算法实现代码:

目前主流的分布式架构包括：

1.基于mapreduce模型的spark-mllib，采用数据分布式+同步的模式，缺点是对异步和模型分布式不支持，但是社区完善。
![image](https://user-images.githubusercontent.com/45705519/125217759-0775f280-e2f4-11eb-83cd-f24de27b460b.png)

2.基于参数服务器的Multiverso，既可实现数据分布式，也可实现模型分布式，同时支持异步和同步，也可实现大规模的参数更新。
![image](https://user-images.githubusercontent.com/45705519/125217790-1c528600-e2f4-11eb-8d6a-89ca04562005.png)

3.基于数据流图的tensorflow，可以和1，2结合组成复杂的分布式机器学习网络。
![image](https://user-images.githubusercontent.com/45705519/125217871-49069d80-e2f4-11eb-8329-79f63d656744.png)





![image](https://user-images.githubusercontent.com/45705519/125216804-d8f71800-e2f1-11eb-8965-5f81c3591e0b.png)

