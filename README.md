# 动手撸各种分布式模式下的分布式机器学习算法:



目前主流的分布式架构包括spark，参数服务器和tensorflow：

1.基于mapreduce模型的spark-mllib，采用数据分布式+同步的模式，缺点是对异步和模型分布式不支持，但是社区完善。

<div align=center><img src="https://user-images.githubusercontent.com/45705519/125217759-0775f280-e2f4-11eb-83cd-f24de27b460b.png" width="600" height="300" /></div>

2.基于参数服务器的Multiverso，既可实现数据分布式，也可实现模型分布式，同时支持异步和同步，也可实现大规模的参数更新。

<div align=center><img src="https://user-images.githubusercontent.com/45705519/125217790-1c528600-e2f4-11eb-8d6a-89ca04562005.png" width="600" height="500" /></div>

3.基于数据流图的tensorflow，可以和1，2结合组成复杂的分布式机器学习网络。

<div align=center><img src="https://user-images.githubusercontent.com/45705519/125218464-a3ecc480-e2f5-11eb-871e-d6fb8074752f.png" width="600" height="300" /></div>

4.3种模式的区别：

<div align=center><img src="https://user-images.githubusercontent.com/45705519/125216804-d8f71800-e2f1-11eb-8965-5f81c3591e0b.png" width="800" height="400" /></div>

5.各种框架对比图, angel是腾讯开源的参数服务器框架，spark是mapreduce流派的代表，tensorflow和pytorch是数据流图的代表。

<div align=center><img src="https://user-images.githubusercontent.com/45705519/125265174-29449900-e337-11eb-9393-915b879999d3.png" width="600" height="400" /></div>



参考：《分布式机器学习：算法，理论与实践》刘铁岩<br>
最近更新日期为：2022/1<br><br>
**拒绝白嫖，欢迎star！**<br><br>
笔者组建了个AIOPS技术交流的群，群友遍布硅谷，新加坡，腾讯，阿里，浙大等等，欢迎志同道合的朋友与我联系加入！<img width="90" height="90" align=center src="https://user-images.githubusercontent.com/45705519/147529773-5474a194-b323-4f34-b5c9-a46442afa68f.png"/>
