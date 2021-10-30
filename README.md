# 191870294-朱云佳-作业5

**说明：1st jar是第一题的jar包，2nd jar是第二题的jar包**

## 0.Intellij环境配置

### Gradle or Maven?

- 在班级群公布教程之前，曾经是直接在`file->project structure`内直接导入所需要的包。但是之后与同学交流后，发现用gradle 或maven打jar包较为方便，于是调整

- gradle方面，参考了https://cloud.tencent.com/developer/article/1435044，试图创建gradle项目--但是初始创建的gradle项目并没有自行生成src文件夹（至今困惑，不知道当时为何会出现这个问题）

- 也参考了maven项目的创建教程--但苦于pom.xml的配置文件时常报错，即使install和刷新之后这些报错仍然没能消失
- 班级群教程出现之后，同学大多采用maven管理依赖包，遂转移到maven。参考https://zhuanlan.zhihu.com/p/89617163教程，将hadoop-core这个远古包注释掉，解决了一直困扰的配置文件报错问题
- 以上对于环境的探索，就持续了整整一周😥...尝试屡屡愈挫

### 最终配置情况

最终的项目结构如下：<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030171101208.png" alt="image-20211030171101208" style="zoom:50%;" /><img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030174114532.png" alt="image-20211030174114532" style="zoom:50%;" />

（右侧的output是中间结果，最终结果记录在final_output中）

上述项目结构中input文件夹是存放shakespeare的所有.txt文件的（仅用于本地intellij调试，到linux hdfs环境下会改成hdfs中的路径）

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030172124639.png" alt="image-20211030172124639" style="zoom:50%;" />

只有WordCount一个类用于实现功能，resources文件夹中存放了标点和停词文件，还有记录文件名的txt，这主要是为了方便打成jar包之后利用xx.class.getClassLoader().getResource()接口快速调取文件

### 其他说明

值得一提的是pom.xml的此处（完整版直接查看文件）：<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030171316147.png" alt="image-20211030171316147" style="zoom:50%;" />

为了导出jar时包含plugin，参考教程加入了以上这一段，无论修改version为3.2.0或3.3.0（教程3.2.0，本机hadoop 3.3.0）都显示红色报错。但可以正常在intellij中运行，也可以打成jar包在分布式环境中运行。

## 1.解题思路

因为涉及到调整后排序，所以整体思路是启动两个job：第一个进行去除标点、停词、忽略大小写和wordcount计数，第二个专门做排序。第二个job的input路径是第一个job的输出路径

去标点延续课件中parseSkipFile的思路--读入命令行参数punctuation.txt，忽略大小写--用`tmpword.toLowerCase()`变小写然后写入,去除数字--直接用正则表达式搞定，如下所示：

![image-20211030173156187](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030173156187.png)

第一个job的map基本上就完成了上述三件事（当然还过滤了单词长度，并为第一、第二题不同的处理做铺垫，下面会说）

第一个job的reduce是简单计数，第一题和第二题的差别主要体现在第二个job

### 第一题

对于第一题，统计全部作品的单词频数，map中会仅仅以`context.write(new Text(word),one)`写入，不标记作品名称。第二个job中只需对第一个job得到的output进行统计，生成最终的final_output

**关于排序**

参照教程，自定义myclass类

```java
public static class myclass
            implements WritableComparable<myclass>{
        public int x;
        public String y;
        public int getX(){
            return x;
        }
        public String getY(){
            return y;
        }
        public void readFields(DataInput in) throws IOException{
            x = in.readInt();
            y = in.readUTF();
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(x);
            out.writeUTF(y);
        }
        public int compareTo(myclass p) {
            if (this.x > p.x) {
                return -1;
            } else if (this.x < p.x) {
                return 1;
            } else {
                if (this.getY().compareTo(p.getY()) < 0) {
                    return -1;
                } else if (this.getY().compareTo(p.getY()) > 0) {
                    return 1;
                } else {
                    return 0;
                }

            }
        }
    }
```

第二个job中map输出的key类型是myclass，其中x和y属性分别存储出现频次和单词。通过自定义compareTo函数，无需reverse类即可将单词按出现频次--字典序进行排列

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030174818821.png" alt="image-20211030174818821" style="zoom:50%;" />

在最后的reduce输出上，只输出前100个，所以做如下的设计

![image-20211030175316226](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030175316226.png)

title为boolean变量，判断是否已经写入标题，其余定义sortcounter进行计数--当输出达到100个则停止

### 第二题

第二问的与第一问的不同之处在于，在第一个job写入时，需要加入文件名属性：

![image-20211030175600946](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030175600946.png)

在处理第一个job得到的结果时，需要依据输出属性，对首个"-"后面的文件名称进行解析。定义WordCountPartioner对单词进行分区

![image-20211030175706643](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030175706643.png)

这里用到的filename是读取的文件名list，回顾setup函数：![image-20211030175929652](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030175929652.png)

分区完毕后进入reduce：仍然像partitioner中一样，解析得到need_word（真正需要统计的单词）![image-20211030180031765](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030180031765.png)

这里定义了cur_process_num（目前遍历的单词位于哪一个文件）和should_process_num（应该输出的单词位于哪一个文件），当两者保持一致时才会输出。这是基于我在debug时的一个发现：![image-20211030180342725](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030180342725.png)

分区后，对文件单词的统计似乎是逐一进行的（通过println序号判断），由此产生了上面的想法

reduce过后写入结果，两题的解答到此也就完成了！

## 2.实验结果

### 本地

本地intellij实验的结果位于result文件夹，以下选取截图：

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030180551116.png" alt="image-20211030180551116" style="zoom:50%;" /><img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030180604419.png" alt="image-20211030180604419" style="zoom:50%;" />

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030180648956.png" alt="image-20211030180648956" style="zoom: 50%;" /><img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030180704420.png" alt="image-20211030180704420" style="zoom:50%;" />

### 分布式环境

#### 第一题

![image-20211030170209938](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030170209938.png)

![image-20211030170221516](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030170221516.png)

![image-20211030170406481](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030170406481.png)

#### 第二题

![image-20211030164852824](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030164852824.png)

![image-20211030164805941](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030164805941.png)

正确的！与本地相符，泪目！

## 3.实验遇到的问题及解决方法

### 本地实验遇到的问题

- <img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211025172911804.png" alt="image-20211025172911804" style="zoom:50%;" />

  配置intellij环境的问题--上面已提及，此处不多赘述，耗费了接近一周的时间调试环境，惨痛教训！

- ![image-20211030181330743](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030181330743.png)

  我的log4j开始没有.properties文件，每次warning，按照网上教程https://www.jianshu.com/p/ccafda45bcea配置过后，变成一条条的细致记录输出（所以debug还是主要依靠println，目前这个问题应该是暂时没有接近--也没有发现遇到相近问题的同学）

- 开始发现分区过后，只能写入part-r-00000这一个文件的前100，其他统统为空

  思索良久--推测是100的个数限制导致的，随便写入了一些内容，确实验证了想法

  <img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211028154126187.png" alt="image-20211028154126187" style="zoom:50%;" />

  解决办法就是进行partitioner输出实验，发现逐个文件计数的原理后，设计成到100个就停--强行切换到下个文件![image-20211030181940126](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030181940126.png)

  

### 分布式遇到的问题

- 运行jar的时候，多次报错---已经检查了路径名称，和实验2是类似的操作

  ![image-20211029084519385](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211029084519385.png)

  解决方法：增加了主类名称，已解决问题

- 实现读取stop-word-list，原因是我在IDE中写死了这个参数，只有以下是命令行传入的![image-20211030182055411](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030182055411.png)

  解决方法：通过xx.class.getClassLoader().getResource()接口快速调取文件，同时把要用的文件放到resources文件夹下

- 在运行时又爆出的错误：

  ![image-20211030125826579](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030125826579.png)

  且发现此时中间文件output已经可以显示！

  ![image-20211030130607772](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030130607772.png)

  解决方法：

  ![image-20211030152714329](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030152714329.png)

## 4.可能的改进之处

- 了解到部分同学采用的是提高排序函数的功能，实现在一个文件中写入所有分作品的词频统计，我觉得十分值得尝试--避免了partitioner分区，也就不会生成40个统计文件，极大地节约了hdfs资源（在运行的时候还爆出了如下错误，怀疑是资源不足所致，最后修改了yarn-site.xml文件）![image-20211030182536519](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211030182536519.png)
- 此次实验的排序算法可以改进，曾经看到有的教程提出用reversemapper类或treemap算法实现，但最后没能一一尝试
- 在文件读取方面，虽然最后采用了getResource()方法解决了问题（思考到几乎最后时刻...），但有同学是采用了命令行读取参数的方法，更加灵活--应该反思，改进读取文件的方式（因为最后代码主体已经完成了，就不太敢对setup等处读取的方式做修改了）

## 总结

本次作业是发下来就开始研究的--前前后后做了有2周时间，现在甚至于要卡ddl才能完成😖，暂时得出如下的体会吧：

- 最先开始探索总是不易的，因为可以参考的不多，要一个人摸索--和同学的有效交流能够避免走一个人的弯路
- 要提升资料搜集能力--这也是解决问题能力的一部分（发现一周后用bing搜比之前搜到了更加满意的答案）
- 需要投入整块的时间思考问题，否则不容易有思路

接下来的策略应该要有所调整了。



















