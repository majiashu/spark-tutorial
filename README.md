### map与flatMap的区别
- map 是指映射, 输入输出相对应
- flatMap 返回了一个迭代器
```text
输入：
txt文件 如下
one spark hive hadoop
two scala HBase phoenix

map输出：
textFile.map(line=>line.split("//s"))结果
Array(Array(one, spark, hive, hadoop),Array(two, scala, HBase, phoenix))

flatMap输出：
textFile.flatMap(line=>line.split("//s"))结果
Array(one, spark, hive, hadoop, two, scala, HBase, phoenix)
```
---

