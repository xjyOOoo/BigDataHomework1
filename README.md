#I.设计思路
##任务1：统计股票代码出现次数
1. Mapper：
使用StockCountMapper，解析输入文件的每一行，提取第四个字段stock,输出<股票代码,1>。
2. CombinerandReducer:
都使用StockCountReducer，接收每个股票代码对应的value,汇总每个股票代码的总出现次数，输出<股票代码,总次数>。
3. 排序:
使用SortByCountMapper和SortByCountReducer，将<股票代码,总次数>反转为<总次数,股票代码>，按总次数降序排序。

##任务2：统计高频单词
1. Mapper:
使用WordCountMapper，解析输入文件的每一行，提取第二个字段headline，去除标点符号，转换为小写并过滤停用词，最后分割成多个单词，分别输出<单词,1>。
2. Combiner and Reducer:
都使用WordCountReducer，接收每个单词对应的value，汇总每个单词的总出现次数，输出<单词,总次数>。
3. 排序:
使用SortByCountMapper和SortByCountReducerLimited，将<单词,总次数>反转为<总次数,单词>，按总次数降序排序，输出前100个。