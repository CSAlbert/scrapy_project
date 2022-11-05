# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import redis
import pymongo
import pymysql
import json
from kafka import KafkaProducer
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class ScrapyProjectPipeline:
    def process_item(self, item, spider):
        return item


# 对形式字段做转换的Item Pipeline
class QidianHotPipeline:
    def process_item(self, item, spider):
        if item["form"] == "连载":
            item["form"] = "LZ"
        else:
            item["form"] = "WJ"
        return item


# 去除重复作者的Item Pipeline
class DuplicatesPipeline(object):
    def __init__(self):
        # 定义一个保持作者姓名的集合
        self.author_set = set()

    def process_item(self, item, spider):
        if item['author'] in self.author_set:
            # 抛弃重复的Item项
            raise DropItem("查找到重复姓名的项目：%s" % item)
        else:
            self.author_set.add(item['author'])
        return item


# 将数据保存于文本文档中的Item Pipeline
class SaveToTxtPipeline(object):
    file_name = "hot.txt"  # 文件名称
    file = None  # 文件对象

    # Spider开启时，执行打开文件操作
    def open_spider(self, spider):
        # 以追加形式打开文件
        self.file = open(self.file_name, "a", encoding="utf-8")

    # 数据处理
    def process_item(self, item, spider):
        # 获取item中的各个字段，将其连接成一个字符串
        # 字段之间用分号隔开
        # 字符串结尾要有换行符
        novel_str = item["name"] + "；" + \
                    item["author"] + "；" + \
                    item["type"] + "；" + \
                    item["form"] + "\n"
        # 将字符串写入文件中
        self.file.write(novel_str)
        return item

    # Spider 关闭时，执行关闭文件操作
    def close_spider(self, spider):
        # 关闭文件
        self.file.close()


# 将数据保存于Mysql的Item Pipeline
class MySQLPipeline(object):
    # Spider开启时，获取数据库配置信息，连接Mysql数据库
    def open_spider(self, spider):
        # 获取配置文件中的MySQL配置信息
        host = spider.settings.get("MYSQL_HOST", "localhost")
        db_name = spider.settings.get("MYSQL_DB_NAME", "scrapy")
        user = spider.settings.get("MYSQL_USER", "root")
        pwd = spider.settings.get("MYSQL_PASSWORD", "123456")

        # 连接MySQL数据库
        self.db_conn = pymysql.connect(host=host, database=db_name, user=user, password=pwd, charset="utf8")

        # 使用cursor()方式获取操作游标
        self.db_cursor = self.db_conn.cursor()

    # 将数据保存于MySQL
    def process_item(self, item, spider):
        # 获取Item中的各个字段，保存于元组中
        values = (item['name'], item['author'], item['type'], item['form'])

        # 设计插入操作的SQL语句
        sql_insert = 'insert into hot(name ,author,type,form)values (%s,%s,%s,%s)'

        # 执行SQL语句，实现插入功能
        self.db_cursor.execute(sql_insert, values)
        return item

    # Spider关闭时，执行数据库关闭工作
    def close_spider(self, spider):
        # 提交数据
        self.db_conn.commit()
        # 关闭游标
        self.db_cursor.close()
        # 关闭数据库
        self.db_conn.close()


# 将数据保存到MongoDB的Item Pipeline
class MongoDBPipeline(object):
    # Spider开启时，获取数据库配置信息，连接MongoDB数据库
    def open_spider(self, spider):
        # 获取配置文件中的MongoDB配置信息
        host = spider.settings.get("MONGODB_HOST", "localhost")
        port = spider.settings.get("MONGODB_POST", 27017)
        db_name = spider.settings.get("MONGODB_NAME", "scrapy")
        collection_name = spider.settings.get("MONGODB_COLLECTION", "qidian_hot")

        # 连接MongoDB，得到一个客户端对象
        self.db_client = pymongo.MongoClient(host=host, port=port)

        # 指定数据库，得到一个数据库对象
        self.db = self.db_client[db_name]

        # 指定集合，得到一个集合对象
        self.db_collection = self.db[collection_name]

    # 将数据存储于MongoDB数据库中
    def process_item(self, item, spider):
        # 将Item转换为字典类型（MongoDB的文档/记录-行数据，只能是由字段和值对组成的数据结构）
        item_dict = dict(item)

        # 将数据插入集合中
        self.db_collection.insert_one(item_dict)

        return item

    # Spider关闭时，执行数据库关闭工作
    def close_spider(self, spider):
        # 关闭数据库
        self.db_client.close()


# 将数据保存到Redis的Item Pipeline
class RedisPipeline(object):
    # Spider开启时，获取数据库配置信息，连接Redis数据库
    def open_spider(self, spider):
        # 获取配置文件中的Redis配置信息
        host = spider.settings.get("REDIS_HOST", "localhost")
        port = spider.settings.get("REDIS_PORT", 6379)
        db_index = spider.settings.get("REDIS_DB_INDEX", 0)
        db_psd = spider.settings.get("REDIS_PASSWORD", "Redis@1996")

        # 连接Redis，得到一个连接对象
        self.db_conn = redis.StrictRedis(host=host, port=port, db=db_index, password=db_psd)

    # 将数据存储于Redis数据库中
    def process_item(self, item, spider):
        # 将Item转换为字典类型
        item_dict = dict(item)

        # 将item_dict保存于key为novel的列表中
        self.db_conn.rpush("novel", item_dict)

        return item

    # Spider关闭时，执行数据库关闭工作
    def close_spider(self, spider):
        # 关闭数据库连接
        self.db_conn.connection_pool.disconnect()


# 将名言数据推送到Kafka
class QuoteToKafka(object):
    # # 初始化函数，一般用作建立数据库连接
    # def __init__(self):
    #     self.producer = KafkaProducer(bootstrap_servers='43.142.183.27:9092')

    # Spider开启时，获取数据库配置信息，连接Kafka
    def open_spider(self, spider):
        # 获取配置文件中的Redis配置信息
        bootstrap_servers = spider.settings.get("kafka_bootstrap_servers", "linux101:9092")

        # 连接Redis，得到一个连接对象
        self.producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf8'),
                                      bootstrap_servers=bootstrap_servers)

    # 输出数据处理方法
    def process_item(self, item, spider):
        # 将Item转换为字典类型
        item_dict = dict(item)

        # 将item_dict推送到quotes主题
        self.producer.send("quotes", item_dict)
        self.producer.flush()

    # # Spider关闭时，执行数据库关闭工作
    # def close_spider(self, spider):
    #     # 关闭数据库连接
    #     self.db_conn.connection_pool.disconnect()


# 将网咖评论明细数据保存到MongoDB
class WkOTACommentToMongoDB(object):
    # Spider开启时，获取数据库配置信息，连接MongoDB数据库
    def open_spider(self, spider):
        # 获取配置文件中的MongoDB配置信息
        host = spider.settings.get("MONGODB_HOST", "localhost")
        port = spider.settings.get("MONGODB_POST", 27017)
        db_name = spider.settings.get("MONGODB_NAME", "scrapy")
        collection_name = spider.settings.get("MONGODB_COLLECTION_WK_OTA_COMMENT", "wk_ota_comment_detail")

        # 连接MongoDB，得到一个客户端对象
        self.db_client = pymongo.MongoClient(host=host, port=port)

        # 指定数据库，得到一个数据库对象
        self.db = self.db_client[db_name]

        # 指定集合，得到一个集合对象
        self.db_collection = self.db[collection_name]

    # 将数据存储于MongoDB数据库中
    def process_item(self, item, spider):
        # 将Item转换为字典类型（MongoDB的文档/记录-行数据，只能是由字段和值对组成的数据结构）
        item_dict = dict(item)

        # 将数据插入集合中
        self.db_collection.insert_one(item_dict)

        return item

    # Spider关闭时，执行数据库关闭工作
    def close_spider(self, spider):
        # 关闭数据库
        self.db_client.close()
