# -*- coding: utf-8 -*-
"""
    File Name: mysql_stream
    Description: ""
    Author: Donny.fang
    Date: 2020/6/8 16:06
"""

from spl_arch.stream.base_stream import BaseStream
from spl_arch.stream.mysql.mysql import Mysql
from spl_arch.stream.stream_exception import StreamFinishException
import json
from threading import Condition

StreamCondition = Condition


class MysqlStream(BaseStream):
    """
    每个MysqlStream对象对应数据库中的一张表
    在创建对应流时完成对表的创建，在MysqlStream对象被销毁时销毁对应的数据库表

    """
    def __init__(self, name, size=1):
        super().__init__(name)
        mysql = Mysql()
        self._mysql = mysql
        self._capacity = size
        self._conn = mysql.conn
        self._table = name
        self._cur = mysql.cursor()
        self._cur_read = mysql.cursor()
        self._create_table()
        self._condition = StreamCondition()
        self._length = 0
        self._offset = 0
        self._finish_in = False

    def _create_table(self):
        """
        表结构中id为自增ID
        type表明在序列化和反序列化中需要的一些判断依据，这里表示events的类型，未来也许回增加新属性进行丰富
        events数据类型暂定为文本，未来也许考虑使用二进制类型
        :return:
        """
        create_table = """CREATE TABLE {0}(
                id INT AUTO_INCREMENT,
                type CHAR(80),
                events MEDIUMTEXT ,
                PRIMARY KEY(id)
                )""".format(self._table)
        self._cur.execute(create_table)
        self._conn.commit()

    def _delete_table(self):
        del_table = "DROP TABLE {0}".format(self._table)
        self._cur.execute(del_table)
        self._conn.commit()

    def pull(self):
        """
        通过offset记录队列偏移量，每次pull获取一个events，即数据库中的一行
        目前通过offset记录偏移量，即offset对应events的ID
        pull不清除数据库中已读数据，而是在对象销毁时销毁存储的数据，未来可以考虑在pull后销毁已读取数据，满足数据队列定义，需要mysql任务管理器支持
        同步机制通过condition锁保证，当数据库数据不足时进入休眠
        :return: events
        """
        self._condition.acquire()
        if self._length < 1:
            self._condition.wait()
        select_cmd = self._cur_read.mogrify("SELECT type, events FROM {0} LIMIT 1 OFFSET %s".format(self._table),
                                            self._offset)
        self._cur_read.execute(select_cmd)
        result = self._cur_read.fetchall()
        event = self.loads(result)

        # 删除操作需要mysql任务管理器支持
        # delete_cmd = self._cur_read.mogrify("DELETE FROM {0} WHERE id < %s".format(self._table), self._offset)
        # delete_cmd = self._cur_read.execute(delete_cmd)
        # self._cur_read.execute(delete_cmd)
        # self._conn.commit()

        if isinstance(event, StreamFinishException):
            raise event
        self._offset += 1
        self._length -= 1
        self._condition.notify()
        self._condition.release()
        return event

    def push(self, events):
        """
        通过offset记录队列偏移量，每次push插入一个events，即数据库中的一行，id自增长
        同步机制通过condition锁保证，当数据队列已满时进入休眠，等待消费者pull数据后唤醒
        :param events:
        :return:
        """
        if self.finish_in:
            raise StreamFinishException("finish")

        self._condition.acquire()
        if self._length > self._capacity:
            self._condition.wait()

        events_type, events = self.dumps(events)
        insert_cmd = self._cur.mogrify('INSERT INTO {0} (type, events) VALUES (%s, %s)'.format(self._table),
                                       (events_type, events))
        self._cur.execute(insert_cmd)
        self._conn.commit()
        self._length += 1
        self._condition.notify()
        self._condition.release()

    @property
    def finish_in(self):
        return self._finish_in

    @finish_in.setter
    def finish_in(self, v):
        self.push(StreamFinishException("finish"))
        self._finish_in = True

    @staticmethod
    def dumps(events):
        # 将数据序列化为数据库可存储格式，暂时只支持字典，字符串或可json化数据类型
        # 完善版本中，需要对events事件数据载体进行定义，使得各种序列化转换器在实现时对events有统一的认知
        # 没使用python的pickle包进行序列化和反序列化，是因为其与python语言强相关

        if isinstance(events, StreamFinishException):
            return "StreamFinishException", ''
        elif isinstance(events, dict) or isinstance(events, list):
            return "dict", json.dumps(events)
        elif isinstance(events, str):
            return "str", events
        elif isinstance(events, object):
            e = Exception("The MysqlStream does not support object writing temporarily !!!")
            raise e

    @staticmethod
    def loads(events):
        # 将从数据库获取的数据进行反序列化
        # 完善版本中，需要对events事件数据载体进行定义，使得各种序列化转换器在实现时对events有统一的认知
        # 没使用python的pickle包进行序列化和反序列化，是因为其与python语言强相关

        for event_type, event in events:
            if event_type == 'StreamFinishException':
                return StreamFinishException("finish")
            elif event_type == 'dict' or event_type == 'list':
                return json.loads(event)
            elif event_type == 'str':
                return event

    # def __del__(self):
    #     self._delete_table()
    #     pass

    def clean(self):
        self._delete_table()


if __name__ == '__main__':
    items = [
        "test",
        {"dict": 123},
        StreamFinishException("finish")
    ]
    s0 = MysqlStream("test", 3)
    for i in items:
        s0.push(i)

    for i in items:
        print(i, s0.pull())
