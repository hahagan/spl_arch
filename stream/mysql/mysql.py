# -*- coding: utf-8 -*-
"""
    File Name: mysql
    Description: ""
    Author: Donny.fang
    Date: 2020/6/8 16:09
"""

import pymysql
from threading import Lock
import logging


class Mysql(object):
    """
    demo版本每个Mysql对象代表一个数据库连接
    可用与生产的版本，应考虑通过单例模式创建sql任务管理器，sql任务管理器管理若干数据库连接，sql操作由sql任务管理器以任务的方式进行调度管理
    """

    # _Instance = None  # 单例
    # _Lock = Lock()  # 单例创建使用锁，保证并发创建时无异常
    # _Conn = None  # 数据库连接

    # def __new__(cls, *args, **kwargs):
    #
    # mysql连接创建采用单例模式，确保mysql连接受控
    # 单例模式下多线程对mysql的操作不可并行，否则会导致读写乱序，mysql抛出异常
    # 如果未来需要使用mysql，需要增加mysql读写任务管理模块，将读写操作以任务的方式分配给连接池中的各个连接
    # 因此多线程情况下单例行为暂时舍弃
    #
    #     if cls._Instance is not None:
    #         return cls._Instance
    #
    #     with cls._Lock:
    #         if cls._Instance is None:
    #             config = {
    #                 "host": '127.0.0.1',
    #                 'port': 3306,
    #                 'password': "eisoo.com123",
    #                 'user': "root"
    #             }
    #             conn = pymysql.connect(**config)
    #             cur = conn.cursor()
    #             create_db = "CREATE DATABASE IF NOT EXISTS SPLCache"
    #             cur.execute(create_db)
    #             conn.select_db('SPLCache')
    #             cls._Conn = conn
    #
    #             cls._Instance = super().__new__(cls)
    #
    #     return cls._Instance

    def __init__(self):
        """
        非单例模式下，连接mysql并创建数据库
        即目前mysql操作每个mysql对象可以代表一个mysql连接
        """
        config = {
            "host": '127.0.0.1',
            'port': 3306,
            'user': "root"
        }
        conn = pymysql.connect(**config)
        cur = conn.cursor()
        create_db = "CREATE DATABASE IF NOT EXISTS SPLCache"
        cur.execute(create_db)
        conn.select_db('SPLCache')
        self._Conn = conn

    def cursor(self):
        cursor = self._Conn.cursor()
        return MySqlCursor(cursor)

    @property
    def conn(self):
        return self._Conn

    def __del__(self):
        try:
            self._Conn.close()
        except Exception as e:
            logging.exception(e)


class MySqlCursor(object):
    """
    包装  pymysql.cursor对象操作，添加额外的安全处理，避免使用者在使用结束后没进行释放操作等行为
    """

    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, arg=None):
        self._cursor.execute(query, arg)

    def mogrify(self, query, args=None):
        return self._cursor.mogrify(query, args)

    def fetchmany(self, size=1):
        return self._cursor.fetchmany(size)

    def fetchall(self):
        return self._cursor.fetchall()

    def __del__(self):
        try:
            self._cursor.close()
        except Exception as e:
            logging.exception(e)


if __name__ == '__main__':
    print(Mysql())
    print(Mysql())
    print(Mysql()._Conn)
    print(Mysql()._Conn)
