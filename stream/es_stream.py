# -*- coding: utf-8 -*-
"""
    File Name: es_stream
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 16:10
"""
from elasticsearch import Elasticsearch
from spl_arch.stream.base_stream import BaseStream


class EsStream(BaseStream):
    """
    原始日志以{"_raw": "53,93,74,two"}的方式存入，即整条日志以_raw为字段名，整条日志作为值的方式写入ES
    """

    def __init__(self, name, host, port, indexer):
        super(EsStream, self).__init__(name)
        self.client = Elasticsearch(host=host, port=port, timeout=300)
        self.indexer = indexer

    def pull(self):
        res = self.client.search(index=self.indexer)
        return res["hits"]["hits"]  # list

    def push(self):
        pass
