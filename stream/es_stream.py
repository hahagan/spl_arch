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
        # res = self.client.search(index=self.indexer)
        res = {
            "took": 6,
            "timed_out": False,
            "_shards": {
                "total": 3,
                "successful": 3,
                "skipped": 0,
                "failed": 0
            },
            "hits": {
                "total": 4,
                "max_score": 1,
                "hits": [
                    {
                        "_index": "mytest",
                        "_type": "doc",
                        "_id": "oTQlf3IBadl8xYL7Er0J",
                        "_score": 1,
                        "_source": {
                            "_raw": "87,70,50,one"
                        }
                    },
                    {
                        "_index": "mytest",
                        "_type": "doc",
                        "_id": "njQjf3IBadl8xYL7tb3n",
                        "_score": 1,
                        "_source": {
                            "_raw": "53,93,74,two"
                        }
                    },
                    {
                        "_index": "mytest",
                        "_type": "doc",
                        "_id": "nzQkf3IBadl8xYL7dr0J",
                        "_score": 1,
                        "_source": {
                            "_raw": "63,79,86,three"
                        }
                    },
                    {
                        "_index": "mytest",
                        "_type": "doc",
                        "_id": "oDQkf3IBadl8xYL7yr1A",
                        "_score": 1,
                        "_source": {
                            "_raw": "80,71,53,one"
                        }
                    }
                ]
            }
        }
        return res["hits"]["hits"]  # list

    def push(self):
        pass
