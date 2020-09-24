# -*- coding: utf-8 -*-
import pickle
import threading
import time
import datetime
from pykafka import KafkaClient
from queue import Queue

# kafka 配置
kafka_config = {}
kafka_config['get_data_topic'] = 'get_data_topic'
kafka_config['output_data_topic'] = 'output_data_topic'
kafka_config['group'] = 'kafka_group'
kafka_config['kafka_servers'] = '127.0.0.1:9093'

# 创建临时队列
kafka_queue = Queue(5000)


class DispatcherProducer(threading.Thread):
    """
    kafka生产者，负责发送消息到kafka
    """

    def __init__(self):
        super(DispatcherProducer, self).__init__()
        self.producer = None
        self.quit = 0

    def kafka_producer(self):
        # 生产者producer方法
        while True:
            try:
                client = KafkaClient(hosts=kafka_config['kafka_servers'])
                topic = client.topics[kafka_config['get_data_topic']]
                producer = topic.get_producer(linger_ms=100)  # 100ms延迟发送，默认5000ms
                return producer
            except Exception as e:
                time.sleep(5)
                continue

    def send_messages(self, row):
        message = pickle.dumps(row)  # 序列化数据
        try:
            self.producer.produce(message)  # 发送
        except Exception as e:
            time.sleep(0.1)

    def start(self):
        while not kafka_queue.empty():
            row_data = kafka_queue.get()
            self.send_messages(row_data)

    def stop(self):
        self.quit = 1


class DispatcherConsumer(threading.Thread):
    """
    kafka消费者，负责重kafka将数据读取出来
    """

    def __init__(self):
        super(DispatcherConsumer, self).__init__()
        self.consumer = None
        self.row_list = []
        self.quit = 0

    def kafka_consumer(self):
        # 消费者consumer方法
        while True:
            try:
                client = KafkaClient(hosts=kafka_config['kafka_servers'])
                topic = client.topics[kafka_config['output_data_topic']]
                consumer = topic.get_simple_consumer(
                    consumer_group=kafka_config['group'],  # 定义一个topic下的分组, 同一个分组下不能重复消费
                    auto_commit_enable=True,  # 消费者的offset将在后台周期性的提交
                    auto_commit_interval_ms=1,
                    consumer_id=kafka_config['group'],
                    consumer_timeout_ms=1000,
                    # auto_offset_reset=OffsetType.LATEST,  # 在consumer_group存在的情况下，设置此变量，表示从最新的开始取
                )
                return consumer
            except Exception as e:
                time.sleep(5)
                continue

    def tran_data(self, data):
        # data = {
        #     "app_package_name": "com.google.app",
        #     "app_type": 100,
        #     "app_uuid": "6e1343ff1942828",
        #     "auth_passed": True,
        #     "client": {
        #         "dev_manufacturer": "vivo",
        #         "dev_model": "vivo 1612",
        #         "user_agent": ""
        #     },
        #     "country": "ID",
        #     "latitude": "120.3399",
        #     "log_time": "2020-08-15 00:00:02",
        #     "longtitude": "30.1211",
        #     "os_lang": "in_ID",
        #     "remote_addr": "110.139.149.102",
        #     "user_id": "8371792",
        #     "version": "3.3.1"
        # }
        new_data = data
        new_data['common'] = {}
        new_data['location'] = {}
        if data['version']:
            new_data['common']['version'] = data['version']
        if data['latitude']:
            new_data['location']['lat'] = float(data['latitude'])
        if data['longtitude']:
            new_data['location']['lng'] = float(data['longtitude'])
        if data['log_time']:
            log_time = datetime.datetime.timestamp(datetime.datetime.strptime(data['log_time'], "%Y-%m-%d %H:%M:%S"))
            new_data['timestampMs'] = int(log_time) * 1000
        if data['app_type'] == 100:
            if data['client']['user_agent'] == '':
                new_data['common']['platform'] = 'H5'
                new_data['common']['os'] = 'UNKNOWN'
            elif data['client']['user_agent'] in ['IOS', 'Android']:
                new_data['common']['platform'] = data['client']['user_agent']
                new_data['common']['os'] = data['client']['user_agent']
        return new_data

    def start(self):
        try:
            self.consumer = self.kafka_consumer()
        except Exception as e:
            pass
        while True:
            try:
                if self.quit == 1:
                    return
                try:
                    message = self.consumer.consume(block=True)
                except Exception as e:
                    self.consumer = self.kafka_consumer()  # 重连kafka
                    time.sleep(0.1)
                    continue
                if message and kafka_queue.full() is False:
                    kafka_queue.put(pickle.loads(message.value))
            except Exception as e:
                continue

    def stop(self):
        self.quit = 1


def main():
    DispatcherConsumer().start()
    DispatcherProducer().start()


if __name__ == '__main__':
    main()
