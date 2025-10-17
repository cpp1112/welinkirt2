# -*- coding:utf-8-*-
import json
import redis
import os

import config


def get_redis_client():
    RD_HOST = "192.168.1.8"
    RD_PORT = 6379
    return redis.StrictRedis(host=RD_HOST, port=RD_PORT, decode_responses=True)


class Producer:
    """任务生产者"""   
    def __init__(self):
        self.redis_client = get_redis_client()
        
    def connect(self):
        """# 测试连接"""
        try:
            self.redis_client.ping()
            print("成功连接到Redis")
            return True
        except redis.ConnectionError as e:
            print(f"无法连接到Redis: {e}")
            return False

    def close(self):
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis连接已关闭")


class TaskProducer(Producer):
    """任务生产者"""   
    def __init__(self, channel='video_tasks'):
        self.channel = channel
        self.redis_client = get_redis_client()

    def produce(self, task_message):
        if not task_message:
            return
        task_id = task_message.get('task_id')
        if not task_id:
            return
        try:
            # 发布任务到Redis频道
            self.redis_client.publish(self.channel, json.dumps(task_message))
            print(f"已发布任务: {task_id}")
            return task_id
        except redis.RedisError as e:
            print(f"发布任务失败: {e}")
            return None


class EventProducer(Producer):
    """任务生产者"""   
    def __init__(self, channel='raw_events'):
        self.channel = channel
        self.redis_client = get_redis_client()

    def produce(self, event_message):
        if not event_message:
            return
        event_id = event_message.get('alarmID')
        if not event_id:
            return
        try:
            # 发布任务到Redis频道
            self.redis_client.publish(self.channel, json.dumps(event_message))
            print(f"已发布event: {event_id}")
            return event_id
        except redis.RedisError as e:
            print(f"发布任务失败: {e}")
            return None


class Consumer:
    """任务消费者"""
    def __init__(self, channel, handler=None):
        self.channel = channel
        self.redis_client = get_redis_client()
        self.pubsub = self.redis_client.pubsub()
        self.handler = handler

    def set_handler(self, handler):
        """
        设置任务处理函数
        """
        self.handler = handler
        
    def start_listening(self):
        if not self.handler:
            print("Consumer has no handler!")
            sys.exit()
        if not self.channel:
            print("Consumer has no channel!")
            sys.exit()
        # 订阅频道
        self.pubsub.subscribe(self.channel)
        print(f"开始监听频道: {self.channel}")
        self.is_listening = True
        
        # 监听消息
        for message in self.pubsub.listen():
            if not self.is_listening:
                break
            # 处理订阅确认消息
            if message['type'] == 'subscribe':
                print(f"成功订阅频道: {message['channel']}")
                continue
            # 处理消息
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    print(f"收到消息: {data}")
                    # 调用处理函数
                    self.handler(data)
                except json.JSONDecodeError as e:
                    print(f"解析任务消息失败: {e}")
                except KeyError as e:
                    print(f"任务消息格式错误: {e}")
                except Exception as e:
                    print(f"处理任务时发生错误")
                    raise
        return True
        
    def stop_listening(self):
        """停止监听任务"""
        self.is_listening = False
        if self.pubsub:
            self.pubsub.unsubscribe()
            print("已停止监听任务")
            
    def close(self):
        """关闭Redis连接"""
        self.stop_listening()
        if self.redis_client:
            self.redis_client.close()
            print("Redis连接已关闭")


class EventInfo:
    # redis存储事件。作为事件接收服务的存储使用。生产环境需要使用mysql等持久存储。
    def __init__(self):
        self.redis_client = get_redis_client()
        self.key = "event_info"

    def append(self, event_info):
        self.redis_client.lpush(self.key, json.dumps(event_info))
        return

    def get_events(self):
        return self.redis_client.lrange(self.key, 0, -1)


event_producer = EventProducer(channel='raw_events')
event_consumer = Consumer(channel='raw_events')
event_info = EventInfo()

