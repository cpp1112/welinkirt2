# -*- coding:utf-8-*-
import json
import redis
import os

import config


def get_redis_client():
    RD_HOST = config.REDIS_HOST
    RD_PORT = config.REDIS_PORT
    RD_DB = config.REDIS_DB
    RD_PWD = config.REDIS_PWD
    return redis.StrictRedis(host=RD_HOST, port=RD_PORT, db=RD_DB, password=RD_PWD, decode_responses=True)


class TaskProducer:
    """任务生产者"""   
    def __init__(self, channel='video_tasks'):
        self.channel = channel
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

    def produce(self, task_message):
        print("+++++", task_message)
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


class TaskInfo:
    # redis存储任务
    def __init__(self):
        self.redis_client = get_redis_client()
        self.hkey = "task_clip"

    def append(self, task_info):
        task_id = task_info.get("task_id")
        if not task_id:
            return
        self.redis_client.hset(self.hkey, task_id, json.dumps(task_info))
        return

    def exists(self, task_id):
        return self.redis_client.hget(self.hkey, task_id)

    def get_status(self, task_id):
        task = self.redis_client.hget(self.hkey, task_id)
        if not task:
            return
        task = json.loads(task)
        return task.get("status")

    def update_status(self, task_id, task_status):
        task = self.exists(task_id)
        if not task:
            return
        task = json.loads(task)
        if task["status"] == task_status:
            return
        task["status"] = task_status
        self.redis_client.hset(self.hkey, task_id, json.dumps(task)) 

    def delete_task(self, task_id):
        self.redis_client.hdel(self.hkey, task_id)


class TaskConsumer:
    """任务消费者"""
    def __init__(self, handler=None, channel='video_tasks'):
        self.channel = channel
        self.redis_client = get_redis_client()
        self.pubsub = self.redis_client.pubsub()
        self.task_handler = handler

    def set_handler(self, handler):
        """
        设置任务处理函数
        """
        self.task_handler = handler
        
    def start_listening(self):
        if not self.task_handler:
            print("TaskConsumer has no handler!")
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
            # 处理实际任务消息
            if message['type'] == 'message':
                try:
                    task_data = json.loads(message['data'])
                    print(f"收到任务: {task_data}")
                    # 调用任务处理函数
                    self.task_handler(task_data)
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


task_producer = TaskProducer()
task_info = TaskInfo()
task_consumer = TaskConsumer()

