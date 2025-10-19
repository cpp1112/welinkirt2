import asyncio
import json
import uuid
import time
import aiohttp
from datetime import datetime, timedelta
import logging
import redis
from redis.asyncio import Redis
import inspect

from redis_client import event_consumer

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventFusionService:
    def __init__(self):
        self.redis_client = None
        # 需要融合相邻桩号的事件类型
        self.FUSION_EVENT_TYPES = {"01", "04", "05", "06", "07", "08", "09", "15"}
        
        # 严重拥堵事件抑制的事件类型
        self.CONGESTION_SUPPRESSED_TYPES = {"07", "08", "09"}
        
        # 时间窗口配置（秒）
        # 本demo为了简单起见，使用了接收到事件的时间作为事件时间进行判断。生产中需要根据时间戳字段进行判断。
        self.SILENCE_WINDOW = 60  # 1分钟静默窗口
        self.NEW_EVENT_THRESHOLD = 120  # 2分钟新事件阈值
        
    async def initialize(self):
        """初始化Redis连接"""
        if not self.redis_client:
            RD_HOST = "192.168.1.8"
            RD_PORT = 6379
            #RD_DB = 0
            #RD_PWD = "re7890dis"
            #self.redis_client = await Redis(host=RD_HOST, port=RD_PORT, db=RD_DB, password=RD_PWD, decode_responses=True)
            self.redis_client = await Redis(host=RD_HOST, port=RD_PORT, decode_responses=True)
            #logger.info("Redis客户端初始化完成")
    
    async def process_event(self, event_data):
        """
        处理事件数据的主要接口函数
        """
        try:
            # 1. 检查施工占道事件抑制
            if await self._is_construction_suppression_active():
                logger.info(f"事件 {event_data.get('alarmID')} 被施工占道事件抑制")
                return None
            
            # 2. 检查严重拥堵事件抑制
            if (event_data['eventType'] in self.CONGESTION_SUPPRESSED_TYPES and 
                await self._is_congestion_suppression_active()):
                logger.info(f"事件 {event_data.get('alarmID')} 被严重拥堵事件抑制")
                return None
            
            # 3. 查找相似事件
            similar_events = await self._find_similar_events(event_data)
            
            if similar_events:
                # 4. 融合事件
                fused_event = await self._fuse_events(event_data, similar_events)
                return fused_event
            else:
                # 5. 新事件处理
                new_event = await self._handle_new_event(event_data)
                return new_event
                
        except Exception as e:
            raise
            logger.error(f"处理事件时发生错误: {e}")
            return None
    
    async def _is_construction_suppression_active(self):
        """检查是否存在活跃的施工占道事件"""
        try:
            # 检查最近2分钟内是否有施工占道事件
            two_minutes_ago = time.time() - self.NEW_EVENT_THRESHOLD
            construction_events = await self.redis_client.zrangebyscore(
                "active_events:11", two_minutes_ago, "+inf"
            )
            return len(construction_events) > 0
        except Exception as e:
            logger.error(f"检查施工占道抑制时发生错误: {e}")
            return False
    
    async def _is_congestion_suppression_active(self):
        """检查是否存在活跃的严重拥堵事件"""
        try:
            # 检查最近2分钟内是否有严重拥堵事件
            two_minutes_ago = time.time() - self.NEW_EVENT_THRESHOLD
            congestion_events = await self.redis_client.zrangebyscore(
                "active_events:01:5", two_minutes_ago, "+inf"
            )
            return len(congestion_events) > 0
        except Exception as e:
            logger.error(f"检查拥堵抑制时发生错误: {e}")
            return False
    
    async def _find_similar_events(self, event_data):
        """
        查找相似事件（相同类型、方向、相邻桩号）
        返回相似事件ID列表
        """
        event_type = event_data['eventType']
        direction = event_data['direction']
        stake_num = event_data['stakeNum']
        
        # 生成搜索键
        search_keys = await self._generate_search_keys(event_type, direction, stake_num)
        
        similar_event_ids = []
        current_time = time.time()
        
        for search_key in search_keys:
            # 获取该搜索键下的所有事件ID
            events = await self.redis_client.zrangebyscore(
                search_key, current_time - self.NEW_EVENT_THRESHOLD, "+inf"
            )
            
            for event_id in events:
                # 获取事件详情并验证是否真的相似
                event_details = await self.redis_client.hgetall(f"event:{event_id}")
                if event_details and self._is_truly_similar(event_data, event_details):
                    similar_event_ids.append(event_id)
        
        return similar_event_ids
    
    async def _generate_search_keys(self, event_type, direction, stake_num):
        """生成搜索相似事件的Redis键"""
        base_key = f"events:{event_type}:{direction}"
        
        if event_type in self.FUSION_EVENT_TYPES:
            # 需要融合相邻桩号的事件类型
            adjacent_stakes = self._get_adjacent_stakes(stake_num)
            return [f"{base_key}:{stake}" for stake in adjacent_stakes]
        else:
            # 只需要相同桩号的事件类型
            return [f"{base_key}:{stake_num}"]
    
    def _get_adjacent_stakes(self, stake_num):
        """
        获取相邻桩号列表。
        注意：此处的相邻判断完全是自己随便定义了一个规则，非实际规则。仅为模拟该功能。
        """
        try:
            # 解析桩号格式：KX+YYY。
            prefix = stake_num[:3]  # 如 "K0+"
            number_part = int(stake_num[4])  # 如 "100"中的1

            adjacent_stakes = [stake_num]  # 包含自身
            
            # 生成相邻桩号（前后各一个）
            for delta in [-1, 1]:
                new_num = number_part + delta
                if 10 > new_num > 0:
                    adjacent_stake = prefix + str(new_num) + "00"
                    adjacent_stakes.append(adjacent_stake)
            return adjacent_stakes
            
        except (ValueError, IndexError) as e:
            logger.warning(f"解析桩号 {stake_num} 时发生错误: {e}")
            return [stake_num]
    
    def _is_truly_similar(self, event1, event2):
        """验证两个事件是否真正相似"""
        return (event1.get('eventType') == event2.get('eventType') and
                str(event1.get('direction')) == str(event2.get('direction')))
    
    async def _fuse_events(self, new_event, similar_event_ids):
        """融合相似事件"""
        try:
            # 获取所有相似事件的详细信息
            similar_events = []
            for event_id in similar_event_ids:
                event_data = await self.redis_client.hgetall(f"event:{event_id}")
                if event_data:
                    similar_events.append(event_data)
            
            if not similar_events:
                return await self._handle_new_event(new_event)
            
            # 选择基础事件（使用第一个相似事件作为基础）
            base_event = similar_events[0]
            base_event_id = base_event.get('alarmID', similar_event_ids[0])
            
            # 检查静默窗口
            current_time = time.time()
            last_report_time = float(base_event.get('last_report_time', 0))
            
            if current_time - last_report_time < self.SILENCE_WINDOW:
                # 在静默窗口内，只更新计数，不输出
                await self._update_event_count(base_event_id, new_event)
                return None
            else:
                # 超过静默窗口，融合并输出
                fused_event = await self._create_fused_event(base_event_id, similar_events, new_event)
                return fused_event
                
        except Exception as e:
            logger.error(f"融合事件时发生错误: {e}")
            return await self._handle_new_event(new_event)
    
    async def _update_event_count(self, base_event_id, new_event):
        """更新事件计数（静默窗口内）"""
        try:
            # 增加报告计数
            await self.redis_client.hincrby(f"event:{base_event_id}", "reportCount", 1)
            
            # 更新最后报告时间
            await self.redis_client.hset(
                f"event:{base_event_id}", 
                "last_report_time", 
                time.time()
            )
            
            # 如果有新视频URL，更新视频URL
            if new_event.get('videoUrl'):
                await self.redis_client.hset(
                    f"event:{base_event_id}", 
                    "videoUrl", 
                    new_event['videoUrl']
                )
                
        except Exception as e:
            logger.error(f"更新事件计数时发生错误: {e}")
    
    async def _create_fused_event(self, base_event_id, similar_events, new_event):
        """创建融合后的事件"""
        try:
            # 计算总报告次数
            total_count = sum(int(event.get('reportCount', 1)) for event in similar_events) + 1
            
            # 使用基础事件的信息，合并新事件的信息
            fused_event = similar_events[0].copy()
            fused_event['alarmID'] = base_event_id
            fused_event['reportCount'] = total_count
            fused_event['eventTime'] = new_event['eventTime']  # 使用最新时间
            
            # 更新视频URL（如果有新的）
            if new_event.get('videoUrl'):
                fused_event['videoUrl'] = new_event['videoUrl']
            
            # 更新Redis中的事件信息
            await self.redis_client.hset(
                f"event:{base_event_id}",
                mapping={
                    'reportCount': total_count,
                    'eventTime': new_event['eventTime'],
                    'last_report_time': time.time(),
                    'videoUrl': fused_event.get('videoUrl', '')
                }
            )
            
            # 清理其他相似事件
            for event in similar_events[1:]:
                other_event_id = event.get('alarmID')
                if other_event_id and other_event_id != base_event_id:
                    await self._cleanup_event(other_event_id)
            
            return fused_event
            
        except Exception as e:
            logger.error(f"创建融合事件时发生错误: {e}")
            return await self._handle_new_event(new_event)
    
    async def _handle_new_event(self, event_data):
        """处理新事件"""
        try:
            # 生成新的事件ID（如果前端没有提供或需要替换）
            if not event_data.get('alarmID') or event_data.get('alarmID', '').startswith('K'):
                event_data['alarmID'] = str(uuid.uuid4())
            
            # 设置初始报告计数
            event_data['reportCount'] = event_data.get('reportCount', 1)
            
            # 存储事件到Redis
            event_key = f"event:{event_data['alarmID']}"
            event_mapping = event_data.copy()
            event_mapping['last_report_time'] = time.time()
            event_mapping['created_time'] = time.time()
            
            await self.redis_client.hset(event_key, mapping=event_mapping)
            
            # 添加到事件索引
            search_key = await self._get_primary_search_key(event_data)
            await self.redis_client.zadd(
                search_key, 
                {event_data['alarmID']: time.time()}
            )
            
            # 如果是施工占道事件或严重拥堵事件，添加到特殊索引
            if event_data['eventType'] == '11':
                await self.redis_client.zadd(
                    "active_events:11",
                    {event_data['alarmID']: time.time()}
                )
            elif event_data['eventType'] == '01' and event_data.get('eventLevel') == '5':
                await self.redis_client.zadd(
                    "active_events:01:5",
                    {event_data['alarmID']: time.time()}
                )
            
            return event_data
            
        except Exception as e:
            logger.error(f"处理新事件时发生错误: {e}")
            # 返回原始数据作为fallback
            return event_data
    
    async def _get_primary_search_key(self, event_data):
        """获取主搜索键"""
        event_type = event_data['eventType']
        direction = event_data['direction']
        stake_num = event_data['stakeNum']
        
        if event_type in self.FUSION_EVENT_TYPES:
            # 使用第一个相邻桩号作为主键
            adjacent_stakes = self._get_adjacent_stakes(stake_num)
            primary_stake = adjacent_stakes[0]
        else:
            primary_stake = stake_num
        
        return f"events:{event_type}:{direction}:{primary_stake}"
    
    async def _cleanup_event(self, event_id):
        """清理事件数据"""
        try:
            # 获取事件详情以确定搜索键
            event_data = await self.redis_client.hgetall(f"event:{event_id}")
            if event_data:
                search_key = await self._get_primary_search_key(event_data)
                await self.redis_client.zrem(search_key, event_id)
            
            # 删除事件详情
            await self.redis_client.delete(f"event:{event_id}")
            
        except Exception as e:
            logger.error(f"清理事件 {event_id} 时发生错误: {e}")
    
    async def cleanup_old_events(self):
        """清理过期事件（后台任务）"""
        try:
            current_time = time.time()
            # 清理超过2分钟的事件
            cutoff_time = current_time - self.NEW_EVENT_THRESHOLD
            
            # 获取所有事件索引键
            pattern = "events:*"
            keys = await self.redis_client.keys(pattern)
            
            for key in keys:
                # 移除过期的索引
                await self.redis_client.zremrangebyscore(key, "-inf", cutoff_time)
            
            # 清理特殊事件索引
            await self.redis_client.zremrangebyscore("active_events:11", "-inf", cutoff_time)
            await self.redis_client.zremrangebyscore("active_events:01:5", "-inf", cutoff_time)
            
        except Exception as e:
            logger.error(f"清理过期事件时发生错误: {e}")


def generate_video(event):
    #模拟视频生成。因为笔试一已经实现了视频生成服务，在此直接写死视频地址，不另行处理。
    video_url = "http://127.0.0.1:80/static/test_20251016.mp4"
    event["videoUrl"] = video_url
    return event


async def async_handler(event_data):
    fusion_service = EventFusionService()
    await fusion_service.initialize()
    event = await fusion_service.process_event(event_data)
    if not event:  #事件被抑制
        print("事件{}被抑制".format(event_data["alarmID"]))
    else:
        print(event)
        #模拟生成事件视频地址
        event = generate_video(event)

        #向事件接收服务发送最终事件
        data = {"data": event}
        headers = {"Content-Type": "application/json", "Accept": "text/plain"}
        url = 'http://192.168.1.8/api/v1/receiver/event'
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers) as response:
                response.raise_for_status()
        print("已向事件接收服务发送事件{}".format(event["alarmID"]))
        #resp = requests.post(url, json={"data": event}, headers={"Content-Type": "application/json", "Accept": "text/plain"})
        #print("已向事件接收服务发送事件{}".format(event["alarmID"]), resp.status_code)


def sync_handler_wrapper(event_data):
    """
    解决同步函数里跑异步函数。主要是为了复用笔试一中的同步代码。
    demo使用，生产环境不推荐。
    """
    return asyncio.run(async_handler(event_data))


def main():
    logger.info("开启监听模式")
    event_consumer.set_handler(sync_handler_wrapper)
    try:
        print("开始监听...")
        event_consumer.start_listening()
    except KeyboardInterrupt:
        print("收到中断信号，停止监听...")
    finally:
        # 关闭连接
        event_consumer.close()


async def test():
    # 初始化服务
    fusion_service = EventFusionService()
    fusion_service.SILENCE_WINDOW = 5  # 60 1分钟静默窗口
    fusion_service.NEW_EVENT_THRESHOLD = 10  # 120 2分钟新事件阈值
    await fusion_service.initialize()

    # 示例事件数据
    # 需要融合相邻桩号的事件类型{"01", "04", "05", "06", "07", "08", "09", "15"}
    # 严重拥堵事件抑制的事件类型{"07", "08", "09"}
    events = [
        {
            "alarmID": "202509160001",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:00",
            "eventType": "01",
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160002",
            "stakeNum": "K0+200",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:01",
            "eventType": "01",  #需要和第一个event合并处理
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {"time": 10},
        {
            "alarmID": "202509160003",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:01",
            "eventType": "01",  #新的时间窗口
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160004",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:27:00",
            "eventType": "07",  #需要和前一个event合并处理,间隔
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160005",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:00",
            "eventType": "01",
            "direction": 1,
            "eventLevel": "5",  #事件窗口期内，抑制事件 07、08、09
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160006",
            "stakeNum": "K0+200",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:01",
            "eventType": "08",  #被抑制
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160007",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:29:00",
            "eventType": "11",  #施工占道事件，应抑制其他事件上报
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160008",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:30:00",
            "eventType": "01",  #被抑制
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {"time": 5},
        {
            "alarmID": "202509160009",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:30:00",
            "eventType": "01",  #被抑制
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {"time": 8},
        {
            "alarmID": "202509160010",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:30:00",
            "eventType": "01", 
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        }
    ]
    # 处理事件
    for e in events:
        if "time" in e:
            print("sleep...")
            time.sleep(e["time"])
            continue
        else:
            event = await fusion_service.process_event(e)
            if not event:  #事件被抑制
                print("事件{}被抑制".format(e["alarmID"]))
                continue
            else:
                print("上报事件：", e)
                continue

        '''
        generate_video(event)

        data = {"data": event}
        headers = {"Content-Type": "application/json", "Accept": "text/plain"}
        url = 'http://192.168.1.8:5000/api/v1/receiver/event'
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers) as response:
                response.raise_for_status()
        print("已向事件接收服务发送事件{}".format(e["alarmID"]))
        '''

        #resp = requests.post(url, data={"data": event}, headers={"Content-Type": "application/json"})
        #print("已向事件接收服务发送事件{}".format(e["alarmID"]), resp.status_code)


if __name__ == '__main__':
    main()
    #asyncio.run(test())
