import cv2
import threading
import time
import os
from collections import deque
import logging
from datetime import datetime

from redis_client import task_info, task_consumer
import config


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Video-Worker")


class VideoWorker:
    """视频处理工作器"""
    def __init__(self, rtsp_url, buffer_duration=300):
        self.rtsp_url = rtsp_url
        self.buffer_duration = buffer_duration
        self.buffer = deque()
        self.is_running = False
        self.cap = None
        self.thread = None
        self.fps = 30
        
    def start(self):
        """启动工作器"""
        self.is_running = True
        self.thread = threading.Thread(target=self._buffer_worker)
        self.thread.daemon = True
        self.thread.start()
        logger.info("视频工作器已启动")
        
    def stop(self):
        """停止工作器"""
        self.is_running = False
        if self.cap:
            self.cap.release()
        if self.thread:
            self.thread.join()
        logger.info("视频工作器已停止")
        
    def _buffer_worker(self):
        """缓存工作线程"""
        # 连接到RTSP流
        self.cap = cv2.VideoCapture(self.rtsp_url)
        if not self.cap.isOpened():
            logger.error("无法连接到RTSP流")
            return
        logger.info("成功连接到RTSP流")
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0:
            fps = 30  # 默认值
        self.fps = fps
        logger.info(f"视频帧率为：{fps}")

        self.frame_interval = 1.0 / self.fps
        # 使用更精确的帧率控制
        next_frame_time = time.time()
        
        while self.is_running:
            # 等待直到下一帧的时间
            current_time = time.time()
            if current_time < next_frame_time:
                sleep_time = next_frame_time - current_time
                time.sleep(sleep_time)
                continue
                
            # 更新下一帧的时间
            next_frame_time += self.frame_interval
            
            # 尝试读取帧
            ret, frame = self.cap.read()
            if not ret:
                logger.warning("读取视频帧失败")
                # 如果读取失败，重置时间计算
                next_frame_time = time.time()
                continue
                
            # 获取当前时间戳
            current_time = time.time()
            # 添加到缓冲区
            self.buffer.append((current_time, frame))
            # 移除过期的帧
            while self.buffer and current_time - self.buffer[0][0] > self.buffer_duration:
                self.buffer.popleft()
            
    def extract_and_save_clip(self, target_timestamp, before_seconds=15, after_seconds=15, output_path=None):
        """
        提取指定时间范围的视频并保存为文件
        Returns:
            str: 保存的视频文件路径，如果失败则返回None
        """
        # 如果没有指定输出路径，生成一个
        if output_path is None:
            timestamp_str = datetime.fromtimestamp(target_timestamp).strftime('%Y%m%d_%H%M%S')
            output_path = f"clip_{timestamp_str}.mp4"
        
        # 提取时间范围内的帧
        frames = self.extract_clip(target_timestamp, before_seconds, after_seconds)
        
        if not frames:
            logger.warning(f"在指定时间范围内未找到视频帧: {target_timestamp}")
            return None
        
        logger.info("总共截取{}帧,当前缓存总共{}帧".format(len(frames), len(self.buffer)))
        timestamp, frame = frames[0]
        logger.info(f"帧形状: {frame.shape}, 帧数据类型: {frame.dtype}, 帧值范围: {frame.min()} - {frame.max()}")
    
        # 保存为视频文件
        success = self.save_frames_as_video(frames, output_path)
        if success:
            logger.info(f"视频片段已保存: {output_path}")
            return output_path
        else:
            logger.error(f"保存视频片段失败: {output_path}")
            return None
            
    def extract_clip(self, target_timestamp, before_seconds=15, after_seconds=15):
        """
        提取指定时间范围的视频帧
        Returns:
            list: 包含时间戳和帧的元组列表 [(timestamp, frame), ...]
        """
        start_time = target_timestamp - before_seconds
        end_time = target_timestamp + after_seconds
        # 检查是否需要等待未来帧
        current_time = time.time()
        if end_time > current_time:
            # 需要等待未来帧
            wait_until = end_time
            wait_duration = wait_until - current_time
            logger.info(f"需要等待 {wait_duration:.2f} 秒以获取未来帧")
            time.sleep(wait_duration)
        
        # 查找在时间范围内的帧
        frames_in_range = []
        for ts, frame in self.buffer:
            if start_time <= ts <= end_time:
                frames_in_range.append((ts, frame))
                
        # 按时间戳排序
        frames_in_range.sort(key=lambda x: x[0])
        
        return frames_in_range

    def save_frames_as_video(self, frames_with_timestamps, output_path):
        """
        将带时间戳的帧序列保存为视频文件
        """
        if not frames_with_timestamps:
            logger.error("没有帧可保存")
            return False
            
        try:
            # 获取视频尺寸（假设所有帧尺寸相同）
            height, width, channels = frames_with_timestamps[0][1].shape
            logger.info(f"视频尺寸: {width}x{height}, 通道数: {channels}")
            
            # 使用实际帧率
            fps = self.fps
            logger.info(f"使用帧率: {fps}")
            
            # 尝试不同的编码器
            codecs = [
                ('mp4v', 'MP4V'),
                ('avc1', 'H264'),
                ('xvid', 'XVID'),
                ('h264', 'H264')
            ]
            
            success = False
            last_error = None
            
            for codec_name, codec_desc in codecs:
                try:
                    # 定义视频编码器
                    fourcc = cv2.VideoWriter_fourcc(*codec_name)
                    
                    # 创建VideoWriter对象
                    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
                    
                    if not out.isOpened():
                        logger.warning(f"无法使用编码器 {codec_desc} 初始化 VideoWriter")
                        continue
                        
                    logger.info(f"使用编码器 {codec_desc} 初始化 VideoWriter 成功")
                    
                    # 写入所有帧
                    logger.info("开始写入本地视频文件")
                    frame_count = 0
                    for i, (_, frame) in enumerate(frames_with_timestamps):
                        # 确保帧的尺寸匹配
                        if frame.shape[:2] != (height, width):
                            frame = cv2.resize(frame, (width, height))
                        
                        out.write(frame)
                        frame_count += 1
                        
                        # 每100帧记录一次进度，减少日志输出
                        if frame_count % 100 == 0:
                            logger.info(f"已写入 {frame_count}/{len(frames_with_timestamps)} 帧")
               
                        
                    # 释放VideoWriter
                    logger.info("写入本地视频文件完成")
                    out.release()
                    
                    # 检查文件是否成功创建
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                        logger.info(f"成功保存视频: {output_path}, 帧数: {frame_count}")
                        success = True
                        break
                    else:
                        logger.warning(f"使用编码器 {codec_desc} 保存视频失败")
                        
                except Exception as e:
                    last_error = str(e)
                    logger.error(f"使用编码器 {codec_desc} 时出错: {e}")
                    continue
                    
            if not success:
                logger.error(f"所有编码器都失败: {last_error}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"保存视频时发生异常: {e}")
            return False

        
    def get_buffer_info(self):
        """获取缓冲区信息"""
        if not self.buffer:
            return {
                "buffer_size": 0,
                "buffer_duration": self.buffer_duration,
                "oldest_timestamp": None,
                "newest_timestamp": None
            }
            
        return {
            "buffer_size": len(self.buffer),
            "buffer_duration": self.buffer_duration,
            "oldest_timestamp": self.buffer[0][0],
            "newest_timestamp": self.buffer[-1][0]
        }


def test_clip():
    current_time = time.time() - 15
    clip_path = video_worker.extract_and_save_clip(current_time, 15, 15)
    if clip_path:
        print(f"视频剪辑已保存: {clip_path}")
    else:
        print("提取视频剪辑失败")


def handler(task_data):
    #{"task_id": task_id, "status": "created", "timestamp": timestamp}
    before_seconds=15
    after_seconds=15
    save_dir = config.STATIC_DIR

    task_id = task_data["task_id"]
    output_path = save_dir + "/" + task_id + ".mp4"
    try:
        r = video_worker.extract_and_save_clip(
            task_data["timestamp"],
            before_seconds,
            after_seconds,
            output_path = output_path
        )
        if not r:
            print("剪辑失败")
            task_info.update_status(task_id, "failed")
            return
        print(f"视频剪辑已保存:", output_path)
        task_info.update_status(task_id, "done")
        print(f"已经更新任务{task_id}状态")
        return
    except:
        raise


def main(rtsp_url):
    logger.info("初始化，请耐心等待...")
    global video_worker
    # 初始化视频工作器
    video_worker = VideoWorker(rtsp_url, buffer_duration=40)  # 40s缓存
    video_worker.start()
    # 等待一段时间让缓冲区填充
    time.sleep(30)
    logger.info("初始化完成")

    logger.info("开启监听模式")
    task_consumer.set_handler(handler)
    try:
        print("开始监听任务...")
        task_consumer.start_listening()
    except KeyboardInterrupt:
        print("收到中断信号，停止监听...")
    finally:
        # 关闭连接
        task_consumer.close()
    

if __name__ == '__main__':
    rtsp_url = f"rtsp://{config.RTSP_URL}/stream"
    main(rtsp_url)
