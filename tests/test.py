# -*- coding:utf-8 -*-
import os
import json
import sys
import time
import traceback
from json.decoder import JSONDecodeError
import requests
from requests.sessions import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


session = Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)


class HttpClient():
    def __init__(self, baseurl):
        self.request = session
        self.base_url = baseurl

    def get_header(self):
        header = {"Content-Type": "application/json", "Accept": "text/plain"}
        return header

    def get(self, url, data=None):
        url = self.base_url + url
        header = self.get_header()
        return self.request.get(url, json=data, headers=header)

    def post(self, url, data=None):
        url = self.base_url + url
        header = self.get_header()
        return self.request.post(url, json=data, headers=header)


def response_verify(r):
    if r.status_code != 200:
        if r.status_code == 401:
            print('Your login is invalid, please login again.')
            sys.exit()
        elif r.status_code == 403:
            print("No permission!")
        else:
            print(r.text)
            sys.exit()


def send_events(hclient):
    url = "/api/v1/fusion/event"
    #url = "/api/v1/receiver/event"
    # 示例事件数据
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
        {
            "alarmID": "202509160003",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:02",
            "eventType": "07",
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160004",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:03",
            "eventType": "01",
            "direction": 1,
            "eventLevel": "5",  #事件窗口期内，抑制事件 07、08、09
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160005",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:04",
            "eventType": "11",  #施工占道事件，应抑制其他事件上报
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        },
        {
            "alarmID": "202509160006",
            "stakeNum": "K0+100",
            "reportCount": 1,
            "eventTime": "2025-09-16 10:26:05",
            "eventType": "01",
            "direction": 1,
            "eventLevel": "1",
            "videoUrl": "http://example.com/v.mp4"
        }
    ]
    
    # 处理事件
    for e in events:
        print("发送事件：", e["alarmID"])
        r = hclient.post(url, data={"data": e})
        response_verify(r)
        d = json.loads(r.text)


def get_events(hclient):
    url = "/api/v1/events"
    r = hclient.get(url)
    response_verify(r)
    d = json.loads(r.text)['data']
    if not d:
        print("no data")
        sys.exit()
    d = [json.loads(i) for i in d]
    print("获取最终事件：", [i["alarmID"] for i in d])
    return d


def get_one_event_video(event):
    url = event.get("videoUrl")
    if not url:
        return
    print("下载视频文件...")
    #download_url = baseurl + "/" + furl
    spath = "./test.mp4"
    download_file_simple(url, spath)
    return


def download_file_simple(url, save_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"文件已下载: {save_path}")
    else:
        print(f"下载失败，状态码: {response.status_code}")


def test_chain():
    baseurl = "http://127.0.0.1:5000"
    hclient = HttpClient(baseurl=baseurl)

    #提交视频剪辑请求
    print("模拟前端发送事件")
    send_events(hclient)

    print("等待事件处理....")
    time.sleep(3)
    
    #获取events
    events = get_events(hclient)
    #获取事件视频文件
    #event = json.loads(events[0])
    event = events[0]
    print("下载任意一个事件的视频...")
    get_one_event_video(event)
    return


def test():
    baseurl = "http://127.0.0.1:5000"
    hclient = HttpClient(baseurl=baseurl)

    #提交视频剪辑请求
    print("模拟前端发送事件")
    send_events(hclient)
    #print("获取事件")
    #get_events(hclient)


if __name__ == '__main__':
    test_chain()
    #test()









