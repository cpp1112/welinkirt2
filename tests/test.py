# -*- coding:utf-8 -*-
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


def create_clip_task(hclient):
    url = "/api/v1/task/clip"
    data = {"timestamp": time.time()}
    r = hclient.post(url, data=data)
    response_verify(r)
    d = json.loads(r.text)
    task_info = d['data'].get("task")
    #{"task_id": task_id, "status": "created", "timestamp": timestamp}
    if not task_info:
        print("创建剪辑任务失败")
        sys.exit()
    return task_info


def get_task_status(hclient, task_id):
    url = f"/api/v1/task/{task_id}/status"
    r = hclient.get(url)
    response_verify(r)
    d = json.loads(r.text)['data']
    if not d:
        print("no data")
        sys.exit()
    task_status = d.get("task_status")
    return task_status


def run_task(hclient, task_id):
    url = f"/api/v1/task/{task_id}/run"
    r = hclient.post(url)
    response_verify(r)
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
    baseurl = "http://127.0.0.1:80"
    hclient = HttpClient(baseurl=baseurl)

    #提交视频剪辑请求
    print("提交视频剪辑请求")
    task_info = create_clip_task(hclient)
    task_id = task_info["task_id"]
    print("剪辑任务id:", task_id)
    
    #获取task状态
    task_status = get_task_status(hclient, task_id)
    if task_status not in ("done", "failed"):
        bt = time.time()
        timeout = 30
        while  True:
            time.sleep(1)
            task_status = get_task_status(hclient, task_id)
            if task_status in ("done", "failed"):
                break
            if time.time() - bt > timeout:
                print(f"任务超过{timeout}s未完成，程序将退出")
                sys.exit()
    if task_status == "failed":
        print(f"任务状态failed，程序将退出")
        sys.exit()
    # task done
    print("下载视频文件...")
    furl = baseurl + "/static/" + task_id + ".mp4"
    spath = "./" + task_id + ".mp4"
    download_file_simple(furl, spath)
    return


def test():
    baseurl = "http://127.0.0.1:80"
    hclient = HttpClient(baseurl=baseurl)

    #提交视频剪辑请求
    #print("提交视频剪辑请求")
    #task_info = create_clip_task(hclient)
    
    #获取task状态
    #task_id = task_info["task_id"]
    
    task_id = "task_clip_1758355563_JaCfR"
    #print(get_task_status(hclient, task_id))
    #return
    
    furl = baseurl + "/static/" + task_id + ".mp4"
    spath = "./" + task_id + ".mp4"
    download_file_simple(furl, spath)
    return

    print("剪辑任务id:", task_id)

    task_status = get_task_status(hclient, task_id)
    if task_status not in ("done", "failed"):
        bt = time.time()
        timeout = 30
        while  True:
            time.sleep(1)
            task_status = get_task_status(hclient, task_id)
            if task_status in ("done", "failed"):
                break
            if time.time() - bt > timeout:
                print(f"任务超过{timeout}s未完成，程序将退出")
                sys.exit()
    if task_status == "failed":
        print(f"任务状态failed，程序将退出")
        sys.exit()

    dowload_url = baseurl + f"/static/{task_id}"



if __name__ == '__main__':
    test_chain()









