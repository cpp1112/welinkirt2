# -*- coding:utf-8 -*-
import time
import json
import random
import string
from flask import Blueprint, request, jsonify

from redis_client import task_info, task_producer


api_v1 = Blueprint('api_v1', __name__)


def make_json_response(data=None, code=1, message=""):
    return jsonify({'code': code, 'data': data, 'message': message})


@api_v1.route("/ping", methods=['GET'])
def ping():
    return make_json_response()


@api_v1.route("/task/clip", methods=['POST'])
def create_clip_task():
    paras = request.get_json()
    if "timestamp" not in paras:
        return make_json_response(code=0, message="timestamp is required!")

    timestamp = paras["timestamp"]
    salt = "".join(random.sample(string.ascii_letters, 5))
    task_id = "task_clip_{}_{}".format(str(int(timestamp)), salt)
    file_url = "static/" + task_id + ".mp4"

    task = {"task_id": task_id, "status": "created", "timestamp": timestamp, "download_url": file_url}
    task_info.append(task)
    task_producer.produce(task)

    return make_json_response(data={"task": task})


@api_v1.route("/task/<task_id>/status", methods=['GET'])
def get_task_status(task_id):
    if not task_info.exists(task_id):
        return make_json_response(code=0, message="task is not exists")
    status = task_info.get_status(task_id)
    return make_json_response(data={"task_status": status})


#测试使用
@api_v1.route("/task/<task_id>/run", methods=['POST'])
def run_clip_task(task_id):
    task = task_info.exists(task_id)
    if not task:
        return make_json_response(code=0, message="task is not exists")
    task = json.loads(task)
    #task = {"task_id": task_id, "status": "created", "timestamp": timestamp}
    task_producer.produce(task)
    return make_json_response(data={"task": task})

#测试使用
@api_v1.route("/task/<task_id>/status/<status>", methods=['PATCH'])
def update_task_status(task_id, status):
    if not task_info.exists(task_id):
        return make_json_response(code=0, message="task is not exists")
    if status not in ("created", "running", "done", "failed"):
        return make_json_response(code=0, message="status is invalid")
    task_info.update_status(task_id, task_status)
    return make_json_response(data={"task_status": status})




