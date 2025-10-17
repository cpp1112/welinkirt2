# -*- coding:utf-8 -*-
import time
import json
import random
import string
from flask import Blueprint, request, jsonify

from redis_client import event_producer, event_consumer, event_info 


api_v1 = Blueprint('api_v1', __name__)


def make_json_response(data=None, code=1, message=""):
    return jsonify({'code': code, 'data': data, 'message': message})


@api_v1.route("/ping", methods=['GET'])
def ping():
    return make_json_response()


@api_v1.route("/fusion/event", methods=['POST'])
#模拟事件聚合服务接口。
#该接口接收来自前端的事件报告
def receive_raw_event():
    paras = request.get_json()
    event_data = paras.get("data")
    if not event_data:
        return make_json_response(code=0, message="event data is required!")
    print("produce...")
    event_producer.produce(event_data)
    return make_json_response(data={"data": event_data})


@api_v1.route("/receiver/event", methods=['POST'])
#模拟事件接收服务器receive接口
def event_receiver():
    paras = request.get_json()
    event_data = paras.get("data")
    if not event_data:
        return make_json_response(code=0, message="event data is required!")
    event_info.append(event_data)
    return make_json_response(data={"data": event_data})



@api_v1.route("/events", methods=['GET'])
def get_events():
    data = event_info.get_events()
    return make_json_response(data=data)
