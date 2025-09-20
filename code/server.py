from flask import Flask
import logging

from apis_v1 import api_v1
    

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Flask API服务
app = Flask(__name__)
app.register_blueprint(api_v1, url_prefix='/api/v1')


def main():
    # 3. 启动API服务
    logger.info("启动API服务...")
    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭...")
    finally:
        logger.info("服务已关闭")


if __name__ == "__main__":
    main()