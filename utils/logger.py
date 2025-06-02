import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # 日志格式
    # filename='app.log'  # 日志输出到文件
)

# 创建日志记录器
logger = logging.getLogger(__name__)
