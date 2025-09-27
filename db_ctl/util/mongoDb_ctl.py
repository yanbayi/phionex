from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import logging
from common import const

# 配置日志 - 同时输出到控制台和文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.log"),  # 日志文件
        logging.StreamHandler()  # 控制台输出
    ]
)
logger = logging.getLogger(__name__)

def init_mongo_collection(collection_name: str) -> MongoClient:
    try:
        client = MongoClient(
            const.MONGODB_HOST,
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=10000
        )
        client.admin.command('ping')  # 验证连接

        db = client[const.DB_NAME]
        collection = db[collection_name]

        if collection_name not in db.list_collection_names():
            logger.warning(f"集合 {collection_name} 不存在，将自动创建（建议先执行索引脚本）")
        else:
            logger.info(f"MongoDB连接成功：数据库={const.DB_NAME}，集合={collection_name}")
        return collection
    except ValueError as e:
        logger.error(f" MongoDB配置错误：{str(e)}")
        raise