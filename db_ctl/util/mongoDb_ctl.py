from pymongo import MongoClient
from common import const

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
            print(f"集合 {collection_name} 不存在，将自动创建（建议先执行索引脚本）")
        return collection
    except ValueError as e:
        print(f" MongoDB配置错误：{str(e)}")
        raise