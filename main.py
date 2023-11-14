import asyncio
import json
import logging
import logging.config
import os
from datetime import datetime

import aiofiles
import aiomysql

# Настройка асинхронного логгера
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'parser.log',
            'formatter': 'standard',
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(message)s',
        },
    },
    'root': {
        'handlers': ['file', 'console'],
        'level': 'INFO',
    },
})

# Параметры подключения к базе данных
DB_CONFIG = {
    'host': 'HOST',
    'port': 'PORT',
    'user': 'USER',
    'password': 'PASSWORD',
    'db': 'DB',
}


# Класс для асинхронного парсинга текстовых файлов
class AsyncParser:
    def __init__(self, path_directory):
        self.path_directory = path_directory

    async def parse_txt(self):
        data = {"Name": [], "Device ID": []}
        for filename in os.listdir(self.path_directory):
            if "КОТ" in filename:
                async with aiofiles.open(os.path.join(self.path_directory, filename), mode='r') as file:
                    async for line in file:
                        if "=" in line:
                            name, value = line.strip().split("=")
                            if "DevID" in name:
                                device_id = value
                            if "Подпитка" in value and "м3" in value:
                                data["Device ID"].append(device_id)
                                data["Name"].append(name)
        logging.info('Текстовые файлы успешно обработаны')
        return data


# Класс для асинхронных запросов к базе данных
class AsyncDatabaseQuery:
    def __init__(self, db_config, prsd_txt):
        self.db_config = db_config
        self.prsd_txt = prsd_txt

    async def query_db(self):
        data1 = {}
        data2 = {}
        current_date = datetime.now().strftime('%Y-%m-%d')
        async with aiomysql.create_pool(**self.db_config) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    for device_id, name in zip(self.prsd_txt['Device ID'], self.prsd_txt['Name']):
                        query1 = f"""SELECT dev.DEVICE_ID, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
                        FROM powerdb.devices dev
                        LEFT JOIN powerdb.adapters a ON dev.DEVICE_ID = a.ID_DEVICE
                        LEFT JOIN powerdb.adapter_parameters ap ON a.ID_ADAPTER = ap.ID_ADAPTER
                        LEFT OUTER JOIN powerdb.records r ON a.ID_ADAPTER = r.ID_ADAPTER
                        LEFT OUTER JOIN powerdb.data d ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                        WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) and
                        a.ID_ADAPTER in
                        (SELECT ID_ADAPTER FROM adapters a WHERE ADAPTER_NAME = "Энергия на начало суток")
                        and dev.DEVICE_NAME LIKE "%КОТ%"
                        and ap.PARAMETER_NAME = "A+ (активная суммарная)"
                        ORDER BY dev.DEVICE_NAME;"""
                        query2 = f"""SELECT dev.DEVICE_ID, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
                        FROM powerdb.devices dev
                        LEFT JOIN powerdb.adapters a ON dev.DEVICE_ID = a.ID_DEVICE
                        LEFT JOIN powerdb.adapter_parameters ap ON a.ID_ADAPTER = ap.ID_ADAPTER
                        LEFT OUTER JOIN powerdb.records r ON a.ID_ADAPTER = r.ID_ADAPTER
                        LEFT OUTER JOIN powerdb.data d ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                        WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) and
                        a.ID_ADAPTER in
                        (SELECT ID_ADAPTER FROM adapters a WHERE a.ID_DEVICE = "{device_id}" and ADAPTER_NAME = 
                        "Суточный архив") and ap.PARAMETER_NAME in ("{name}")
                        and dev.DEVICE_NAME LIKE "%КОТ%"
                        ORDER BY dev.DEVICE_NAME;"""
                        
                        await cursor.execute(query1)
                        result1 = await cursor.fetchall()
                        data1.setdefault(device_id, []).extend(result1)
                        await cursor.execute(query2)
                        result2 = await cursor.fetchall()
                        data2.setdefault(device_id, []).extend(result2)

        logging.info('Запросы к базе данных выполнены')
        return data1, data2


# Класс для записи данных в формат JSON
class AsyncJsonWriter:
    def __init__(self, prsd_txt, prsd_db1, prsd_db2):
        self.prsd_txt = prsd_txt
        self.prsd_db1 = prsd_db1
        self.prsd_db2 = prsd_db2

    async def to_json(self):
        all_data = {
            "Parsed Text Data": self.prsd_txt,
            "Database Query Data 1": self.prsd_db1,
            "Database Query Data 2": self.prsd_db2
        }
        filename = datetime.now().strftime("%Y%m%d") + "_all_data.json"
        async with aiofiles.open(filename, mode='w') as file:
            await file.write(json.dumps(all_data, ensure_ascii=False, indent=4))
        logging.info(f'Данные записаны в файл {filename}')


# Основная асинхронная функция для выполнения работы
async def job():
    path_directory = "DIR"
    parser = AsyncParser(path_directory)
    prsd_txt = await parser.parse_txt()

    db_query = AsyncDatabaseQuery(DB_CONFIG, prsd_txt)
    prsd_db1, prsd_db2 = await db_query.query_db()

    writer = AsyncJsonWriter(prsd_txt, prsd_db1, prsd_db2)
    await writer.to_json()


# Функция для запуска асинхронной задачи
async def main():
    logging.info("Запуск основной асинхронной задачи")
    await job()


# Запуск асинхронной функции main
if __name__ == "__main__":
    asyncio.run(main())
