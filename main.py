import json
import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

import mysql.connector
import pandas as pd
import schedule

# Настройка логирования
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = "parser.log"

log_handler = RotatingFileHandler(log_file, mode='a', maxBytes=5 * 1024 * 1024, backupCount=2, encoding=None, delay=0)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger()
logger.addHandler(log_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


class Parser:
    def __init__(self, path_directory):
        self.path_directory = path_directory
        self.data = {"Name": [], "Device ID": []}

    def parse_txt(self):
        logging.info('Начало разбора текстовых файлов')
        for filename in os.listdir(self.path_directory):
            if "КОТ" in filename:
                with open(os.path.join(self.path_directory, filename), "r") as file:
                    lines = file.readlines()
                device_id = None
                for line in lines:
                    if "=" in line:
                        name, value = line.strip().split("=")
                        if "DevID" in name:
                            device_id = value
                        if "Подпитка" in value and "м3" in value and device_id is not None:
                            self.data["Device ID"].append(device_id)
                            self.data["Name"].append(name)
        logging.info('Разбор текстовых файлов завершен')
        return pd.DataFrame(self.data)


class DatabaseQuery:
    def __init__(self, prsd_txt):
        self.prsd_txt = prsd_txt
        self.data1 = []
        self.data2 = []

    def query_db(self):
        with mysql.connector.connect(
                host='HOST',
                user='USER',
                password='PASSWORD',
                database='DB'
        ) as conn:
            cursor = conn.cursor()

            for index, row in self.prsd_txt.iterrows():
                device_id = row['Device ID']
                name = row['Name']
                current_date = datetime.now().strftime('%Y-%m-d')

                # Запросы на получение электроэнергии на начало суток и подпитки
                queries = [f"""
                SELECT dev.DEVICE_ID, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
                FROM powerdb.devices dev
                LEFT JOIN powerdb.adapters a ON dev.DEVICE_ID = a.ID_DEVICE
                LEFT JOIN powerdb.adapter_parameters ap ON a.ID_ADAPTER = ap.ID_ADAPTER
                LEFT OUTER JOIN powerdb.records r ON a.ID_ADAPTER = r.ID_ADAPTER
                LEFT OUTER JOIN powerdb.data d ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) and
                a.ID_ADAPTER in
                (SELECT ID_ADAPTER FROM adapters a WHERE ADAPTER_NAME = "Энергия на начало суток")
                and
                ORDER BY dev.DEVICE_NAME;
                """,
                           f"""
                SELECT ap.PARAMETER_NAME, d.MEASURE_VALUE
                FROM powerdb.devices dev
                LEFT JOIN powerdb.adapters a ON dev.DEVICE_ID = a.ID_DEVICE
                LEFT JOIN powerdb.adapter_parameters ap ON a.ID_ADAPTER = ap.ID_ADAPTER
                LEFT OUTER JOIN powerdb.records r ON a.ID_ADAPTER = r.ID_ADAPTER
                LEFT OUTER JOIN powerdb.data d ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) and
                a.ID_ADAPTER in
                (SELECT ID_ADAPTER FROM adapters a WHERE a.ID_DEVICE = "{device_id}" and ADAPTER_NAME = "Суточный архив")
                and ap.PARAMETER_NAME in ("{name}")
                ORDER BY dev.DEVICE_NAME;
                """
                           ]

                for i, query in enumerate(queries):
                    try:
                        # Логирование SQL-запросов
                        logging.info(f'Выполнение SQL-запроса {i + 1}: {query}')
                        cursor.execute(query)
                        results = cursor.fetchall()
                        for result in results:
                            if result and not all(pd.isnull(val) for val in result):
                                if i == 0:
                                    self.data1.append(result)
                                else:
                                    self.data2.append(result)
                    except Exception as e:
                        logging.error(f"Ошибка при выполнении SQL-запроса {i + 1}: {str(e)}")

        self.data1 = pd.DataFrame(self.data1, columns=['Device_Id', 'Device_Name', 'Parameter_Name', 'Measure_Value'])
        self.data2 = pd.DataFrame(self.data2, columns=['Parameter_Name', 'Measure_Value'])

        return self.data1, self.data2


class JsonWriter:
    def __init__(self, prsd_txt, prsd_db1, prsd_db2):
        self.prsd_txt = prsd_txt
        self.prsd_db1 = prsd_db1
        self.prsd_db2 = prsd_db2

    def to_json(self):
        all_data = {
            "Parsed Text Data": self.prsd_txt.to_dict(orient='records'),
            "Database Query Data 1": self.prsd_db1.to_dict(orient='records'),
            "Database Query Data 2": self.prsd_db2.to_dict(orient='records')
        }

        filename = datetime.now().strftime("%Y%m%d") + "_all_data.json"

        with open(filename, 'w') as json_file:
            json.dump(all_data, json_file, ensure_ascii=False, indent=4)

        logging.info(f'JSON file {filename} has been written.')


def job():
    logging.info('Job started.')
    directory = "DIR"
    parser = Parser(directory)
    df_txt = parser.parse_txt()
    db_query = DatabaseQuery(df_txt)
    df_db1, df_db2 = db_query.query_db()
    writer = JsonWriter(df_txt, df_db1, df_db2)
    writer.to_json()
    logging.info('Job completed.')


schedule.every().day.at("09:57").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
