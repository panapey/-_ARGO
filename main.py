import logging
import os
import time
from datetime import datetime

import mysql.connector
import pandas as pd
import schedule


class Parser:
    def __init__(self, path_directory):
        self.path_directory = path_directory
        self.data = {"Name": [], "Value": [], "Device ID": []}

    def parse_txt(self):
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
                            self.data["Value"].append(value)
        logging.info('Parsing text files completed.')
        return pd.DataFrame(self.data)


class DatabaseQuery:
    def __init__(self, prsd_txt):
        self.prsd_txt = prsd_txt
        self.data = {"Device Id": [], "Device Name": [], "Parameter Name": [], "Measure Value": []}

    def query_db(self):
        conn = mysql.connector.connect(
            host='HOST',
            user='USER',
            password='PASSWORD',
            database='DB'
        )
        cursor = conn.cursor()

        # Создаем DataFrame для хранения результатов
        result_data = pd.DataFrame(
            columns=['Device_Id', 'Device_Name', 'Adapter_Name', 'Id_Parameter', 'Parameter_Name', 'Record_Time',
                     'Measure_Value'])

        for index, row in self.prsd_txt.iterrows():
            device_id = row['Device ID']
            name = row['Name']
            current_date = datetime.now().strftime('%Y-%m-%d')

            # Первый запрос
            query1 = f"""
                SELECT dev.DEVICE_ID, dev.DEVICE_NAME, a.ADAPTER_NAME, ap.ID_PARAMETER, ap.PARAMETER_NAME, r.RECORD_TIME, d.MEASURE_VALUE
                FROM powerdb.devices dev
                LEFT JOIN powerdb.adapters a ON dev.DEVICE_ID = a.ID_DEVICE
                LEFT JOIN powerdb.adapter_parameters ap ON a.ID_ADAPTER = ap.ID_ADAPTER
                LEFT outer JOIN powerdb.records r ON a.ID_ADAPTER = r.ID_ADAPTER
                LEFT outer JOIN powerdb.data d ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) and
                a.ID_ADAPTER in
                (SELECT ID_ADAPTER FROM adapters a WHERE ADAPTER_NAME = "Энергия на начало суток")
                ORDER BY dev.DEVICE_NAME;
            """

            # Второй запрос
            query2 = f"""
                SELECT dev.DEVICE_ID, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
                FROM powerdb.devices dev
                LEFT JOIN powerdb.adapters a ON dev.DEVICE_ID = a.ID_DEVICE
                LEFT JOIN powerdb.adapter_parameters ap ON a.ID_ADAPTER = ap.ID_ADAPTER
                LEFT outer JOIN powerdb.records r ON a.ID_ADAPTER = r.ID_ADAPTER
                LEFT outer JOIN powerdb.data d ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) and
                a.ID_ADAPTER in
                (SELECT ID_ADAPTER FROM adapters a WHERE a.ID_DEVICE = "{device_id}" and ADAPTER_NAME = "Суточный архив")
                    and ap.PARAMETER_NAME in ("{name}")
                    ORDER BY dev.DEVICE_NAME;
            """

            # Выполняем оба запроса и сохраняем результаты в DataFrame
            for query in [query1, query2]:
                cursor.execute(query)
                results = cursor.fetchall()

                # Добавляем результаты в DataFrame только если они не пустые и не содержат только NA значения
                for result in results:
                    if result and not all(pd.isnull(val) for val in result):
                        result_data.loc[len(result_data)] = list(result)

        conn.close()

        # Возвращаем данные в виде pandas DataFrame
        return result_data


class ExcelWriter:
    def __init__(self, prsd_txt, prsd_db):
        self.prsd_txt = prsd_txt
        self.prsd_db = prsd_db

    def to_excel(self):
        df = pd.concat([self.prsd_txt, self.prsd_db], axis=1)
        df = df.fillna("NULL")
        filename = datetime.now().strftime("%Y%m%d") + "_данные_по_котельным.xlsx"
        df.to_excel(filename, index=False)
        logging.info(f'Excel file {filename} has been written.')


def job():
    logging.info('Job started.')
    directory = "//192.168.40.50/ParamComments"
    parser = Parser(directory)
    df_txt = parser.parse_txt()
    db_query = DatabaseQuery(df_txt)
    df_db = db_query.query_db()
    writer = ExcelWriter(df_txt, df_db)
    writer.to_excel()
    logging.info('Job completed.')


schedule.every().day.at("11:03").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
