from datetime import date
import psycopg2
import os
import json


class TransferData:
    """
    Класс, ответственный за сбор данных из внешних источников и сохранение их в аналитическую БД
    """
    def __init__(self, work_date=date.today(), jobs_dir='etl_jobs'):
        self.work_date = work_date
        self.jobs_dir = jobs_dir
        self.loaded_dependencies = set()


    def load_data(self, load_job):
        """
        Функция для загрузки данных из внешних источников на диск.
        Должна по итогу создавать csv-файл на диске с форматом sep='|', quotechar="^".

        :param load_job: словарь с описанием джобы для загрузки данных
        """
        if load_job.get('db_from') == 'open_api':
            # Специфичная для источника логика сбора данных
            import requests
            import pandas as pd
            data = requests.get('https://api.publicapis.org/entries').json()
            data = pd.DataFrame(data['entries'])
            data['work_date'] = self.work_date
            data = data.loc[:, ['API', 'Description', 'Auth', 'HTTPS', 'Cors', 'Link', 'Category', 'work_date']]
            # Сохраняем в специальный формат для уменьшения количества ошибок при загрузке
            data.to_csv(load_job.get('file_name'), index=False, sep='|', quotechar="^")

        elif load_job.get('db_from') == 'other_db_type':
            # DO SOME HERE AND SAVE FILE TO load_job.get('file_name')
            pass

    def upload_data(self, load_job):
        """
        Функция для загрузки данных с диска в таблицы в аналитической БД.
        Для корректной работы необходим файл в формате csv с параметрами DELIMITER '|', QUOTE '^'
        ANALYTICS_DB_CONN_STRING - переменная окружения, в которой содержится connect-строка для
        нашей аналитической БД для юзера loader.
        Например, postgres://loader:*loader_password*@localhost:5432/postgres

        :param load_job: словарь с описанием джобы для загрузки данных
        """
        #
        with psycopg2.connect(os.environ['ANALYTICS_DB_CONN_STRING']) as conn:
            curs = conn.cursor()
            file_name = load_job.get('file_name')
            with open(file_name, 'r') as f:
                product = load_job.get('product')
                curs.execute(load_job.get('ddl'))
                curs.execute(f'truncate {product};')
                copy_query = f"""COPY {product} from STDIN with (FORMAT csv, DELIMITER '|', QUOTE '^', HEADER True)"""
                curs.copy_expert(copy_query, f)
                conn.commit()
            os.remove(file_name)

    def process_data(self, process_job):
        """
        Функция для обработки данных внутри БД.

        :param process_job: Описание джобы по обработке данных.
        """
        with psycopg2.connect(os.environ['ANALYTICS_DB_CONN_STRING']) as conn:
            curs = conn.cursor()
            curs.execute(process_job.get('query').format(work_date=self.work_date))
            conn.commit()

    def launch_job(self, job):
        """
        Функция, которая запускает джобы в зависимости от их типа

        :param job: Джоба по сбору или обработке данных
        """
        job_type = job.get('job_type')
        if job_type == 'load':
            self.load_data(job)
            self.upload_data(job)
        elif job_type == 'process':
            self.process_data(job)
        else:
            print('Unknown job type for job', job)

    def launch_all_jobs(self):
        """
        Запускаем все джобы с учётом наличия зависимостей в них
        """
        # Загружаем все джобы в память
        job_paths = os.listdir(self.jobs_dir)
        jobs = {}
        for job_path in job_paths:
            with open(os.path.join(self.jobs_dir, job_path), 'r') as f:
                jobs[job_path] = json.load(f)
        # Итерируемся по джобам, проверяя, загружены ли зависимости для них.
        # Осторожно, если зависимости не найдутся, цикл будет вечным
        while True:
            jobs_local = jobs.copy() # Для удаления элементов словаря во время итерации по нему
            for job_name, job in jobs_local.items():
                print(job)
                job_dependencies = job.get('dependencies')
                if job_dependencies is None or len(set(job_dependencies) - self.loaded_dependencies) == 0:
                    print(f'Started {job_name}')
                    self.launch_job(job)
                    product = job.get('product')
                    if product is not None:
                        self.loaded_dependencies.add(product)
                    jobs.pop(job_name)
                    print(f'Finished {job_name}')
            if len(jobs) == 0:
                break
        print('All jobs processed')


if __name__ == '__main__':
    """
    Запускаем обновление данных раз в день в 10.00
    """
    import schedule
    import time

    def scheduled_update():
        tr = TransferData(jobs_dir='path_to_jobs_dir/')
        tr.launch_all_jobs()

    schedule.every().day.at("10:00").do(scheduled_update)

    while True:
        schedule.run_pending()
        time.sleep(60)