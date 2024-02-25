from sqlalchemy import create_engine, text
from datetime import datetime


def connection_yandex_cloud_dwh(echo):
    """Connection to DataBase dwh"""
    login = 'student'
    password = 'student!'
    host = 'rc1b-7ng6ih3jte3824x8.mdb.yandexcloud.net'
    port = '6432'
    db_name = 'dwh'
    return create_engine(f'postgresql://{login}:{password}@{host}:{port}/{db_name}',
                         echo=echo,
                         isolation_level="AUTOCOMMIT")


engine = connection_yandex_cloud_dwh(False)

dag_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 5),
    "provide_context": True,
    'max_active_runs': 1
}
