
    # 💼 Платформа для анализа и визуализации данных пользователей банковского сервиса
    
    ## 📌 Цель
    Создать полноценную инфраструктуру для **обработки**, **хранения** и **визуализации** данных о клиентах банка,  
    а также автоматизировать обновление данных и дашборда аналитиков.
    
    ---
    
    ## ⚙️ Используемые технологии
    
    | Компонент | Назначение |
    |------------|-------------|
    | 🐘 **PostgreSQL** | Хранение и управление данными клиентов |
    | 🪶 **Apache Airflow** | Автоматизация процессов ETL (загрузка, обработка, обновление данных) |
    | 📊 **Apache Superset** | Построение интерактивных аналитических дашбордов |
    
    ---
    
    ## 🚀 Этапы реализации
    
    ### 1. Настройка окружения на виртуальной машине
    
    #### 1.1 Создание виртуального сервера
    Арендуйте виртуальную машину на платформе [Selectel](https://docs.selectel.ru/cloud/servers/create/create-server/).
    
    **Рекомендуемые параметры:**
    
    | Компонент | Минимальные требования | Рекомендовано |
    |------------|------------------------|----------------|
    | PostgreSQL | 1 CPU, 1 GB RAM | 2 CPU, 2 GB RAM |
    | Superset | 2 CPU, 2 GB RAM | 4 CPU, 4 GB RAM |
    | Airflow | 2 CPU, 2 GB RAM | 4 CPU, 4 GB RAM |
    | **Итого (VM)** | — | **8 CPU, 8 GB RAM, 30+ GB SSD** |
    
    > 💡 Можно использовать Docker-контейнеры, объединив все сервисы в одну сеть,  
    > либо установить каждый компонент отдельно на машине.
    
    #### 1.2 Установка необходимых пакетов
    - PostgreSQL — [Инструкция Selectel](https://selectel.ru/blog/tutorials/how-to-install-and-use-postgresql-on-ubuntu-20-04/)
    - Apache Superset — установка по стандартной инструкции с настройкой подключения к PostgreSQL
    - Apache Airflow — рекомендуется установка без Docker (`pip install apache-airflow`)
    
    ---
    
    ### 2. Создание базы данных в PostgreSQL
    
    #### 2.1 Загрузка исходных данных
    Данные о клиентах находятся в S3-хранилище:
    
    - [credit_clients.csv](https://9c579ca6-fee2-41d7-9396-601da1103a3b.selstorage.ru/credit_clients.csv)
    - [Описание датасета на Kaggle](https://www.kaggle.com/datasets/shrutimechlearn/churn-modelling)
    
    #### 2.2 Создание БД и загрузка данных
    1. Создайте базу данных `bank_clients`
    2. Создайте таблицу `clients` и импортируйте данные из CSV  
    3. Убедитесь, что PostgreSQL принимает подключения:
       - снаружи (для Airflow)
       - внутри docker-сети (для Superset)
    
    Пример команды для импорта:
    ```bash
    \copy clients FROM '/path/to/credit_clients.csv' DELIMITER ',' CSV HEADER;
    

* * *

### 3\. Построение дашборда в Apache Superset

#### 3.1 Настройка соединения

Подключите PostgreSQL в Superset через интерфейс:  
**Data → Databases → + Database**

#### 3.2 Создание визуализаций

Создайте интерактивный дашборд, отображающий ключевые метрики:

*   Распределение клиентов по полу
    
*   Возрастная структура
    
*   Географическое распределение
    
*   Уровень дохода и кредитный рейтинг
    

* * *

### 4\. Автоматизация загрузки данных с помощью Airflow

#### 4.1 DAG для обновления данных

Создайте DAG, который **каждый час** выполняет следующие шаги:

1.  **fetch\_new\_clients** — загрузка новых данных из S3  
    [Пример CSV файла](https://d382a55f-addf-48fd-a6cb-09b4305b5cf9.selstorage.ru/new_clients.csv)
    
2.  **insert\_new\_clients** — добавление новых записей в PostgreSQL
    
3.  **update\_dashboard** — обновление данных в Superset
    

> DAG должен быть устойчив к ошибкам: при падении одного шага другие не должны ломаться.

#### 4.2 Пример структуры DAG

    @dag(schedule_interval="@hourly", start_date=datetime(2025, 10, 16))
    def client_update_pipeline():
        fetch_new_clients = PythonOperator(...)
        insert_new_clients = PythonOperator(...)
        refresh_superset = BashOperator(...)
    
        fetch_new_clients >> insert_new_clients >> refresh_superset
    

* * *

📒 Документация по запуску
--------------------------

1.  Клонируйте репозиторий:
    
        git clone https://github.com/<your-repo>/bank-analytics-platform.git
        cd bank-analytics-platform
        
    
2.  Поднимите окружение:
    
        docker-compose up -d
        
    
3.  Проверьте доступность сервисов:
    
    *   Airflow UI → `http://<vm_ip>:8080`
        
    *   Superset → `http://<vm_ip>:8088`
        
    *   PostgreSQL → `psql -h <vm_ip> -U postgres`
        
4.  Настройте DAG и убедитесь, что задачи выполняются корректно.
    

* * *

📊 Мониторинг и логирование
---------------------------

*   **Airflow UI** — отслеживание статусов DAG и логов задач
    
*   **Superset Logs** — проверка корректности обновлений
    
*   **PostgreSQL logs** — контроль вставки новых данных
    

> При ошибках DAG должен уведомлять администратора (например, через email или Slack).

* * *

🧩 Структура репозитория
------------------------

    bank-analytics-platform/
    ├── dags/
    │   ├── clients_etl_dag.py
    │   └── ...
    ├── sql/
    │   ├── create_clients_table.sql
    │   └── ...
    ├── docs/
    │   ├── dashboard_screenshot.png
    │   └── ...
    ├── docker-compose.yml
    └── README.md
    

* * *

✅ Результат
-----------

*   Полностью функционирующая аналитическая платформа
    
*   Автоматическая загрузка новых данных каждые 60 минут
    
*   Дашборд с актуальной аналитикой
    
*   Документация для развёртывания с нуля
    

* * *

**Автор:** *<твоё имя>*  
**Контакт:** *<почта или ник>*  
**Технологии:** PostgreSQL · Airflow · Superset · Docker

    
