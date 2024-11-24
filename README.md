
Package ``apache-airflow-providers-gun``

Release: ``0.3.2``

`(⊙.⊙(☉̃ₒ☉)⊙.⊙) <https://alm-itsk.gazprom-neft.local:8080/TFS/GPN/dmpdwh/_git/airflow_providers?path=/gun/README.md>`__

Information
----------------

Провайдер расширяет возможности написания кода в стиле ``taskflow task api``

Позволяет расширить функционал ``@task.python()`` задачи в виде цепочки действий
Доступные расширения:

``airflow.providers.gun.pipe`` базовый компонент для запуска цепочек задач

``airflow.providers.gun.ch`` компоненты для работы с clickhouse (использует native protocol, порты 9440 и 9000)

``airflow.providers.gun.pg`` компоненты для работы с postgres

``airflow.providers.gun.p7`` компоненты для работы с plus7 formit

``airflow.providers.gun.http`` компоненты для работы с http запросами. Поддерживает несколько типов авторизации: basic, preemptive_basic, digest, ntlm, kerberos, p7

``airflow.providers.gun.back`` компонентыы для работы с callback'ами в airflow, в стиле taskflow api

``airflow.providers.gun.log`` компоненты для бизнес-логирования интеграций

Installation
------------

Пакет можно установить так
``pip install apache-airflow-providers-gun``

Поддерживаемая версия python: 3.10

Зависимости
------------


| PIP package       | Version required |
| :---              |    :----:        |
| kerberos          | >=1.3.1`         |
| psycopg2          | >=2.9            |
| clickhouse_driver | >=0.2.6          |
| apache-airflow    | >=2.6.0          |

Примеры использования
------------

```mapping run informatica с использованием task.python airflow```
```py
    @task.python()
    @http_run()
    @http_retry_if_code([400, 401], 5)
    @http_error_if_code([404, range(500, 600)])
    @http_body_dict(
        {
            "action": "RunDeployedMapping",
            "customParameters": "{{  params.infa_parameters }}",
        }
    )
    @http_headers(
        {
            "servicename": "{{ params.infa_dis }}",
            "Content-Type": "application/json",
        }
    )
    @http_auth_conn_id("{{ params.infa_dis_id }}", schema="informatica")
    @http_post(
        "https://{{ host_port_conn(params.infa_dis_id) }}/DataIntegrationService/modules/ms/v1/applications/{{ params.infa_app }}/mappings/{{ params.infa_mapping }}"
    )
    @pipe()
    def mapping_run(context):
        """запуск маппинга на informatica и возврат jobId в xcom"""
        res = context["pipe"]["res"]
        res = res.read1()
        print(res)
        return json.loads(res)["jobId"]
```

```mapping_wait informatica с использованием task.sensor airflow```
```py
    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule")
    @http_run()
    @http_retry_if_code([400, 401], 5)
    @http_error_if_code([404, range(500, 600)])
    @http_auth_conn_id("{{ params.infa_hub_id }}", schema="informatica")
    @http_get(
        "https://{{ {{ host_port_conn(params.infa_hub_id) }} }}/RestOperationsHub/services/v1/MappingService/MappingStats('{{ task_instance.xcom_pull(task_ids='mapping_run') }}')"
    )
    @pipe()
    def mapping_wait(context) -> PokeReturnValue:
        """Ожидаю выполнения Job на informatica"""
        res = context["pipe"]["res"]
        res = res.read1()
        status = json.loads(res)["mappingDetails"]["status"]
        match status:
            case "QUEUED":
                print("👉 wait")
                return PokeReturnValue(is_done=False)
            case "RUNNING":
                print("🚴")
                return PokeReturnValue(is_done=False)
            case "COMPLETED":
                print("😎")
                return PokeReturnValue(is_done=True)
            case "FAILED":
                print("😭")
                raise RuntimeError(f"mapping failed with an error.\n{res}")
            case "CANCELED":
                print("😤")
                raise RuntimeError(f"mapping canceled with an error.\n{res}")
            case _:
                print("😕")
                raise RuntimeError(f"unknown mapping status: {status}.\n{res}")
```

```более сложные цепочки действий могут комбинировать http, postgres clickhouse одновременно если выполнять переключение pipe```

```py
        @task.sensor(
            poke_interval=10,
            timeout=3600,
            mode="reschedule",
            on_success_callback=task_log,
            on_failure_callback=task_log,
            on_execute_callback=task_log,
            on_retry_callback=task_log,
        )
        @pg_save_to_context("target_row", fetch_count)
        @pg_execute_file_and_commit("$PWD/load_buf_count.sql")
        @pg_auth_airflow_conn(
            conn_id="{{ params.gp_conn_id }}",
            dsn="application_name={{ dag.dag_id }}",
            cursor_factory=PrintSqlCursor,
        )
        @pipe.switch("second_pipe")
        @http_run()
        @http_retry_if_code([400, 401], 5)
        @http_error_if_code([404, range(500, 600)])
        @http_auth_conn_id("{{ params.infa['conn_id'] }}", schema="informatica")
        @http_get(
            "https://{{ params.infa['hub_host'] }}/RestOperationsHub/services/v1/MappingService/MappingStats('{{ task_instance.xcom_pull(task_ids='load_buf.mapping_run') }}')",
            debuglevel=1,
        )
        @pipe()
        def mapping_wait(context) -> PokeReturnValue:
            """
                Здесь описана сложная цепочка действия с использованием http и postgres запроса
                -> pipe - использование первого пайпа
                ---> запуск http запроса
                -> pipe_second - переключение на второй pipe
                ---> выполнение postgres запроса из файла
                ---> сохранение необходимых данных в airflow context
                -> и только после этого выполнение основного метода mapping_wait
                -> а после завершения mapping_wait будут выполнены callback функции для логирования
            """
            res = context["pipe"]["res"]
            res = res.read1()
            status = json.loads(res)["mappingDetails"]["status"]

            return PokeReturnValue(is_done=True)
