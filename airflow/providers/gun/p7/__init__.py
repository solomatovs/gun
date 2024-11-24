from http.client import HTTPResponse
import json
from time import sleep
from typing import Callable, Dict, List, Optional, Union
from urllib.request import (
    Request,
    BaseHandler,
)

from airflow.sensors.base import PokeReturnValue

from airflow.providers.gun.http import (
    HttpAddhandler,
    HttpConnReq,
    HttpAddAuthConnIdHandler,
    HttpHeadersFromDictModule,
    HttpReqBodyFromDictModule,
    HttpRes,
    HttpSaveToContextModule,
    HTTPRetryRequestIfResponseModule,
    HTTPDefaultErrorWithBodyHandler,
)
from airflow.providers.gun.http.informatica_auth import InformaticaAuthHandler
from airflow.providers.gun.http.airflow_http_password_mgr import (
    AirflowHTTPConnectionPasswordMgr,
)
from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage


def p7_http_request(
    conn_id: str,
    method: Optional[str] = None,
    path: Optional[str] = None,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel: int = 0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет http реквест в p7 используя conn_id для основы url (host:port) + авторизация
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            HttpConnReq(
                builder.context_key,
                builder.template_render,
                conn_id,
                method,
                path,
                timeout,
                ssl_verify,
                debuglevel,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id,
                lambda b: InformaticaAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id),
                ),
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def p7_http_get(
    conn_id: str,
    path: str,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить GET запрос в p7 используя conn_id для основы url (host:port) + авторизация
    """
    return p7_http_request(
        conn_id, "GET", path, timeout, ssl_verify, debuglevel, pipe_stage
    )


def p7_http_post(
    conn_id: str,
    path: str,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить POST запрос в p7 используя conn_id для основы url (host:port) + авторизация
    """

    return p7_http_request(
        conn_id, "POST", path, timeout, ssl_verify, debuglevel, pipe_stage
    )


def p7_http_conn_put(
    conn_id: str,
    path: str,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить PUT запрос в p7 используя conn_id для основы url (host:port) + авторизация
    """

    return p7_http_request(
        conn_id, "PUT", path, timeout, ssl_verify, debuglevel, pipe_stage
    )


def p7_http_delete(
    conn_id: str,
    path: str,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить DELETE запрос в p7 используя conn_id для основы url (host:port) + авторизация
    """

    return p7_http_request(
        conn_id, "DELETE", path, timeout, ssl_verify, debuglevel, pipe_stage
    )


class HttpReqBodyMappingRunModuleDeprecated(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        custom_parameters=None,
        http_callback_url=None,
        parameter_file_path=None,
        parameter_set=None,
        optimization_level=None,
        pushdown_type=None,
        operating_system_profile=None,
        custom_properties=None,
        runtime_instance_name=None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "custom_parameters",
                "http_callback_url",
                "parameter_file_path",
                "parameter_set",
                "optimization_level",
                "pushdown_type",
                "operating_system_profile",
                "custom_properties",
                "runtime_instance_name",
            ]
        )
        super().set_template_render(template_render)

        self.custom_parameters = custom_parameters
        self.http_callback_url = http_callback_url
        self.parameter_file_path = parameter_file_path
        self.parameter_set = parameter_set
        self.optimization_level = optimization_level
        self.pushdown_type = pushdown_type
        self.operating_system_profile = operating_system_profile
        self.custom_properties = custom_properties
        self.runtime_instance_name = runtime_instance_name

        self.request_key = "req"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        req: Request = share[self.request_key]

        body = {
            "action": "RunDeployedMapping",
        }

        if self.http_callback_url:
            body |= {
                "httpCallbackUrl": self.http_callback_url,
            }

        if self.parameter_file_path:
            body |= {
                "parameterFilePath": self.parameter_file_path,
            }

        if self.parameter_set:
            body |= {
                "parameterSet": self.parameter_set,
            }

        if self.custom_parameters:
            body |= {
                "customParameters": self.custom_parameters,
            }

        if self.optimization_level:
            body |= {
                "optimizationLevel": self.optimization_level,
            }

        if self.pushdown_type:
            body |= {
                "pushdownType": self.pushdown_type,
            }

        if self.operating_system_profile:
            body |= {
                "operatingSystemProfile": self.operating_system_profile,
            }

        if self.custom_properties:
            body |= {
                "customProperties": self.custom_properties,
            }

        if self.runtime_instance_name:
            body |= {
                "runtimeInstanceName": self.runtime_instance_name,
            }

        body = json.dumps(
            body,
            ensure_ascii=False,
            default=str,
        )
        body = body.encode(
            encoding="utf-8",
            errors="backslashreplace",
        )

        req.data = body


class HttpReqBodyMappingRunModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        custom_parameters=None,
        http_callback_url=None,
        parameter_file_path=None,
        parameter_set=None,
        optimization_level=None,
        pushdown_type=None,
        operating_system_profile=None,
        custom_properties=None,
        runtime_instance_name=None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "custom_parameters",
                "http_callback_url",
                "parameter_file_path",
                "parameter_set",
                "optimization_level",
                "pushdown_type",
                "operating_system_profile",
                "custom_properties",
                "runtime_instance_name",
            ]
        )
        super().set_template_render(template_render)

        self.custom_parameters = custom_parameters
        self.http_callback_url = http_callback_url
        self.parameter_file_path = parameter_file_path
        self.parameter_set = parameter_set
        self.optimization_level = optimization_level
        self.pushdown_type = pushdown_type
        self.operating_system_profile = operating_system_profile
        self.custom_properties = custom_properties
        self.runtime_instance_name = runtime_instance_name

        self.request_key = "req"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        req: Request = share[self.request_key]

        body = {
            "action": "RunDeployedMapping",
        }

        if self.http_callback_url:
            body |= {
                "httpCallbackUrl": self.http_callback_url,
            }

        if self.parameter_file_path:
            body |= {
                "parameterFilePath": self.parameter_file_path,
            }

        if self.parameter_set:
            body |= {
                "parameterSet": self.parameter_set,
            }

        if self.custom_parameters:
            # преобразую словарь в набор [{name: value},{name_2: value_2}...]
            param = map(
                lambda x: {"name": x[0], "value": x[1]}, self.custom_parameters.items()
            )
            param = list(param)

            body |= {
                "customParameters": param,
            }

        if self.optimization_level:
            body |= {
                "optimizationLevel": self.optimization_level,
            }

        if self.pushdown_type:
            body |= {
                "pushdownType": self.pushdown_type,
            }

        if self.operating_system_profile:
            body |= {
                "operatingSystemProfile": self.operating_system_profile,
            }

        if self.custom_properties:
            body |= {
                "customProperties": self.custom_properties,
            }

        if self.runtime_instance_name:
            body |= {
                "runtimeInstanceName": self.runtime_instance_name,
            }

        body = json.dumps(
            body,
            ensure_ascii=False,
            default=str,
        )
        body = body.encode(
            encoding="utf-8",
            errors="backslashreplace",
        )

        req.data = body


def p7_http_mapping_run(
    conn_id: str,
    dis: str,
    app: str,
    mapping: str,
    mapping_custom_params: Optional[dict] | str = None,
    http_callback_url: Optional[str] = None,
    parameter_file_path: Optional[str] = None,
    parameter_set: Optional[str] = None,
    optimization_level: Optional[str] = None,
    pushdown_type: Optional[str] = None,
    operating_system_profile: Optional[str] = None,
    custom_properties: Optional[str] = None,
    runtime_instance_name: Optional[str] = None,
    port: int = 8100,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить http запрос на запуск маппинга в p7
    host, учётные данные для авторизации берутся из указанного conn_id
    """

    path = f":{ port }/DataIntegrationService/modules/ms/v1/applications/{ app }/mappings/{ mapping }"

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            HttpConnReq(
                builder.context_key,
                builder.template_render,
                conn_id,
                "POST",
                path,
                timeout,
                ssl_verify,
                debuglevel,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id,
                lambda b: InformaticaAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id),
                ),
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpHeadersFromDictModule(
                builder.context_key,
                builder.template_render,
                {
                    "servicename": dis,
                    "Content-Type": "application/json",
                },
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPDefaultErrorWithBodyHandler(),
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpReqBodyMappingRunModuleDeprecated(
                builder.context_key,
                builder.template_render,
                mapping_custom_params,
                http_callback_url,
                parameter_file_path,
                parameter_set,
                optimization_level,
                pushdown_type,
                operating_system_profile,
                custom_properties,
                runtime_instance_name,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpRes(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def p7_http_mapping_stats(
    conn_id: str,
    jobId: str,
    port: int = 8096,
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить http запрос для получения информации о маппинге через jobId
    JobId это уникальный номер запуска маппинга
    JobId маппинга может быть получен после запуска через метод p7_http_mapping_run
    host, учётные данные для авторизации берутся из указанного conn_id
    """

    path = f":{ port }/RestOperationsHub/services/v1/MappingService/MappingStats('{ jobId }')"

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            HttpConnReq(
                builder.context_key,
                builder.template_render,
                conn_id,
                "GET",
                path,
                timeout,
                ssl_verify,
                debuglevel,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id,
                lambda b: InformaticaAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id),
                ),
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpRes(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def p7_http_mapping_stats_checker(mapping_stats: dict) -> PokeReturnValue:
    """
    Выполняет проверку результата запроса mapping_stats для запущенного маппинга
    Выдаёт результат в виде PokeReturnValue который необходим Airflow Sensor
    В зависимости от статуса выполнения маппинга, сенсор:
        - завершится если status работы маппинга: complited
        - заново запланирует запрос если status работы маппинга: queued, running
        - свалится с ошибкой если статус работы маппинга: failed, canceled, любой другой неизвестный статус
    """

    status = mapping_stats["mappingDetails"]["status"]
    match status:
        case "QUEUED":
            print("👉 queue")
            return PokeReturnValue(is_done=False)
        case "RUNNING":
            print("🚴 running")
            return PokeReturnValue(is_done=False)
        case "COMPLETED":
            print("😎 complited")
            return PokeReturnValue(is_done=True)
        case "FAILED":
            print("😭 failed")
            raise RuntimeError(f"mapping failed with an error.\n{mapping_stats}")
        case "CANCELED":
            print("😤 canceled")
            raise RuntimeError(f"mapping canceled with an error.\n{mapping_stats}")
        case _:
            print("😕 unknown")
            raise RuntimeError(f"unknown mapping status: {status}.\n{mapping_stats}")


class HTTPRetryRequestIfMappingNotDone(BaseHandler):
    """
    Повторяет запрос если p7 вернул статус незавершённого маппинга
    """

    def __init__(self, poke_interval: float = 10):
        self.poke_interval = poke_interval

    def retry_if_mapping_not_done(self, req: Request, res: HTTPResponse):
        """Ожидаю выполнения Job на informatica"""

        if res.status != 200:
            raise RuntimeError(
                f"""Failed to complete the request to wait for mapping to complete.
Please review the informatica http response body:
{res.read1()}
"""
            )

        result = res.read1()
        result = json.loads(result)
        print(result)

        match p7_http_mapping_stats_checker(result):
            case PokeReturnValue(is_done=True):
                return False
            case PokeReturnValue(is_done=False):
                print(f"waiting {self.poke_interval} seconds...")
                sleep(self.poke_interval)
                return True

        return False
    
    def http_response(self, request: Request, response: HTTPResponse) -> HTTPResponse:
        if self.retry_if_mapping_not_done(request, response):
            return self.parent.open(request, timeout=request.timeout)

        return response

    def https_response(self, request: Request, response: HTTPResponse) -> HTTPResponse:
        if self.retry_if_mapping_not_done(request, response):
            return self.parent.open(request, timeout=request.timeout)

        return response


class P7RetryRequestIfMappingNotDone(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        poke_interval,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "poke_interval",
            ]
        )
        super().set_template_render(template_render)

        self.poke_interval = poke_interval

        self.before_modules_key = "modules"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.before_modules_key]

        modules.append(
            HTTPRetryRequestIfMappingNotDone(
                self.poke_interval,
            )
        )


def p7_http_mapping_run_and_wait(
    conn_id: str,
    dis: str,
    app: str,
    mapping: str,
    custom_parameters: Optional[Union[Dict[str, str], str]] = None,
    http_callback_url: Optional[str] = None,
    parameter_file_path: Optional[str] = None,
    parameter_set: Optional[str] = None,
    optimization_level: Optional[str] = None,
    pushdown_type: Optional[str] = None,
    operating_system_profile: Optional[str] = None,
    custom_properties: Optional[str] = None,
    runtime_instance_name: Optional[str] = None,
    port_to_run: int = 8100,
    port_to_wait: int = 8096,
    poke_interval: Union[float, str] = 10,
    http_timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel=0,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Позволяет выполнить запустить маппинг на informatica
    Ожидает завершение маппинга выполняя опрос каждые {poke_interval} секунд
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            HttpConnReq(
                builder.context_key,
                builder.template_render,
                conn_id,
                "POST",
                f":{ port_to_run }/DataIntegrationService/modules/ms/v1/applications/{ app }/mappings/{ mapping }",
                http_timeout,
                ssl_verify,
                debuglevel,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id,
                lambda b: InformaticaAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id),
                ),
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpHeadersFromDictModule(
                builder.context_key,
                builder.template_render,
                {
                    "servicename": dis,
                    "Content-Type": "application/json",
                },
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPDefaultErrorWithBodyHandler(),
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpReqBodyMappingRunModule(
                builder.context_key,
                builder.template_render,
                custom_parameters,
                http_callback_url,
                parameter_file_path,
                parameter_set,
                optimization_level,
                pushdown_type,
                operating_system_profile,
                custom_properties,
                runtime_instance_name,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPDefaultErrorWithBodyHandler(),
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpRes(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
            ),
            pipe_stage,
        )

        def fetch_job_id(req, res, context):
            res = res.read1()
            res2 = json.loads(res)
            res2 = res2["jobId"]
            return res2

        builder.add_module(
            HttpSaveToContextModule(
                builder.context_key,
                builder.template_render,
                "formit_job_id",
                fetch_job_id,
            ),
            pipe_stage,
        )

        path = f":{ port_to_wait }/RestOperationsHub/services/v1/MappingService"
        path = "{}/{}".format(path, "MappingStats('{{ formit_job_id }}')")

        builder.add_module(
            HttpConnReq(
                builder.context_key,
                builder.template_render,
                conn_id,
                "GET",
                path,
                http_timeout,
                ssl_verify,
                debuglevel,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id,
                lambda b: InformaticaAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id),
                ),
            ),
            pipe_stage,
        )

        builder.add_module(
            P7RetryRequestIfMappingNotDone(
                builder.context_key,
                builder.template_render,
                poke_interval,
            ),
            pipe_stage,
        )

        builder.add_module(
            HttpRes(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
