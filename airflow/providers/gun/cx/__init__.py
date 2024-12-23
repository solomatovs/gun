import json
import logging
from jinja2.runtime import StrictUndefined

from typing import Optional, Callable, Any, Iterable, Sequence, Mapping

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage

from airflow.models.taskinstance import TaskInstance
from airflow.utils.xcom import XCOM_RETURN_KEY


__all__ = [
    "cx_save",
    "cx_save_from_xcom",
    "cx_save_to_xcom",
    "cx_print",
    "cx_print_result",
    "cx_print_xcom",
    "cx_render_result",
]

class ContextSaveModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        save_to: str,
        save_object: Any,
        save_if: Callable[[Any, Any], bool] | bool | str,
        jinja_render: bool,
    ):
        super().__init__(context_key)
        super().set_template_fields(["save_to"])
        super().set_template_render(template_render)

        self.ti_key = "ti"
        self.save_to = save_to
        self.save_object = save_object
        self.save_if = save_if
        self.jinja_render = jinja_render

    def __call__(self, context):
        self.render_template_fields(context)

        if self.jinja_render:
            res = self.template_render(self.save_object, context)
        else:
            res = self.save_object
        
        if self.save_if_eval(context, res):
            context[self.save_to] = res
    
    def save_if_eval(self, context, res):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if(context, res)

        return save_if


def cx_save(
    save_to: str,
    save_object: Any,
    save_if: Callable[[Any, Any], bool] | bool | str = True,
    jinja_render: bool = True,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования

    Args:
        save_to: это имя ключа внутри Airflow Context, в которое будет сохранена информация
        save_object: это объект, который будет сохранён в Airflow Context
        jinja_render: если True, то объект будет предварительно обработан через jinja

    Examples:
        сохраним в context["my_key"] результат выполнения функции, которая возвращает словарь:
        >>> @task()
            @cx_save("my_key", {
                'target_row': "{{ params.target_row }}",
                'name': res,
                'error_row': 0,
            })
            @pipe(pipe_stage=PipeStage.After)
             def my_task():
                pass
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextSaveModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_object=save_object,
                save_if=save_if,
                jinja_render=jinja_render,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

def not_none_or_undifined(context, res):
    if res is None:
        return False

    if isinstance(res, StrictUndefined):
        return False
    
    return True


class ContextSaveFromXcomModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        save_to: str,
        save_if: Callable[[Any, Any], bool] | bool | str,
        task_ids: str | Iterable[str],
        xcom_key: str,
        jinja_render: bool,
    ):
        super().__init__(context_key)
        super().set_template_fields(["save_to", "task_ids", "xcom_key"])
        super().set_template_render(template_render)

        self.ti_key = "ti"
        self.save_to = save_to
        self.task_ids = task_ids
        self.xcom_key = xcom_key
        self.jinja_render = jinja_render
        self.save_if = save_if

    def __call__(self, context):
        self.render_template_fields(context)

        ti: TaskInstance = context[self.ti_key]
        res = ti.xcom_pull(task_ids=self.task_ids, key=self.xcom_key)

        if self.jinja_render:
            res = self.template_render(res, context)

        if self.save_if_eval(context, res):
            context[self.save_to] = res

    def save_if_eval(self, context, res):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if(context, res)

        return save_if


def cx_save_from_xcom(
    save_to: str,
    task_ids: str | Iterable[str],
    xcom_key: str = XCOM_RETURN_KEY,
    save_if: Callable[[Any, Any], bool] | bool | str = True,
    jinja_render: bool = True,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить информацию из xcom в Airflow Context для последующего использования

    Args:
        save_to: это имя ключа внутри Airflow Context, в которое будет сохранена информация
        xcom_key: это имя ключа в xcom, из которого будет взята информация
        task_ids: это идентификаторы задач, из которых будет взята информация, например имя предыдущего таска
        jinja_render: если True, то объект будет предварительно обработан через jinja

    Examples:
        сохраним в context["prev_task_result"] результат выполнения функции, которая возвращает словарь:
        >>> @task()
            @cx_save_from_xcom("prev_task_result", "my_prev_task")
            @pipe(pipe_stage=PipeStage.After)
             def my_task():
                pass
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextSaveFromXcomModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                task_ids=task_ids,
                xcom_key=xcom_key,
                jinja_render=jinja_render,
                save_if=save_if,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class ContextSaveToXcomModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        jinja_template: str,
        save_to: str,
        save_if: Callable[[Any, Any], bool] | bool | str,
    ):
        super().__init__(context_key)
        super().set_template_fields(["save_to"])
        super().set_template_render(template_render)

        self.ti_key = "ti"
        self.jinja_template = jinja_template
        self.save_to = save_to
        self.save_if = save_if

    def __call__(self, context):
        self.render_template_fields(context)

        res = self.template_render(self.jinja_template, context)

        if self.save_if_eval(context, res):
            ti: TaskInstance = context[self.ti_key]
            res = ti.xcom_push(key=self.save_to, value=res)
    
    def save_if_eval(self, context, res):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if(context, res)

        return save_if



def cx_save_to_xcom(
    jinja_template: str,
    save_to: str = XCOM_RETURN_KEY,
    save_if: Callable[[Any, Any], bool] | bool | str = True,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить информацию в xcom из Airflow Context для последующего использования

    Args:
        jinja_template: это шаблон jinja с помощью которого можно взять любой объект из Airflow Context
        save_to: это имя ключа в xcom, в которое будет сохранена информация

    Examples:
        вернем в xcom название дага:
        > return_value - это значение ключа в xcom по умолчанию. С таким ключем сохраняются все значения в xcom если не указано инное
        >>> @task()
            @cx_save_to_xcom("{{ dag.dag_id }}", "return_value")
            @pipe(pipe_stage=PipeStage.After)
             def my_task():
                pass
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextSaveToXcomModule(
                builder.context_key,
                builder.template_render,
                jinja_template=jinja_template,
                save_to=save_to,
                save_if=save_if,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PrintComplexObject:
    def __init__(
        self,
        complex_object: Any,
    ):
        self.complex_object = complex_object

    def __call__(self, logger: logging.Logger):
        match self.complex_object:
            case res if isinstance(res, Mapping):
                res = json.dumps(res, indent=5, ensure_ascii=False, default=str)
                logger.info(res)
            case res if isinstance(res, Sequence):
                res = json.dumps(res, indent=5, ensure_ascii=False, default=str)
                logger.info(res)
            case None:
                logger.info("None")
            case res:
                logger.info(str(res))


class ContextPrintModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        jinja_template: Any | str,
        print_if: Callable[[Any, Any], bool] | bool | str,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.ti_key = "ti"
        self.jinja_template = jinja_template
        self.print_if = print_if

    def __call__(self, context):
        self.render_template_fields(context)
        logger = logging.getLogger(self.__class__.__name__)

        res = self.template_render(self.jinja_template, context)

        if self.print_if_eval(context, res):
            logger.info(f">>> print: {self.jinja_template}")
            PrintComplexObject(res)(logger)

    def print_if_eval(self, context, res):
        match self.print_if:
            case bool():
                save_if = self.print_if
            case str():
                save_if = self.template_render(self.print_if, context)
            case _:
                save_if = self.print_if(context, res)

        return save_if
def cx_print(
    jinja_template: Any | str,
    print_if: Callable[[Any, Any], bool] | bool | str = True,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет вывести в лог результат jinja выражения

    Args:
        jinja_template: jinja шаблон, который будет выполнен, а его результат будет выведен в лог

    Examples:
        В лог будет выведено значение dag.dag_id
        >>> @task()
        >>> @cx_print("{{ dag.dag_id }}")
        >>> @pipe(pipe_stage=PipeStage.After)
        >>> def my_task():
        >>>     pass
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextPrintModule(
                builder.context_key,
                builder.template_render,
                jinja_template=jinja_template,
                print_if=print_if,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

# def cx_print_if_not_empty(
#     jinja_template: Any | str,
#     pipe_stage: Optional[PipeStage] = None,
# ):
#     """
#     Модуль позволяет вывести в лог результат jinja выражения
#     Но выполнит это только если результат не None или undifined

#     Args:
#         jinja_template: jinja шаблон, который будет выполнен, а его результат будет выведен в лог

#     Examples:
#         В лог будет выведено значение dag.dag_id
#         >>> @task()
#         >>> @cx_print("{{ dag.dag_id }}")
#         >>> @pipe(pipe_stage=PipeStage.After)
#         >>> def my_task():
#         >>>     pass
#     """

#     def wrapper(builder: PipeTaskBuilder):
#         builder.add_module(
#             ContextPrintModule(
#                 builder.context_key,
#                 builder.template_render,
#                 jinja_template=jinja_template,
#                 print_if=not_none_or_undifined,
#             ),
#             pipe_stage,
#         )
#         return builder

#     return wrapper


class ContextPrintResultModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        jinja_render: bool,
        print_if: Callable[[Any, Any], bool] | bool | str,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.ti_key = "ti"
        self.jinja_render = jinja_render
        self.print_if = print_if

    def __call__(self, context):
        self.render_template_fields(context)
        logger = logging.getLogger(self.__class__.__name__)
        
        logger.info(f">>> print: return value")
        res = context[self.task_result_key]

        if self.jinja_render:
            res = self.template_render(res, context)

        if self.print_if_eval(context, res):
            PrintComplexObject(res)(logger)

    def print_if_eval(self, context, res):
        match self.print_if:
            case bool():
                save_if = self.print_if
            case str():
                save_if = self.template_render(self.print_if, context)
            case _:
                save_if = self.print_if(context, res)

        return save_if

def cx_print_result(
    jinja_render: bool = True,
    print_if: Callable[[Any, Any], bool] | bool | str = True,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет вывести в лог return декорируемой функции

    Examples:
        Результат работы декорируемой функции my_task() будет выведен в лог: {"key": "value"}
        >>> @task()
        >>> @cx_print_result()
        >>> @pipe(pipe_stage=PipeStage.After)
        >>> def my_task():
        >>>     return {"key": "value"}
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextPrintResultModule(
                builder.context_key,
                builder.template_render,
                jinja_render=jinja_render,
                print_if=print_if,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

# def cx_print_result_if_not_empty(
#     jinja_render: bool = True,
#     pipe_stage: Optional[PipeStage] = None,
# ):
#     """
#     Модуль позволяет вывести в лог return декорируемой функции
#     Но только в случае если результат не None или undifined

#     Examples:
#         Результат работы декорируемой функции my_task() будет выведен в лог: {"key": "value"}
#         >>> @task()
#         >>> @cx_print_result()
#         >>> @pipe(pipe_stage=PipeStage.After)
#         >>> def my_task():
#         >>>     return {"key": "value"}
#     """

#     def wrapper(builder: PipeTaskBuilder):
#         builder.add_module(
#             ContextPrintResultModule(
#                 builder.context_key,
#                 builder.template_render,
#                 jinja_render=jinja_render,
#                 print_if=not_none_or_undifined,
#             ),
#             pipe_stage,
#         )
#         return builder

#     return wrapper


class ContextRenderResultModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.ti_key = "ti"

    def __call__(self, context):
        self.render_template_fields(context)
        
        res = context[self.task_result_key]
        res = self.template_render(res, context)
        context[self.task_result_key] = res

def cx_render_result(
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Позволяет отрендерить результат декорируемой функции

    Examples:
        Результат работы декорируемой функции my_task() это шаблон: {"dag_name": "{{ dag.dag_id }}"}
        В таком случае результат работы функции будет выглядеть так: {"dag_name": "my_dag"}
        >>> @task()
        >>> @cx_render_result()
        >>> @pipe(pipe_stage=PipeStage.After)
        >>> def my_task():
        >>>     return {
                    "dag_name": "{{ dag.dag_id }}"
                }
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextRenderResultModule(
                builder.context_key,
                builder.template_render,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

class ContextPrintXComModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        task_ids: str | Iterable[str],
        xcom_key: str,
        jinja_render: bool,
        print_if: Callable[[Any, Any], bool] | bool | str,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.ti_key = "ti"
        self.task_ids = task_ids
        self.xcom_key = xcom_key
        self.jinja_render = jinja_render
        self.print_if = print_if

    def __call__(self, context):
        self.render_template_fields(context)

        ti: TaskInstance = context[self.ti_key]
        res = ti.xcom_pull(task_ids=self.task_ids, key=self.xcom_key)

        if self.jinja_render:
            res = self.template_render(res, context)

        if self.print_if_eval(context, res):
            logger = logging.getLogger(self.__class__.__name__)
            logger.info(f">>> print xcom {self.task_ids}.{self.xcom_key}:")
            PrintComplexObject(res)(logger)
    
    def print_if_eval(self, context, res):
        match self.print_if:
            case bool():
                save_if = self.print_if
            case str():
                save_if = self.template_render(self.print_if, context)
            case _:
                save_if = self.print_if(context, res)

        return save_if

def cx_print_xcom(
    task_ids: str | Iterable[str],
    xcom_key: str = XCOM_RETURN_KEY,
    print_if: Callable[[Any, Any], bool] | bool | str = True,
    jinja_render: bool = True,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Позволяет отрендерить xcom значение в лог (для дебага)

    Args:
        task_ids: id таска, который вернул значение в xcom
        xcom_key: ключ xcom.
            По умолчанию это XCOM_RETURN_KEY. Однако если предыдущий таск использовать xcom_push, то нужно указать ключ с которым происходил push значения

    Examples:
        Предположим предыдущий таск называется my_prev_task и он вернул значение {"dag_name": "my_dag"}
        В таком случае функция принтанёт это значение в логи
        >>> @task()
            @cx_print_xcom(task_ids="my_prev_task")
            @pipe(pipe_stage=PipeStage.After)
            def my_task():
                pass
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ContextPrintXComModule(
                builder.context_key,
                builder.template_render,
                task_ids=task_ids,
                xcom_key=xcom_key,
                jinja_render=jinja_render,
                print_if=print_if,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


if __name__ == "__main__":
    from pathlib import Path
    from airflow.decorators import task, dag

    from airflow.providers.gun.pipe import pipe

    @dag(
        dag_id=f"{Path(__file__).stem}",
        schedule=None,
        render_template_as_native_obj=True
    )
    def gen_dag():

        @task()
        @cx_print("{{ my_key }}")
        @cx_save("my_key", "{{ dag.dag_id }}", jinja_render=False)
        @pipe(pipe_stage=PipeStage.After)
        def cx_save_without_jinja_render_test():
            pass

        @task()
        @cx_print("{{ my_key }}")
        @cx_save("my_key", "{{ dag.dag_id }}", jinja_render=True)
        @pipe(pipe_stage=PipeStage.After)
        def cx_save_with_jinja_render_test():
            pass

        @task()
        @cx_print_result(jinja_render=False)
        @pipe(pipe_stage=PipeStage.After)
        def cx_print_result_without_jinja_render_test():
            return "{{ 1+2 }}"

        @task()
        @cx_print_result(jinja_render=True)
        @pipe(pipe_stage=PipeStage.After)
        def cx_print_result_with_jinja_render_test():
            return {
                "dag_name": "{{ dag.dag_id }}",
                "sum": "{{ 1+2 }}",
            }

        @task()
        @cx_render_result()
        @pipe(pipe_stage=PipeStage.After)
        def cx_render_jinja_result_test():
            return {
                "dag_name": "{{ dag.dag_id }}",
                "sum": "{{ 1+2 }}",
            }

        @task()
        @cx_print("{{ prev_task_result }}")
        @cx_save_from_xcom("prev_task_result", "cx_render_jinja_result_test")
        @pipe(pipe_stage=PipeStage.After)
        def cx_save_from_xcom_test():
            pass

        @task()
        @cx_print_xcom(task_ids="cx_print_jinja_result_with_jinja_render_test")
        @cx_print_xcom(task_ids="cx_render_jinja_result_test")
        @pipe(pipe_stage=PipeStage.After)
        def cx_print_xcom_test():
            pass

        @task()
        @cx_print_result()
        @pipe(pipe_stage=PipeStage.After)
        def cx_save_if_not_none_test():
            return {"my_key": "my_key"}
        
        _ = (
            cx_save_without_jinja_render_test()
            >> cx_save_with_jinja_render_test()
            >> cx_print_result_without_jinja_render_test()
            >> cx_print_result_with_jinja_render_test()
            >> cx_render_jinja_result_test()
            >> cx_render_jinja_result_test()
            >> cx_save_from_xcom_test()
            >> cx_print_xcom_test()
            >> cx_save_if_not_none_test()
        )

    my_dag = gen_dag()

    my_dag.test()