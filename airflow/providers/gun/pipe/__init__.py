from ctypes import Union
import inspect
import random
import string
from typing import Callable, Optional, Sequence, Any, Callable, Mapping
from enum import Enum

from contextlib import ExitStack
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from airflow.utils.operator_helpers import KeywordParameters


__all__ = [
    "pipe",
    "pipe_switch",
    "pipe_airflow_task",
    "pipe_airflow_callback",
    "pipe_airflow_callback_args",
    "PipeTaskBuilder",
    "PipeStage",
]


class PipeStage(str, Enum):
    """
    Этап выполнения модуля, может быть два варианта:

        Before - выполняется перед декорируемой функцией

        After - выполняется после декорируемой функции
    """

    Before = "before"
    After = "after"

    @staticmethod
    def from_str(label):
        if label in ("before", "Before", "BEFORE"):
            return PipeStage.Before
        elif label in ("after", "After", "After"):
            return PipeStage.After
        else:
            raise NotImplementedError


class PipeTask:
    def __init__(self, context_key: str):
        self.template_fields: Sequence[str] = ()
        self.context_key = context_key
        self.template_render = lambda val, context: val
        self.task_result_key = "task_res"

    def __call__(self, context):
        pass

    def set_template_fields(self, template_fields: Sequence[str]):
        self.template_fields = template_fields

    def set_template_render(self, template_render: Callable):
        self.template_render = template_render

    def render_template_fields(self, context):
        for name in self.template_fields:
            val = getattr(self, name)
            val = self.template_render(val, context)
            setattr(self, name, val)


class PipeTaskBuilder(PipeTask):
    def __init__(
        self,
        context_key: str,
        decorated_module: Callable,
        pipe_stage_default: PipeStage,
    ):
        super().__init__(context_key)

        random_suffix = "".join(random.choices(string.ascii_uppercase, k=5))
        self.stack_key = f"{decorated_module.__name__}_stack_{random_suffix}"

        self.before_modules = []
        self.decorated_module = decorated_module
        self.after_modules = []
        self.pipe_stage_default = pipe_stage_default

    def add_module(self, module, pipe_stage: Optional[PipeStage] = None):
        """Добавляет модуль в конвейр обработки
        Если pipe_stage не указан, то pipe_stage берется по умолчанию pipe_stage_default
        """
        if pipe_stage:
            m = (
                self.before_modules
                if pipe_stage == PipeStage.Before
                else self.after_modules
            )
        else:
            m = (
                self.before_modules
                if self.pipe_stage_default == PipeStage.Before
                else self.after_modules
            )

        m.append(module)

    def determine_kwargs(
        self, op_args, context: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return KeywordParameters.determine(
            self.decorated_module, op_args, context
        ).unpacking()

    def __call__(self, *args, **kwargs):
        context = get_current_context()

        for module in self.before_modules:
            if not context.get(module.context_key):
                context[module.context_key] = {}

            module(context)

        op_args = args
        op_kwargs = self.determine_kwargs(op_args, context)
        res_decor = self.decorated_module(*op_args, **op_kwargs)
        context[self.task_result_key] = res_decor

        for module in self.after_modules:
            if not context.get(module.context_key):
                context[module.context_key] = {}

            module(op_kwargs)

        return res_decor


def pipe_switch(context_key: str):
    def wrapper(pipe: PipeTaskBuilder):
        pipe.context_key = context_key
        return pipe

    return wrapper


class PipeAirflowTaskBuilder(PipeTaskBuilder):
    def __init__(
        self,
        context_key: str,
        decorated_module: Callable,
        pipe_stage_default: PipeStage,
    ):
        super().__init__(context_key, decorated_module, pipe_stage_default)

        def render_template(val, context):
            task = context["task"]
            jinja_env = task.get_template_env()
            val = task.render_template(
                val,
                context,
                jinja_env,
                set(),
            )
            return val

        self.set_template_render(render_template)
        self.decorated_module = decorated_module

        self.__name__ = decorated_module.__name__
        self.__doc__ = decorated_module.__doc__
        self.__annotations__ = decorated_module.__annotations__

    def __call__(self, *args, **kwargs):
        context = get_current_context()
        self.render_template_fields(context)

        with ExitStack() as stack:
            context[self.stack_key] = stack

            for module in self.before_modules:
                if not context.get(module.context_key):
                    context[module.context_key] = {}

                module(context)

            op_args = args
            op_kwargs = self.determine_kwargs(op_args, context)

            # следующий код добавлен для обратной совместимости
            # ранее первым аргументов всегда был context, сейчас можно написать метод которые не принимает аргумент context
            # так как используется вызов метода из op_kwargs = self.determine_kwargs(op_args, context)
            # поэтому я вынужден сделать сравнение первого аргумента в args, если это переменная с именем context, то передаю туда текущий контекст airflow
            # как долго будет присутствовать этот hot fix неизвестно
            sig = inspect.signature(self.decorated_module)
            if len(sig.parameters) > 0:
                first_arg = next(iter(sig.parameters))
                if first_arg == "context":
                    op_args = (context,) + op_args

            res_decor = self.decorated_module(*op_args, **op_kwargs)
            context[self.task_result_key] = res_decor

            for module in self.after_modules:
                if not context.get(module.context_key):
                    context[module.context_key] = {}

                module(context)

            res_decor = context[self.task_result_key]
            
            return res_decor


def pipe_airflow_task(context_key: str = "pipe", pipe_stage=PipeStage.Before):
    def wrapper(module):
        builder = PipeAirflowTaskBuilder(context_key, module, pipe_stage)
        return builder

    return wrapper


class PipeAirflowCallbackBuilder(PipeAirflowTaskBuilder):
    def __init__(
        self,
        context_key: str,
        decorated_module: Callable,
        pipe_stage_default,
    ):
        super().__init__(context_key, decorated_module, pipe_stage_default)

    def __call__(self, context):
        random_suffix = "".join(random.choices(string.ascii_uppercase, k=5))
        self.stack_key = f"{self.decorated_module.__name__}_stack_{random_suffix}"

        self.render_template_fields(context)

        with ExitStack() as stack:
            context[self.stack_key] = stack

            for module in self.before_modules:
                if not context.get(module.context_key):
                    context[module.context_key] = {}

                module(context)

            res_decor = self.decorated_module(context)
            context[self.task_result_key] = res_decor

            for module in self.after_modules:
                if not context.get(module.context_key):
                    context[module.context_key] = {}

                module(context)

            return res_decor


class PipeAirflowCallbackArgsBuilder(PipeAirflowCallbackBuilder):
    def __init__(
        self,
        context_key: str,
        decorated_module: Callable,
        pipe_stage_default: PipeStage,
    ):
        super().__init__(context_key, decorated_module, pipe_stage_default)

    def __call__(self, *args, **kwargs):
        def wrapper_decorated_module(context):
            args_ = self.template_render(args, context)
            kwargs_ = self.template_render(kwargs, context)

            return self.decorated_module(context, *args_, **kwargs_)

        wrapper_decorated_module.__name__ = self.decorated_module.__name__
        wrapper_decorated_module.__doc__ = self.decorated_module.__doc__
        wrapper_decorated_module.__annotations__ = self.decorated_module.__annotations__

        wrapper = PipeAirflowCallbackBuilder(
            context_key=self.context_key,
            decorated_module=wrapper_decorated_module,
            pipe_stage_default=self.pipe_stage_default,
        )

        wrapper.before_modules = self.before_modules
        wrapper.after_modules = self.after_modules

        return wrapper


def pipe_airflow_callback(
    context_key: str = "pipe", pipe_stage: PipeStage = PipeStage.Before
):
    def wrapper(module):
        builder = PipeAirflowCallbackBuilder(context_key, module, pipe_stage)
        return builder

    return wrapper


def pipe_airflow_callback_args(
    context_key: str = "pipe", pipe_stage: PipeStage = PipeStage.Before
):
    def wrapper(module):
        builder = PipeAirflowCallbackArgsBuilder(context_key, module, pipe_stage)
        return builder

    return wrapper


class PipeDecoratorCollection:
    switch = staticmethod(pipe_switch)
    airflow_task = staticmethod(pipe_airflow_task)
    airflow_callback = staticmethod(pipe_airflow_callback_args)

    __call__: Any = airflow_task


pipe = PipeDecoratorCollection()
