import pendulum
import logging
from typing import Optional, Union
from datetime import datetime

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage
from airflow.providers.gun.period.shift_period_generator import ShiftPeriodGenerator


save_to_default = "period_gen"


class ShiftPeriodGeneratorModel:
    def __init__(self, gen: ShiftPeriodGenerator) -> None:
        self.gen = gen
        self._period_from, self._period_to = self.gen.generate()
    
    def from_now(self):
        period_from, period_to = self.gen.from_now()
        self._period_from = period_from
        self._period_to = period_to

    def from_point(self, time_point: Union[datetime, pendulum.DateTime]):
        period_from, period_to = self.gen.from_time(time_point)
        self._period_from = period_from
        self._period_to = period_to

    @property
    def period_from(self):
        return self._period_from
    
    @property
    def period_from_str(self):
        return self.period_from.to_iso8601_string()

    @property
    def period_to(self):
        return self._period_to
    
    @property
    def period_to_str(self):
        return self.period_to.to_iso8601_string()
    
    def __str__(self):
        return f"""--------
{self.gen}
period_from: {self.period_from_str}
period_to:   {self.period_to_str}
--------"""
    
    def __repr__(self):
        return f"""ShiftPeriodGeneratorModel(gen={repr(self.gen)},
    period_from='{self.period_from_str}',
    period_to='{self.period_to_str}')"""

class PrintPeriodGeneratorModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ()
        )
        super().set_template_render(template_render)

        self.save_to = save_to
        self.log = logging.getLogger(self.__class__.__name__)

    def __call__(self, context):
        self.render_template_fields(context)
        share = context[self.context_key]

        gen: ShiftPeriodGeneratorModel = share.get(self.save_to)
        if gen is None:
            raise ValueError(f"Period generator not found in context")

        self.log.info(gen)


def period_print(
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Принтует в stdout периоды
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrintPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ShiftPeriodGeneratorModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        shift,
        period,
        timezone,
        start_of,
        end_of,
        from_point,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "shift", "period", "timezone", "start_of", "end_of", "from_point"
            )
        )
        super().set_template_render(template_render)

        self.save_to = save_to
        self.shift = shift
        self.period = period
        self.timezone = timezone
        self.start_of = start_of
        self.end_of = end_of
        self.from_point = from_point

    def __call__(self, context):
        self.render_template_fields(context)
        # share = context[self.context_key]

        gen = ShiftPeriodGenerator(
            self.shift,
            self.period,
            self.timezone,
            self.start_of,
            self.end_of,
            self.from_point,
        )
        gen = ShiftPeriodGeneratorModel(gen)

        context[self.save_to] = gen


def period_generator(
    shift: Optional[int | str] = None,
    period: Optional[str] = None,
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[Union[str, datetime, pendulum.DateTime]] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Генерирует период на основе переданных параметров
    
    Args:
        shift: Сдвиг в {period} от текущей даты или даты переданной в from_point
        period: Период, на который нужно сдвинуть дату
        timezone: Часовой пояс
        start_of: Указание на смещение левой границы периода
        end_of: Указание на смещение правой границы периода
            Допустимые значения:
                - cur: не смещает границу (по умолчанию)
                - year, month, week, day, hour, minute, second, microsecond: смещают на начало указанного периода
                - next_year, next_month, next_week, next_day, next_hour, next_minute, next_second, next_microsecond: смещают на начало следующего периода

    Returns:
        period_from, period_to: Начало и окончание периода

    Examples:
        >>> ("# Последние 30 секунд",
            ShiftPeriodGenerator(
                shift=-30,
                period='second',
            )),
            
            ("# Последние 15 минут",
            ShiftPeriodGenerator(
                shift=-15,
                period='minute',
            )),
            
            ("# Последние 6 часов",
            ShiftPeriodGenerator(
                shift=-6,
                period='hour',
            )),
            
            ("# Текущий месяц",
            ShiftPeriodGenerator(
                start_of='month',
                end_of='next_month'
            )),

            ("# Предыдущий месяц",
            ShiftPeriodGenerator(
                shift=-1,
                period='month',
                start_of='month',
                end_of='month'
            )),

            ("# Последний месяц",
            ShiftPeriodGenerator(
                shift=-1,
                period='month',
                start_of='cur',
                end_of='cur'
            )),

            ("# С начала предыдущего месяца по теукщий момент",
            ShiftPeriodGenerator(
                shift=-1,
                period='month',
                start_of='month',
                end_of='cur'
            )),

            ("# С начала предыдущего месяца по конец текущего месяца",
            ShiftPeriodGenerator(
                shift=-1,
                period='month',
                start_of='month',
                end_of='next_month'
            )),

            ("# Текущий год",
            ShiftPeriodGenerator(
                shift=0,
                period='year',
                start_of='year',
                end_of='next_year'
            )),

            ("# Последний год",
            ShiftPeriodGenerator(
                shift=-1,
                period='year',
            )),

            ("# Последние 10 лет",
            ShiftPeriodGenerator(
                shift=-10,
                period='year',
            )),

            ("# Следующий год",
            ShiftPeriodGenerator(
                shift=2,
                period='year',
                start_of='next_year',
                end_of='year',
            )),

            ("# С начала года по конец следующего",
            ShiftPeriodGenerator(
                shift=2,
                period='year',
                start_of='year',
                end_of='year',
            )),
"""

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period=period,
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_minute(
    timezone: str = 'Europe/Moscow',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=None,
                period=None,
                timezone=timezone,
                start_of='minute',
                end_of='next_minute',
                from_point='now',
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_minute(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='minute',
                timezone=timezone,
                start_of='minute',
                end_of='minute',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_minutes(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='minute',
                timezone=timezone,
                start_of='minute',
                end_of='minute',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_minute(
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='minute',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_minutes(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='minute',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_this_hour(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=None,
                period=None,
                timezone=timezone,
                start_of='hour',
                end_of='next_hour',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_hour(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='hour',
                timezone=timezone,
                start_of='hour',
                end_of='hour',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_hours(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='hour',
                timezone=timezone,
                start_of='hour',
                end_of='hour',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_hour(
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='hour',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_hours(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='hour',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_this_day(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=None,
                period=None,
                timezone=timezone,
                start_of='day',
                end_of='next_day',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_day(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='day',
                timezone=timezone,
                start_of='hour',
                end_of='day',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_days(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='day',
                timezone=timezone,
                start_of='day',
                end_of='day',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_day(
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='day',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_days(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='day',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_this_week(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=None,
                period=None,
                timezone=timezone,
                start_of='week',
                end_of='next_week',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_week(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='week',
                timezone=timezone,
                start_of='week',
                end_of='week',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_weeks(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='week',
                timezone=timezone,
                start_of='week',
                end_of='week',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_week(
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='week',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_weeks(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='week',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_this_month(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=None,
                period=None,
                timezone=timezone,
                start_of='month',
                end_of='next_month',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_month(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='month',
                timezone=timezone,
                start_of='month',
                end_of='month',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_months(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='month',
                timezone=timezone,
                start_of='month',
                end_of='month',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_month(
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='month',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_months(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='month',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_year(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=None,
                period=None,
                timezone=timezone,
                start_of='year',
                end_of='next_year',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_year(
    timezone: str = 'Europe/Moscow',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='year',
                timezone=timezone,
                start_of='year',
                end_of='year',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_prev_years(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='year',
                timezone=timezone,
                start_of='year',
                end_of='year',
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_year(
    timezone: str = 'Europe/Moscow',
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=-1,
                period='year',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

def period_last_years(
    timezone: str = 'Europe/Moscow',
    shift: int | str = -1,
    start_of: str = 'cur',
    end_of: str = 'cur',
    from_point: Optional[str | datetime | pendulum.DateTime] = 'now',
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                period='year',
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_point=from_point,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
