import pendulum
from typing import Optional, Union, Callable
from datetime import datetime

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage
from airflow.providers.gun.period.shift_period_generator import ShiftPeriodGenerator


save_to_default = "period"


class PeriodModel:
    def __init__(
        self,
        period_from: pendulum.DateTime,
        period_to: pendulum.DateTime,
        description: str,
    ):
        self._period_from = period_from
        self._period_to = period_to
        self._description = description

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

    @property
    def description(self):
        return self._description

    def __str__(self):
        return f"""--------
description: {self.description}
period_from: {self.period_from_str}
period_to:   {self.period_to_str}
--------"""


class ShiftPeriodGeneratorModule(PipeTask):
    """Позволяет сгенерировать модель периода на основе сдвига {shift} и базовой даты {from_time}
    Также с помощью start_of и end_of можно "примагнитить" начало и конец периода к определенному "начальному" значению

    Args:
        shift: сдвиг в в человекочитаемом формате. Необходимо указать строку состоящую из двух частей:
                - "1 day"       - сдвиг на 1 день вперед
                - "-1 day"      - сдвиг на 1 день назад
                - "-1 month"    - сдвиг на 1 назад
                - "1 week"      - сдвиг на 1 неделю назад
                - "1 month"     - сдвиг на 1 месяц вперед
        timezone: часовой пояс. Доступны все варианты из pendulum.timezones
        start_of, end_of: "примагничивание" начала периода. Можно отметить варианты:
            cur - означает current, т.е. не магнитить начало периода
            {"minute", "second", "hour", "day", "week", "month", "year"} - означает "примагнитить начало периода к началу указанного периода"
            {"next_minute", "next_second", "next_hour", "next_day", "next_week", "next_month", "next_year"} - означает "примагнитить конец периода к началу указанного периода"
        from_time: базовая дата от которой будет сдвигаться период на указанные в shift, если не указано то берется 'now'
    """

    def __init__(
        self,
        context_key,
        template_render,
        shift,
        timezone,
        start_of,
        end_of,
        from_time,
        save_if: Callable[[], bool] | bool | str,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ("shift", "timezone", "start_of", "end_of", "from_time")
        )
        super().set_template_render(template_render)

        self.save_to = save_to
        self.shift = shift
        self.timezone = timezone
        self.start_of = start_of
        self.end_of = end_of
        self.from_time = from_time
        self.save_if = save_if

    def __call__(self, context):
        if self.save_if_eval(context):
            self.render_template_fields(context)

            shift, period = self.period_parser(self.shift)

            gen = ShiftPeriodGenerator(
                shift=shift,
                period=period,
                timezone=self.timezone,
                start_of=self.start_of,
                end_of=self.end_of,
                from_time=self.from_time,
            )
            period_from, period_to = gen.generate()
            model = PeriodModel(
                period_from,
                period_to,
                """shift period generator:
    shift: {}
    period: {}
    timezone: {}
    start_of: {}
    end_of: {}
    from_time: {}""".format(
                    shift,
                    period,
                    self.timezone,
                    self.start_of,
                    self.end_of,
                    self.from_time,
                ),
            )

            context[self.save_to] = model

    def save_if_eval(self, context):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if()

        return save_if

    def period_parser(self, shift_period):
        shift, period = shift_period.split(" ")
        try:
            shift = int(shift)
        except ValueError:
            raise ValueError(
                f"shift must be integer, but got {shift}. shift_period: {shift_period}"
            )

        period = str(period)

        return shift, period


class ThisPeriodGeneratorModule(PipeTask):
    """Генерирует модель периода, который соответствует текущему времени
    Текущий месяц, текущий год, текущий день, текущий час, текущая минута, текущая секунда
    """

    def __init__(
        self,
        context_key,
        template_render,
        period,
        timezone,
        from_time,
        save_if: Callable[[], bool] | bool | str,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(("period", "timezone", "from_time"))
        super().set_template_render(template_render)

        self.save_to = save_to
        self.period = period
        self.timezone = timezone
        self.from_time = from_time
        self.save_if = save_if

    def __call__(self, context):
        if self.save_if_eval(context):
            self.render_template_fields(context)

            shift, period, start_of, end_of = self.period_parser(self.period)

            gen = ShiftPeriodGenerator(
                shift=shift,
                period=period,
                timezone=self.timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=self.from_time,
            )
            period_from, period_to = gen.generate()
            model = PeriodModel(
                period_from,
                period_to,
                """this period generator:
    shift: {}
    period: {}
    timezone: {}
    start_of: {}
    end_of: {}
    from_time: {}""".format(
                    shift, period, self.timezone, start_of, end_of, self.from_time
                ),
            )

            context[self.save_to] = model

    def save_if_eval(self, context):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if()

        return save_if

    def period_parser(self, period):
        res = period.split(" ")
        if len(res) > 1:
            raise ValueError(
                f"period must be one of the following: {ShiftPeriodGenerator.PERIOD_TYPES}"
            )

        return None, None, f"{period}", f"next_{period}"


class PrevPeriodGeneratorModule(PipeTask):
    """Генерирует модель периода, который смещен назад на указанный диапазон
    А также start_of и end_of "примагничены" к краям периода указанного в shift (например minute, hour, day, week, month, year)

    Args:
        shift: сдвиг периода, например 1 day, 1 week, 3 month, 1 year и так далее на указанный период назад
        timezone: часовой пояс
        from_time: время с которого начинается сдвиг периода, если не указано, то берется 'now'

    Examples:
        Например можно сгенерировать предыдущий месяц начало и окончания которого примагничены к началу и концу месяца
        если now -> 2024-02-05 12:00:00, то
        ```python
        >>> PrevPeriodGeneratorModule('1 month'): 2024-01-01 00:00:00 - 2024-02-01 00:00:00
        ""
        ```

    """

    def __init__(
        self,
        context_key,
        template_render,
        shift,
        timezone,
        from_time,
        save_if: Callable[[], bool] | bool | str,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(("shift", "timezone", "from_time"))
        super().set_template_render(template_render)

        self.save_to = save_to
        self.shift = shift
        self.timezone = timezone
        self.from_time = from_time
        self.save_if = save_if

    def __call__(self, context):
        if self.save_if_eval(context):
            self.render_template_fields(context)

            shift, period, start_of, end_of = self.period_parser(self.shift)

            gen = ShiftPeriodGenerator(
                shift=shift,
                period=period,
                timezone=self.timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=self.from_time,
            )
            period_from, period_to = gen.generate()
            model = PeriodModel(
                period_from,
                period_to,
                """prev period generator:
    shift: {}
    period: {}
    timezone: {}
    start_of: {}
    end_of: {}
    from_time: {}""".format(
                    shift,
                    period,
                    self.timezone,
                    start_of,
                    end_of,
                    self.from_time,
                ),
            )

            context[self.save_to] = model

    def save_if_eval(self, context):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if()

        return save_if

    def period_parser(self, shift_period):
        shift, period = shift_period.split(" ")
        try:
            # так как клас должен генерировать предыдущие периоды, то shift должен быть отрицательным
            # здесь происходит принудительное исправление на отрицательное значение
            shift = abs(int(shift))
            shift = -shift
        except ValueError:
            raise ValueError(
                f"shift must be integer, but got {shift}. original: {shift_period}"
            )

        period = str(period)

        start_of = period
        end_of = period

        return shift, period, start_of, end_of


class LastPeriodGeneratorModule(PipeTask):
    """Генерирует модель периода, который смещен назад на указанный диапазон
    А также start_of и end_of "примагничены" к краям периода указанного в shift (например minute, hour, day, week, month, year)

    Args:
        shift: сдвиг периода, например 1 day, 1 week, 3 month, 1 year и так далее на указанный период назад
        timezone: часовой пояс
        from_time: время с которого начинается сдвиг периода, если не указано, то берется 'now'

    Examples:
        Например можно сгенерировать предыдущий месяц начало и окончания которого примагничены к началу и концу месяца
        если now -> 2024-02-05 12:00:00, то
        >>> LastPeriodGeneratorModule('1 month'): 2024-02-05 12:00:00 - 2024-01-05 12:00:00

    """

    def __init__(
        self,
        context_key,
        template_render,
        shift,
        start_of,
        end_of,
        timezone,
        from_time,
        save_if: Callable[[], bool] | bool | str,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ("shift", "start_of", "end_of", "timezone", "from_time")
        )
        super().set_template_render(template_render)

        self.save_to = save_to
        self.shift = shift
        self.start_of = start_of
        self.end_of = end_of
        self.timezone = timezone
        self.from_time = from_time
        self.save_if = save_if

    def __call__(self, context):
        if self.save_if_eval(context):
            self.render_template_fields(context)

            shift, period, start_of, end_of = self.period_parser(
                self.shift, self.start_of, self.end_of
            )

            gen = ShiftPeriodGenerator(
                shift=shift,
                period=period,
                timezone=self.timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=self.from_time,
            )
            period_from, period_to = gen.generate()
            model = PeriodModel(
                period_from,
                period_to,
                """last period generator:
    shift: {}
    period: {}
    timezone: {}
    start_of: {}
    end_of: {}
    from_time: {}""".format(
                    shift,
                    period,
                    self.timezone,
                    start_of,
                    end_of,
                    self.from_time,
                ),
            )

            context[self.save_to] = model

    def save_if_eval(self, context):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if()

        return save_if

    def period_parser(self, shift_period, start_of, end_of):
        shift, period = shift_period.split(" ")
        try:
            # так как клас должен генерировать предыдущие периоды, то shift должен быть отрицательным
            # здесь происходит принудительное исправление на отрицательное значение
            shift = abs(int(shift))
            shift = -shift
        except ValueError:
            raise ValueError(
                f"shift must be integer, but got {shift}. original: {shift_period}"
            )

        period = str(period)

        return shift, period, start_of, end_of


class CustomPeriodGeneratorModule(PipeTask):
    """Генерирует модель периода из указанных дат

    Args:
        from_time: Дата начала периода
        to_time: Дата окончания периода
        timezone: Часовой пояс
        save_if: Условие сохранения периода. Если True, то период сохраняется в контекст Airflow, если False, то не сохраняется
        save_to: Ключ, по которому период будет сохранен в контекст Airflow. По умолчанию - 'period'

    from_time, to_time: Можно указать несколько вариантов:
        'now' - означает что необходимо взять текущее время
        '1970-01-01 00:00:00' - можно указать дату в формате YYYY-MM-DD HH:MM:SS
        pendulum.DateTime - можно указать объект pendulum.DateTime
        datetime.datetime - можно указать объект datetime.datetime
    """

    def __init__(
        self,
        context_key,
        template_render,
        from_time,
        to_time,
        timezone,
        save_if: Callable[[], bool] | bool | str,
        save_to: str = save_to_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(("from_time", "to_time", "timezone"))
        super().set_template_render(template_render)

        self.from_time = from_time
        self.to_time = to_time
        self.timezone = timezone
        self.save_if = save_if
        self.save_to = save_to

    def __call__(self, context):
        if self.save_if_eval(context):
            self.render_template_fields(context)

            from_time = self._validate_datetime(self.from_time, self.timezone)
            to_time = self._validate_datetime(self.to_time, self.timezone)

            model = PeriodModel(
                from_time,
                to_time,
                """custom period generator
    from_time: {}
    to_time: {}""".format(
                    from_time,
                    to_time,
                ),
            )

            context[self.save_to] = model

    def save_if_eval(self, context):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if()

        return save_if

    def _validate_datetime(
        self,
        from_point: Optional[Union[str, datetime, pendulum.DateTime]],
        timezone: str,
    ):
        match from_point:
            case None:
                dt = pendulum.now(timezone)
            case "now":
                dt = pendulum.now(timezone)
            case str():
                try:
                    dt = pendulum.parse(from_point, tz=timezone)
                except Exception:
                    raise ValueError(
                        f"Invalid datetime: pendulum.parse({from_point}, tz={timezone})"
                        f"Example valid datetime: '2023-01-01 00:00:00'"
                    )
            case x if isinstance(x, datetime):
                try:
                    dt = pendulum.instance(x, timezone)
                except Exception:
                    raise ValueError(
                        f"Invalid datetime: pendulum.instance({x}, {timezone})"
                        f"Example valid datetime: datetime(2023, 1, 1, 0, 0, 0, tzinfo=pendulum.timezone('Europe/Moscow'))"
                    )
            case x if isinstance(x, pendulum.DateTime):
                try:
                    dt = x.in_timezone(timezone)
                except Exception:
                    raise ValueError(
                        f"Invalid pendulum.DateTime: {x}.in_timezone({timezone})"
                        f"Example valid datetime: datetime(2023, 1, 1, 0, 0, 0, tzinfo=pendulum.timezone('Europe/Moscow'))"
                    )
            case _:
                raise ValueError(
                    f"Invalid _from value: {from_point}. "
                    f"Valid values: 'now', datetime object, pendulum.DateTime object"
                )

        if isinstance(dt, pendulum.DateTime):
            return dt
        else:
            raise ValueError(
                f"Invalid datetime: pendulum.parse({from_point}, tz={timezone})"
                f"Example valid datetime: '2023-01-01 00:00:00'"
            )


def period_shift(
    shift: Optional[str] = None,
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[Union[str, datetime, pendulum.DateTime]] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Генерирует период на основе переданных параметров

    Args:
        shift: различные варианты сдвига, например: -1 month, -1 week, -1 day, -1 hour, -1 minute, -1 second, -1 microsecond
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
            Последние 30 секунд
        >>> @period_shift(
                shift='-30 second',
            )

            Последние 15 минут
        >>> @period_shift(
                shift='-15 minute',
            )

            Последние 6 часов
        >>> @period_shift(
                shift='-6 hour',
            )

            Текущий месяц
        >>> @period_shift(
                start_of='month',
                end_of='next_month'
            )

            Предыдущий месяц
        >>> @period_shift(
                shift='-1 month',
                start_of='month',
                end_of='month'
            )

            Последний месяц
        >>> @period_shift(
                shift='-1 month',
                start_of='cur',
                end_of='cur'
            )

            С начала предыдущего месяца по теукщий момент
        >>> @period_shift(
                shift='-1 month',
                start_of='month',
                end_of='cur'
            )

            С начала предыдущего месяца по конец текущего месяца
        >>> @period_shift(
                shift='-1 month',
                start_of='month',
                end_of='next_month'
            )

            Текущий год
        >>> @period_shift(
                shift='0 year',
                start_of='year',
                end_of='next_year'
            )

            Последний год
        >>> @period_shift(
                shift='-1 year',
            )),

            Последние 10 лет
        >>> @period_shift(
                shift='-10 year',
            )

            Следующий год
        >>> @period_shift(
                shift='2 year',
                start_of='next_year',
                end_of='year',
            )

            С начала года по конец следующего
        >>> @period_shift(
                shift='2 year',
                start_of='year',
                end_of='year',
            )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ShiftPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this(
    period: str,
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period=period,
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last(
    shift: str,
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev(
    shift: str,
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=shift,
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_custom(
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    to_time: Optional[str | datetime | pendulum.DateTime] = "now",
    timezone: str = "Europe/Moscow",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            CustomPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                from_time=from_time,
                to_time=to_time,
                timezone=timezone,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_minute(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period="minute",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_minute(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="-1 minute",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_minutes(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} minute",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_minute(
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 minute",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_minutes(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} minute",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_hour(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period="hour",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_hour(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 hour",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_hours(
    shift: int | str = 1,
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} hour",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_hour(
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 hour",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_hours(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} hour",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_day(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period="day",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_day(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 day",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_days(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} day",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_day(
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 day",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_days(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} day",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_week(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period="week",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_week(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 week",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_weeks(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} week",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_week(
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 week",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_weeks(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} week",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_month(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period="month",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_month(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 month",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_months(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} month",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_month(
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 month",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_months(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} month",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_this_year(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ThisPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                period="year",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_year(
    timezone: str = "Europe/Moscow",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 year",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_prev_years(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PrevPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} year",
                timezone=timezone,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_year(
    timezone: str = "Europe/Moscow",
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift="1 year",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def period_last_years(
    timezone: str = "Europe/Moscow",
    shift: int | str = 1,
    start_of: str = "cur",
    end_of: str = "cur",
    from_time: Optional[str | datetime | pendulum.DateTime] = "now",
    save_if: Callable[[], bool] | bool | str = True,
    save_to: str = save_to_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            LastPeriodGeneratorModule(
                builder.context_key,
                builder.template_render,
                shift=f"{shift} year",
                timezone=timezone,
                start_of=start_of,
                end_of=end_of,
                from_time=from_time,
                save_if=save_if,
                save_to=save_to,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
