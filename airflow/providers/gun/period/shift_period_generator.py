from typing import Tuple, Union, Optional
import pendulum
from datetime import datetime

class ShiftPeriodGenerator:
    PERIOD_TYPES =  ['microsecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year']
    SHIFT_PERIODS = ['microsecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year']
    
    def __init__(
        self,
        shift: Optional[int] = None,
        period: Optional[str] = None,
        timezone: str = 'Europe/Moscow',
        start_of: str = 'cur',
        end_of: str = 'cur',
        from_point: Optional[Union[str, datetime, pendulum.DateTime]] = None,
    ):
        self._shift, self.period = self._validate_and_normalize_shift_period(shift, period)
        self._timezone = self._validate_and_normalize_timezone(timezone)
        self._start_of = self._validate_and_normalize_of(start_of, 'start_of')
        self._end_of = self._validate_and_normalize_of(end_of, 'end_of')
        self._from_point = self._validate_from(from_point, self._timezone)

    def __str__(self):
        period_info = f"{self._shift} {self.period}(s)" if self._shift is not None else "no shift"
        return f"ShiftPeriodGenerator({period_info}, timezone={self._timezone}, start_of={self._start_of}, end_of={self._end_of}, from_point={self._from_point})"
    
    def __repr__(self):
        return (f"ShiftPeriodGenerator(shift={self._shift}, period='{self.period}', "
                f"timezone='{self._timezone}', start_of='{self._start_of}', "
                f"end_of='{self._end_of}', from_point={repr(self._from_point)})")
    def _validate_from(self, from_point: Optional[Union[str, datetime, pendulum.DateTime]], timezone: str):
        match from_point:
            case None:
                return from_point
            case 'now':
                return from_point
            case x if isinstance(x, datetime):
                try:
                    pendulum.instance(x, timezone)
                    return from_point
                except Exception:
                    raise ValueError(
                        f"Invalid datetime: pendulum.instance({x}, {timezone})"
                        f"Example valid datetime: datetime(2023, 1, 1, 0, 0, 0, tzinfo=pendulum.timezone('Europe/Moscow'))"
                    )
            case x if isinstance(x, pendulum.DateTime):
                try:
                    x.in_timezone(timezone)
                    return from_point
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

    def _make_from_point(self, from_point: Optional[Union[str, datetime, pendulum.DateTime]], timezone: str):
        match from_point:
            case None:
                return pendulum.now(timezone)
            case 'now':
                return pendulum.now(timezone)
            case x if isinstance(x, datetime):
                return pendulum.instance(x, timezone)
            case x if isinstance(x, pendulum.DateTime):
                return x.in_timezone(timezone)
            case _:
                raise ValueError(
                    f"Invalid from_point value: {from_point}. "
                    f"Valid values: 'now', datetime object, pendulum.DateTime object"
                )

    def _validate_and_normalize_shift_period(self, shift: Optional[int], period: Optional[str]):
        if shift is None and period is None:
            return None, None
        elif period is None and shift is not None:
            raise ValueError(
                f"Period must be specified when shift is not None. period: {period}, shift: {shift}"
            )
        elif period is not None and shift is None:
            raise ValueError(
                f"Shift must be specified when period is not None. period: {period}, shift: {shift}"
            )
        else:
            if not isinstance(period, str):
                raise ValueError(
                    f"Period must be a string. Got: {type(period)}. "
                    f"Valid values: {', '.join(self.PERIOD_TYPES)}"
                )

            if not isinstance(shift, int):
                raise ValueError(
                    f"Shift must be an integer. Got: {type(shift)}"
                )

            normalized_period = self._normalize_period_type(period)
            if normalized_period not in self.PERIOD_TYPES:
                raise ValueError(
                    f"Invalid period: {period}. "
                    f"Valid values: {', '.join(self.PERIOD_TYPES)}"
                )

            return shift, normalized_period

    def _validate_and_normalize_timezone(self, timezone: Optional[str]):
        if timezone is None:
            timezone = 'UTC'
        
        try:
            pendulum.timezone(timezone)
        except Exception:
            raise ValueError(
                f"Invalid timezone: {timezone}. "
                "Example valid timezones: 'UTC', 'Europe/Moscow', 'America/New_York', another pendulum.timezone"
            )

        return timezone

    def _validate_and_normalize_of(self, of: str, param_name: str):
        if of is None:
            of = 'cur'
        
        valid_formats = (
            ['cur'] + 
            [f'{period}' for period in self.SHIFT_PERIODS] +
            [f'next_{period}' for period in self.SHIFT_PERIODS]
        )

        if of not in valid_formats:
            raise ValueError(
                f"Invalid {param_name}: {of}\n"
                f"Valid formats:\n"
                f"- 'cur' (current moment)\n"
                f"- '<period>' (begin of current period)\n"
                f"- 'next_<period>' (begin of next period)\n"
                f"where <period> is one of: {', '.join(self.SHIFT_PERIODS)}"
            )

        return of

    def _normalize_period_type(self, period_type: str) -> str:        
        period_mapping = {
            'microseconds': 'microsecond',
            'microsecond': 'microsecond',
            'seconds': 'second',
            'second': 'second',
            'minutes': 'minute',
            'minute': 'minute',
            'hours': 'hour',
            'hour': 'hour',
            'days': 'day',
            'day': 'day',
            'weeks': 'week',
            'week': 'week',
            'months': 'month',
            'month': 'month',
            'years': 'year',
            'year': 'year',
            'quarters': 'quarter',
            'quarter': 'quarter',
        }

        period_type = period_type.lower()

        return period_mapping.get(period_type, period_type)

    def _apply_correct(self, date: pendulum.DateTime, correct_period: str) -> pendulum.DateTime:
        if correct_period == 'cur':
            return date

        parts = correct_period.split('_')
        
        if len(parts) == 1 and parts[0] in self.SHIFT_PERIODS:
            return date.start_of(parts[0])
        elif len(parts) == 2 and parts[0] == 'next' and parts[1] in self.SHIFT_PERIODS:
            pendulum_shift = self._get_pendulum_shift(1, parts[1])
            date = date.add(**pendulum_shift)
            return date.start_of(parts[1])
        else:
            raise ValueError(
                f"Invalid shift period: {correct_period}. "
                f"Valid formats:\n"
                f"- 'cur' (current moment)\n"
                f"- '<period>' (begin of current period)\n"
                f"- 'next_<period>' (begin of next period)\n"
                f"where <period> is one of: {', '.join(self.SHIFT_PERIODS)}"
            )
    def _get_pendulum_period_type(self, period_type: Optional[str]) -> str:
        return f'{period_type}s'

    def _get_pendulum_shift(self, shift: int, period: str):
        return {
            self._get_pendulum_period_type(period): shift,
        }

    def _generate(self, base_date: pendulum.DateTime) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        if self._shift is not None and self.period is not None:
            pendulum_shift = self._get_pendulum_shift(self._shift, self.period)
            shift_date = base_date.add(**pendulum_shift)
            fix_date = base_date

            if self._shift > 0:
                start_date = fix_date
                end_date = shift_date
            else:
                start_date = shift_date
                end_date = fix_date
        else:
            start_date = base_date
            end_date = base_date
        
        start_date = self._apply_correct(start_date, self._start_of)
        end_date = self._apply_correct(end_date, self._end_of)

        return start_date, end_date
    
    def generate(self) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """Generate period"""
        return self._generate(self._make_from_point(self._from_point, self._timezone))
    
    def from_now(self) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """Generate period based on current time"""
        return self._generate(pendulum.now(tz=self._timezone))

    def from_time(self, time_point: Union[datetime, pendulum.DateTime]) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """Generate period based on specific time point"""
        from_point = self._validate_from(time_point, self._timezone)
        from_point = self._make_from_point(from_point, self._timezone)
        return self._generate(from_point)


if __name__ == "__main__":
    # Создаем генератор для последних 3 месяцев
    # generator = ShiftPeriodGenerator(
    #     shift=3,
    #     period='month',
    #     start_of='month',
    #     end_of='next_day'
    # )
    # # Генерация периода (если передан from_point, то от него иначе берется текущее время)
    # period_from, period_to = generator.generate()
    # # Генерация периода от текущего момента
    # period_from, period_to = generator.from_now()
    # # Генерация периода от конкретной точки
    # specific_date = pendulum.datetime(2023, 10, 15, 12, 30)
    # period_from, period_to = generator.from_time(specific_date)
    # # Также работает с datetime объектами
    # from datetime import datetime
    # dt = datetime(2023, 10, 15, 12, 30)
    # period_from, period_to = generator.from_time(dt)

    # Примеры с разными периодами и смещениями
    examples = [
        ("# Последние 30 секунд",
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
    ]

    # Получение периодов
    from_point = pendulum.now(tz="Europe/Moscow")
    print(f"now: ({from_point})")
    for ex in examples:
        period_from, period_to = ex[1].from_time(from_point)
        print(f"{ex[0]}: {period_from.to_iso8601_string()} - {period_to.to_iso8601_string()}")