from datetime import datetime, timedelta


def last_day_of_month(date):
    next_month = date.replace(day=28) + timedelta(days=4)  # Move to the next month
    return next_month - timedelta(days=next_month.day)

    return last_day


def convert_to_last_day_of_quarter(input_str: str, quater=True) -> str:
    """
    Converts Roman date to last day of month

    Args:
        input_str (str) : roman date

    Returns:
        str: last date of quarter

    """
    month_roman, year_str = input_str.split(" ")
    year = int(year_str)

    if quater:
        # Convert Roman numeral month to numeric month
        roman_numerals = {"I": 3, "II": 6, "III": 9, "IV": 12}
        month = roman_numerals[month_roman]
    else:
        roman_numerals = {
            "I": 1,
            "II": 2,
            "III": 3,
            "IV": 4,
            "V": 5,
            "VI": 6,
            "VII": 7,
            "VIII": 8,
            "IX": 9,
            "X": 10,
            "XI": 11,
            "XII": 12,
        }
        month = roman_numerals[month_roman]

    # Get the last day of the corresponding quarter
    last_day = last_day_of_month(datetime(year, month, 28))

    return last_day.strftime("%Y-%m-%d")


def create_array_first_last_day_of_year(
    start_year: str, end_year: str
) -> list[str, str]:
    """
    This creates array of dates.

    Args:
        start_year (str): date start.
        end_year (str): date end.

    Returns:
        list[str, str]: list of dates.
    """
    year_range = range(start_year, end_year + 1)
    date_array = []

    for year in year_range:
        first_day = datetime(year, 1, 1)
        last_day = datetime(year, 12, 31)
        if last_day > datetime.now():
            last_day = datetime.now()
        date_array.append(
            (first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d"))
        )

    return date_array


def generate_days_in_years(year_start: int, year_end: int) -> list:
    """
    Creates array of dates

    Args:
        year_start (int) : start of data range
        year_end (int) : start of data range
    Returns:
        list: list of date in data range

    """
    start_date = datetime(year_start, 1, 1)
    end_date = datetime(year_end, 12, 31)

    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(
            (
                current_date.strftime("%Y-%m-%d"),
                last_day_of_month(current_date).strftime("%Y-%m-%d"),
            )
        )
        current_date += timedelta(days=1)

    return date_list


# print(generate_days_in_year(2006,2023))
