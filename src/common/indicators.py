"""Pure functions for computing technical indicators.

All functions operate on plain Python lists of floats and return computed
indicator values. No side effects, no I/O — suitable for both Spark UDFs
and plain Python batch processing.
"""

import statistics


def calculate_sma(prices: list[float], period: int) -> float | None:
    """Calculate Simple Moving Average over the last *period* prices.

    Args:
        prices: Chronologically ordered closing prices.
        period: Number of periods to average.

    Returns:
        The SMA value, or None if fewer data points than *period*.

    Raises:
        ValueError: If period is less than 1.
    """
    if period < 1:
        raise ValueError("period must be >= 1")
    if len(prices) < period:
        return None
    return statistics.mean(prices[-period:])


def calculate_ema(prices: list[float], period: int) -> float | None:
    """Calculate Exponential Moving Average over the given prices.

    Uses the standard smoothing multiplier ``2 / (period + 1)``.

    Args:
        prices: Chronologically ordered closing prices.
        period: Number of periods for the EMA span.

    Returns:
        The EMA value, or None if fewer data points than *period*.

    Raises:
        ValueError: If period is less than 1.
    """
    if period < 1:
        raise ValueError("period must be >= 1")
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = statistics.mean(prices[:period])
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


def calculate_rsi(prices: list[float], period: int = 14) -> float | None:
    """Calculate Relative Strength Index.

    Uses the Wilder smoothing method (exponential moving average of
    gains and losses).

    Args:
        prices: Chronologically ordered closing prices. Needs at least
            ``period + 1`` data points.
        period: Look-back period (default 14).

    Returns:
        RSI value between 0 and 100, or None if insufficient data.

    Raises:
        ValueError: If period is less than 1.
    """
    if period < 1:
        raise ValueError("period must be >= 1")
    if len(prices) < period + 1:
        return None

    deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]

    gains = [d if d > 0 else 0.0 for d in deltas[:period]]
    losses = [-d if d < 0 else 0.0 for d in deltas[:period]]

    avg_gain = statistics.mean(gains)
    avg_loss = statistics.mean(losses)

    for d in deltas[period:]:
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calculate_macd(
    prices: list[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[float, float, float] | None:
    """Calculate MACD line, signal line, and histogram.

    Args:
        prices: Chronologically ordered closing prices.
        fast_period: Fast EMA period (default 12).
        slow_period: Slow EMA period (default 26).
        signal_period: Signal EMA period (default 9).

    Returns:
        Tuple of (macd_line, signal_line, histogram), or None if
        insufficient data.

    Raises:
        ValueError: If any period is less than 1.
    """
    if fast_period < 1 or slow_period < 1 or signal_period < 1:
        raise ValueError("all periods must be >= 1")

    min_length = slow_period + signal_period
    if len(prices) < min_length:
        return None

    macd_values: list[float] = []
    for i in range(slow_period, len(prices) + 1):
        subset = prices[:i]
        fast_ema = calculate_ema(subset, fast_period)
        slow_ema = calculate_ema(subset, slow_period)
        if fast_ema is not None and slow_ema is not None:
            macd_values.append(fast_ema - slow_ema)

    if len(macd_values) < signal_period:
        return None

    signal = calculate_ema(macd_values, signal_period)
    if signal is None:
        return None

    macd_line = macd_values[-1]
    histogram = macd_line - signal
    return (macd_line, signal, histogram)


def calculate_bollinger_bands(
    prices: list[float],
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[float, float, float] | None:
    """Calculate Bollinger Bands (upper, middle, lower).

    Args:
        prices: Chronologically ordered closing prices.
        period: Look-back period for the middle band SMA (default 20).
        num_std: Number of standard deviations for the bands (default 2.0).

    Returns:
        Tuple of (upper_band, middle_band, lower_band), or None if
        insufficient data.

    Raises:
        ValueError: If period is less than 2 or num_std is negative.
    """
    if period < 2:
        raise ValueError("period must be >= 2")
    if num_std < 0:
        raise ValueError("num_std must be >= 0")
    if len(prices) < period:
        return None

    window = prices[-period:]
    middle = statistics.mean(window)
    std_dev = statistics.stdev(window)
    upper = middle + num_std * std_dev
    lower = middle - num_std * std_dev
    return (upper, middle, lower)


def detect_volume_anomaly(
    volumes: list[int],
    period: int = 20,
    threshold: float = 2.0,
) -> bool:
    """Detect whether the latest volume is anomalously high.

    Args:
        volumes: Chronologically ordered volume values.
        period: Look-back period for the average (default 20).
        threshold: Multiplier to define anomaly (default 2.0).

    Returns:
        True if the latest volume exceeds ``threshold × average``.

    Raises:
        ValueError: If period is less than 1 or threshold is negative.
    """
    if period < 1:
        raise ValueError("period must be >= 1")
    if threshold < 0:
        raise ValueError("threshold must be >= 0")
    if len(volumes) < period + 1:
        return False

    avg_volume = statistics.mean(volumes[-(period + 1) : -1])
    if avg_volume == 0:
        return volumes[-1] > 0
    return volumes[-1] > threshold * avg_volume


def detect_crossover(
    short_series: list[float],
    long_series: list[float],
) -> str | None:
    """Detect golden cross or death cross between two moving average series.

    Compares the last two values of each series to check if a crossover
    occurred.

    Args:
        short_series: Shorter-period MA values (e.g. SMA-50).
        long_series: Longer-period MA values (e.g. SMA-200).

    Returns:
        ``"golden_cross"`` if short crossed above long,
        ``"death_cross"`` if short crossed below long,
        or None if no crossover.

    Raises:
        ValueError: If either series has fewer than 2 elements.
    """
    if len(short_series) < 2 or len(long_series) < 2:
        raise ValueError("both series must have at least 2 elements")

    prev_short, curr_short = short_series[-2], short_series[-1]
    prev_long, curr_long = long_series[-2], long_series[-1]

    if prev_short <= prev_long and curr_short > curr_long:
        return "golden_cross"
    if prev_short >= prev_long and curr_short < curr_long:
        return "death_cross"
    return None
