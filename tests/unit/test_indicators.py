"""Comprehensive unit tests for technical indicator functions."""

import pytest

from src.common.indicators import (
    calculate_bollinger_bands,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
    detect_crossover,
    detect_volume_anomaly,
)


# ---------------------------------------------------------------------------
# calculate_sma
# ---------------------------------------------------------------------------
class TestCalculateSMA:
    """Tests for the Simple Moving Average function."""

    def test_sma_basic(self, sample_prices: list[float]) -> None:
        """SMA-5 on a 30-element series returns a float."""
        result = calculate_sma(sample_prices, 5)
        assert result is not None
        assert isinstance(result, float)

    def test_sma_known_value(self) -> None:
        """SMA of [2, 4, 6, 8, 10] with period 5 should be 6.0."""
        assert calculate_sma([2, 4, 6, 8, 10], 5) == 6.0

    def test_sma_period_1(self) -> None:
        """SMA with period 1 returns the last price."""
        assert calculate_sma([10.0, 20.0, 30.0], 1) == 30.0

    @pytest.mark.parametrize("prices", [[], [100.0], [100.0, 200.0]])
    def test_sma_insufficient_data(self, prices: list[float]) -> None:
        """Returns None when fewer data points than the period."""
        assert calculate_sma(prices, 5) is None

    def test_sma_all_same_values(self) -> None:
        """SMA of identical values is that value."""
        assert calculate_sma([42.0] * 20, 10) == 42.0

    def test_sma_invalid_period(self) -> None:
        """Raises ValueError for period < 1."""
        with pytest.raises(ValueError, match="period must be >= 1"):
            calculate_sma([1.0, 2.0], 0)

    def test_sma_exact_length(self) -> None:
        """SMA when data length equals period."""
        assert calculate_sma([10.0, 20.0, 30.0], 3) == 20.0


# ---------------------------------------------------------------------------
# calculate_ema
# ---------------------------------------------------------------------------
class TestCalculateEMA:
    """Tests for the Exponential Moving Average function."""

    def test_ema_basic(self, sample_prices: list[float]) -> None:
        """EMA-12 on a 30-element series returns a float."""
        result = calculate_ema(sample_prices, 12)
        assert result is not None
        assert isinstance(result, float)

    def test_ema_period_equals_length(self) -> None:
        """EMA when data length equals period returns the mean (SMA seed)."""
        prices = [10.0, 20.0, 30.0]
        result = calculate_ema(prices, 3)
        assert result is not None
        assert result == pytest.approx(20.0)

    @pytest.mark.parametrize("prices", [[], [100.0]])
    def test_ema_insufficient_data(self, prices: list[float]) -> None:
        """Returns None when fewer data points than the period."""
        assert calculate_ema(prices, 5) is None

    def test_ema_all_same_values(self) -> None:
        """EMA of identical values equals that value."""
        result = calculate_ema([50.0] * 20, 10)
        assert result is not None
        assert result == pytest.approx(50.0)

    def test_ema_invalid_period(self) -> None:
        """Raises ValueError for period < 1."""
        with pytest.raises(ValueError, match="period must be >= 1"):
            calculate_ema([1.0], 0)

    def test_ema_reacts_to_recent(self) -> None:
        """EMA should weight recent prices more heavily than SMA does."""
        prices = [10.0] * 10 + [20.0] * 5
        ema = calculate_ema(prices, 10)
        sma = calculate_sma(prices, 10)
        assert ema is not None and sma is not None
        # EMA reacts faster, so should be closer to 20
        assert ema > sma


# ---------------------------------------------------------------------------
# calculate_rsi
# ---------------------------------------------------------------------------
class TestCalculateRSI:
    """Tests for the Relative Strength Index function."""

    def test_rsi_basic(self, sample_prices: list[float]) -> None:
        """RSI-14 on a 30-element series returns a value in [0, 100]."""
        result = calculate_rsi(sample_prices, 14)
        assert result is not None
        assert 0.0 <= result <= 100.0

    def test_rsi_all_gains(self) -> None:
        """Monotonically increasing prices produce RSI = 100."""
        prices = [float(i) for i in range(1, 20)]
        result = calculate_rsi(prices, 14)
        assert result is not None
        assert result == 100.0

    def test_rsi_all_losses(self) -> None:
        """Monotonically decreasing prices produce RSI near 0."""
        prices = [float(20 - i) for i in range(20)]
        result = calculate_rsi(prices, 14)
        assert result is not None
        assert result < 5.0

    def test_rsi_flat_prices(self) -> None:
        """Flat prices (no change) produce RSI = 100 (no losses)."""
        prices = [100.0] * 20
        result = calculate_rsi(prices, 14)
        assert result is not None
        # avg_gain == 0 and avg_loss == 0 → avg_loss is 0, returns 100
        assert result == 100.0

    @pytest.mark.parametrize("n", [1, 5, 14])
    def test_rsi_insufficient_data(self, n: int) -> None:
        """Returns None with fewer than period + 1 points."""
        prices = [100.0] * n
        assert calculate_rsi(prices, 14) is None

    def test_rsi_invalid_period(self) -> None:
        """Raises ValueError for period < 1."""
        with pytest.raises(ValueError, match="period must be >= 1"):
            calculate_rsi([1.0, 2.0], 0)


# ---------------------------------------------------------------------------
# calculate_macd
# ---------------------------------------------------------------------------
class TestCalculateMACD:
    """Tests for the MACD function."""

    def test_macd_basic(self) -> None:
        """MACD with enough data returns a 3-tuple."""
        prices = [float(100 + i) for i in range(50)]
        result = calculate_macd(prices)
        assert result is not None
        macd_line, signal, histogram = result
        assert isinstance(macd_line, float)
        assert isinstance(signal, float)
        assert histogram == pytest.approx(macd_line - signal)

    def test_macd_insufficient_data(self) -> None:
        """Returns None with insufficient data (< slow + signal)."""
        prices = [100.0] * 30
        assert calculate_macd(prices) is None

    def test_macd_flat_prices(self) -> None:
        """Flat prices produce MACD near zero."""
        prices = [100.0] * 50
        result = calculate_macd(prices)
        assert result is not None
        macd_line, signal, histogram = result
        assert macd_line == pytest.approx(0.0, abs=1e-10)
        assert signal == pytest.approx(0.0, abs=1e-10)
        assert histogram == pytest.approx(0.0, abs=1e-10)

    def test_macd_invalid_period(self) -> None:
        """Raises ValueError for invalid periods."""
        with pytest.raises(ValueError, match="all periods must be >= 1"):
            calculate_macd([1.0] * 50, fast_period=0)

    def test_macd_uptrend(self) -> None:
        """Uptrend produces a positive MACD line."""
        prices = [float(50 + i * 2) for i in range(50)]
        result = calculate_macd(prices)
        assert result is not None
        assert result[0] > 0  # MACD line positive in uptrend


# ---------------------------------------------------------------------------
# calculate_bollinger_bands
# ---------------------------------------------------------------------------
class TestCalculateBollingerBands:
    """Tests for the Bollinger Bands function."""

    def test_bb_basic(self, sample_prices: list[float]) -> None:
        """Bollinger Bands return upper > middle > lower."""
        result = calculate_bollinger_bands(sample_prices)
        assert result is not None
        upper, middle, lower = result
        assert upper > middle > lower

    def test_bb_flat_prices(self) -> None:
        """Flat prices: upper == middle == lower (zero std dev)."""
        prices = [100.0] * 20
        result = calculate_bollinger_bands(prices)
        assert result is not None
        upper, middle, lower = result
        assert upper == pytest.approx(100.0)
        assert middle == pytest.approx(100.0)
        assert lower == pytest.approx(100.0)

    def test_bb_insufficient_data(self) -> None:
        """Returns None with fewer than period data points."""
        assert calculate_bollinger_bands([1.0] * 10, period=20) is None

    def test_bb_invalid_period(self) -> None:
        """Raises ValueError for period < 2."""
        with pytest.raises(ValueError, match="period must be >= 2"):
            calculate_bollinger_bands([1.0] * 5, period=1)

    def test_bb_negative_std(self) -> None:
        """Raises ValueError for negative num_std."""
        with pytest.raises(ValueError, match="num_std must be >= 0"):
            calculate_bollinger_bands([1.0] * 20, num_std=-1.0)

    def test_bb_zero_std_multiplier(self) -> None:
        """Zero std multiplier makes all bands equal to the middle."""
        prices = [100.0, 110.0, 105.0] * 7  # 21 prices
        result = calculate_bollinger_bands(prices, period=20, num_std=0.0)
        assert result is not None
        upper, middle, lower = result
        assert upper == pytest.approx(middle)
        assert lower == pytest.approx(middle)


# ---------------------------------------------------------------------------
# detect_volume_anomaly
# ---------------------------------------------------------------------------
class TestDetectVolumeAnomaly:
    """Tests for the volume anomaly detection function."""

    def test_anomaly_detected(self, sample_volumes: list[int]) -> None:
        """Last element of sample_volumes (5M) is clearly anomalous."""
        assert detect_volume_anomaly(sample_volumes) is True

    def test_no_anomaly(self) -> None:
        """Normal volumes should not trigger an anomaly."""
        volumes = [1_000_000] * 25
        assert detect_volume_anomaly(volumes) is False

    def test_insufficient_data(self) -> None:
        """Returns False when there isn't enough data."""
        assert detect_volume_anomaly([100, 200], period=20) is False

    def test_invalid_period(self) -> None:
        """Raises ValueError for period < 1."""
        with pytest.raises(ValueError, match="period must be >= 1"):
            detect_volume_anomaly([100], period=0)

    def test_invalid_threshold(self) -> None:
        """Raises ValueError for negative threshold."""
        with pytest.raises(ValueError, match="threshold must be >= 0"):
            detect_volume_anomaly([100] * 25, threshold=-1.0)

    def test_zero_average_volume(self) -> None:
        """When prior volumes are all zero, any positive latest is anomalous."""
        volumes = [0] * 21 + [100]
        assert detect_volume_anomaly(volumes, period=20) is True

    def test_zero_average_volume_zero_latest(self) -> None:
        """When all volumes including latest are zero, no anomaly."""
        volumes = [0] * 22
        assert detect_volume_anomaly(volumes, period=20) is False

    def test_custom_threshold(self) -> None:
        """Custom threshold detects anomaly at lower multiplier."""
        volumes = [1_000_000] * 21 + [1_600_000]
        assert detect_volume_anomaly(volumes, period=20, threshold=1.5) is True
        assert detect_volume_anomaly(volumes, period=20, threshold=2.0) is False


# ---------------------------------------------------------------------------
# detect_crossover
# ---------------------------------------------------------------------------
class TestDetectCrossover:
    """Tests for the golden/death cross detector."""

    def test_golden_cross(self) -> None:
        """Short crossing above long is a golden cross."""
        short = [90.0, 110.0]
        long = [100.0, 100.0]
        assert detect_crossover(short, long) == "golden_cross"

    def test_death_cross(self) -> None:
        """Short crossing below long is a death cross."""
        short = [110.0, 90.0]
        long = [100.0, 100.0]
        assert detect_crossover(short, long) == "death_cross"

    def test_no_crossover(self) -> None:
        """No crossover when relative positions don't change."""
        short = [110.0, 120.0]
        long = [100.0, 100.0]
        assert detect_crossover(short, long) is None

    def test_no_crossover_parallel(self) -> None:
        """No crossover when both series move identically."""
        short = [100.0, 105.0]
        long = [100.0, 105.0]
        assert detect_crossover(short, long) is None

    def test_insufficient_data_short(self) -> None:
        """Raises ValueError when short series has < 2 elements."""
        with pytest.raises(ValueError, match="at least 2 elements"):
            detect_crossover([100.0], [100.0, 110.0])

    def test_insufficient_data_long(self) -> None:
        """Raises ValueError when long series has < 2 elements."""
        with pytest.raises(ValueError, match="at least 2 elements"):
            detect_crossover([100.0, 110.0], [100.0])

    def test_longer_series(self) -> None:
        """Only last two elements matter for detection."""
        short = [80.0, 85.0, 90.0, 110.0]
        long = [100.0, 100.0, 100.0, 100.0]
        assert detect_crossover(short, long) == "golden_cross"

    def test_equal_then_cross(self) -> None:
        """Crossing from equal position counts as a crossover."""
        short = [100.0, 101.0]
        long = [100.0, 100.0]
        assert detect_crossover(short, long) == "golden_cross"
