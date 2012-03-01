// EMA = Exponential Moving Average
// EMA helps to smooth the price curve for better trend identification, placing greater importance on recent data.
// Recursive, assumes an SMA to start and finishes with the (close of current bar - previous EMA) * smoothing constant.
function calculateEMA(period) {
	if (bars.length < period) {
		return 0;
	}
	var smoothingConstant = 2.0/(1+period);
	var currentEMA = 0;
	for (var i = bars.length-period; i < bars.length; i++) {
		var current = bars[i];
		if (currentEMA == 0) {
			currentEMA = current['close'];
			continue;
		}
		currentEMA = current['close']*smoothingConstant + (1-smoothingConstant)*currentEMA;
	}
	return Math.round(currentEMA*1000)/1000;
}

// RSI = Relative Strength Index
// RSI is defined as 100 - (100 / (1 + RS)), where RS is Average Gain / Average Loss
// Thus, we sum up the total up periods and total down periods and average them for the RS calculation.
function calculateRSI(period) {
	if (bars.length < period+1) {
		return 0;
	}
	var upPeriodSummation = 0, downPeriodSummation = 0;
	for (var i = bars.length-period; i < bars.length; i++) {
		var current = bars[i], previous = null;
		if (i > 0) {
			previous = bars[i-1];
		}
		if (null != previous) {
			if (previous['close'] > current['close']) {
				downPeriodSummation += previous['close'] - current['close'];
			}
			else if (previous['close'] < current['close']) {
				upPeriodSummation += current['close'] - previous['close'];
			}
		}
	}   
	var avgUp = upPeriodSummation / period;
	var avgDown = downPeriodSummation / period;
	// If the average loss is equal to zero, RSI is defined to be 100 (also causes divide by zero error)
	if (avgDown == 0) {
		return 100;
	}
	var RS = avgUp / avgDown;
	var RSI = 100 - (100.0 / (1 + RS))
	return Math.round(RSI*1000)/1000;
}

// DI = Directional Index | The sign indicates direction (positive vs negative)
// DI+ is defined as the sum of positive directional moves (DM+) / sum of TR
// DI- is defined as the sum of negative directional moves (DM-) / sum of TR
function calculateDI(period, sign) {
	if (bars.length < period+1) {
		return 0;
	}
	var isDIPlus = false;
	if (sign == '+') {
		isDIPlus = true;
	}
	var totalDM = 0, totalTR = 0;
	for (var i = (bars.length-period); i < bars.length; i++) {
		var current = bars[i], previous = null;
		if (i > 0) {
			previous = bars[i-1];
		}
		totalTR += calculateTR(current, previous);
		if (null == previous) {
			continue;
		}
		var deltaHigh = current['high'] - previous['high'];
		var deltaLow = previous['low'] - current['low'];
		// DM+ and DM- are defined to be 0 for an inside interval and/or when their differences are equal
		if ((deltaHigh < 0 && deltaLow < 0) || deltaHigh == deltaLow) {
			continue;
		}
		var minusDM = 0, plusDM = 0;
		// Positive directional move, short circuit if this is not DI+
		if (isDIPlus && deltaHigh > deltaLow) {
			plusDM = deltaHigh;
		}
		// Negative directional move, short circuit if this is not DI-
		else if (!isDIPlus && deltaHigh < deltaLow) {
			minusDM = deltaLow;
		}
		// Something we don't care about (short circuit)
		else { continue; }

		if (isDIPlus) { totalDM += plusDM; }
		else { totalDM += minusDM; }
	}
	return Math.round((totalDM/totalTR*100)*1000)/1000;
}

// ATR = Average True Range
// Currently implemented as the sum of TR's for the interval / the interval (an average of TR's for the interval).
// ATR is technically a recursive calculation, but right now we're using 34 out of 34 bars to compute the first ATR.
// TODO: We could make it more accurate later by incorporating historical data.
function calculateATR(period) {
	if (bars.length < period) {
		return 0;
	}
	var summationTR = 0;
	for (var i = bars.length-period; i < bars.length; i++) {
		var current = bars[i], previous = null;
		if (i > 0) {
			previous = bars[i-1];
		}
		summationTR += calculateTR(current, previous);
	}
	return Math.round((summationTR/period)*1000)/1000;
}

// TR = True Range
// True Range is defined as the greatest out of:
//  1.  Today's high-low
//  2.  Today's low - yesterday's close (absolute value)
//  3.  Today's high - yesterday's close (absolute value)
function calculateTR(currentBar, previousBar) {
	// Assume TR is simply high-low (base case for the first bar)
	var TR = Math.abs(currentBar['high'] - currentBar['low']);
	// If we have a previous bar...
	if (previousBar != null) {
		var high_minus_closePrevious = Math.abs(currentBar['high'] - previousBar['close']);
		var low_minus_closePrevious = Math.abs(currentBar['low'] - previousBar['close']);
		TR = Math.max(TR, high_minus_closePrevious, low_minus_closePrevious);
	}
	return TR;
}
