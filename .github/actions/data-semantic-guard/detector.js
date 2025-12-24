const fs = require('fs');
const path = require('path');

/**
 * Statistical baseline comparison for semantic drift detection
 */
class SemanticCorruptionDetector {
  constructor(comparisonType) {
    this.comparisonType = comparisonType;
    this.anomalies = [];
    this.confidence = 0;
    
    // Known conversion ratios for different comparison types
    this.conversionHeuristics = {
      'currency_usd_to_inr': { ratio: 83.0, tolerance: 0.15, description: 'USD to INR conversion' },
      'currency_usd_to_jpy': { ratio: 150.0, tolerance: 0.15, description: 'USD to JPY conversion' },
      'temperature_c_to_f': { ratio: 1.8, offset: 32, tolerance: 0.1, description: 'Celsius to Fahrenheit' },
      'length_m_to_ft': { ratio: 3.28084, tolerance: 0.05, description: 'Meters to Feet' },
      'custom_numeric_distribution': { tolerance: 0.2, description: 'Generic numeric distribution' }
    };
  }

  /**
   * Calculate statistical features from numeric data
   */
  calculateStats(data) {
    if (!data || data.length === 0) {
      throw new Error('Cannot calculate statistics on empty data');
    }

    const sorted = [...data].sort((a, b) => a - b);
    const n = data.length;
    
    const mean = data.reduce((sum, val) => sum + val, 0) / n;
    
    const median = n % 2 === 0 
      ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2
      : sorted[Math.floor(n / 2)];
    
    const variance = data.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / n;
    const stdDev = Math.sqrt(variance);
    
    const min = sorted[0];
    const max = sorted[n - 1];
    
    // Percentiles
    const p25 = sorted[Math.floor(n * 0.25)];
    const p75 = sorted[Math.floor(n * 0.75)];
    
    return {
      mean,
      median,
      std_dev: stdDev,
      min,
      max,
      p25,
      p75,
      count: n,
      variance
    };
  }

  /**
   * Detect magnitude shifts (order of magnitude changes)
   */
  detectMagnitudeShift(baseline, current) {
    const baselineMean = baseline.mean;
    const currentMean = current.mean;
    
    if (baselineMean === 0) return { detected: false, confidence: 0 };
    
    const ratio = currentMean / baselineMean;
    const logRatio = Math.log10(Math.abs(ratio));
    
    // Detect shifts greater than 0.5 orders of magnitude
    if (Math.abs(logRatio) > 0.5) {
      const percentChange = ((currentMean - baselineMean) / baselineMean * 100).toFixed(2);
      
      if (ratio < 0.1) {
        this.anomalies.push(`Severe magnitude drop: ${percentChange}% decrease (ratio: ${ratio.toFixed(4)})`);
        return { detected: true, confidence: 95 };
      } else if (ratio > 10) {
        this.anomalies.push(`Severe magnitude increase: ${percentChange}% increase (ratio: ${ratio.toFixed(4)})`);
        return { detected: true, confidence: 95 };
      } else if (ratio < 0.5 || ratio > 2.0) {
        this.anomalies.push(`Significant magnitude shift: ${percentChange}% change`);
        return { detected: true, confidence: 70 };
      }
    }
    
    return { detected: false, confidence: 0 };
  }

  /**
   * Detect unit conversion patterns
   */
  detectUnitConversion(baseline, current) {
    const heuristic = this.conversionHeuristics[this.comparisonType];
    if (!heuristic || !heuristic.ratio) return { detected: false, confidence: 0 };
    
    const baselineMean = baseline.mean;
    const currentMean = current.mean;
    
    if (baselineMean === 0) return { detected: false, confidence: 0 };
    
    const ratio = currentMean / baselineMean;
    const expectedRatio = heuristic.ratio;
    const tolerance = heuristic.tolerance;
    
    // Check if ratio matches expected conversion (within tolerance)
    const ratioError = Math.abs(ratio - expectedRatio) / expectedRatio;
    
    if (ratioError < tolerance) {
      this.anomalies.push(
        `Possible ${heuristic.description}: ratio ${ratio.toFixed(2)} matches expected ${expectedRatio.toFixed(2)}`
      );
      return { detected: true, confidence: 90 };
    }
    
    // Check inverse conversion
    const inverseRatio = 1 / ratio;
    const inverseError = Math.abs(inverseRatio - expectedRatio) / expectedRatio;
    
    if (inverseError < tolerance) {
      this.anomalies.push(
        `Possible inverse ${heuristic.description}: ratio ${inverseRatio.toFixed(2)} matches expected ${expectedRatio.toFixed(2)}`
      );
      return { detected: true, confidence: 90 };
    }
    
    return { detected: false, confidence: 0 };
  }

  /**
   * Detect distribution shape changes
   */
  detectDistributionShift(baseline, current) {
    // Compare coefficient of variation (CV = std_dev / mean)
    const baselineCV = baseline.std_dev / baseline.mean;
    const currentCV = current.std_dev / current.mean;
    
    if (!isFinite(baselineCV) || !isFinite(currentCV)) {
      return { detected: false, confidence: 0 };
    }
    
    const cvRatio = currentCV / baselineCV;
    
    if (cvRatio > 2.0 || cvRatio < 0.5) {
      const change = cvRatio > 1 ? 'increase' : 'decrease';
      this.anomalies.push(
        `Distribution variability ${change}: coefficient of variation changed by ${(Math.abs(cvRatio - 1) * 100).toFixed(1)}%`
      );
      return { detected: true, confidence: 60 };
    }
    
    // Compare median shifts relative to mean
    const baselineMedianMeanRatio = baseline.median / baseline.mean;
    const currentMedianMeanRatio = current.median / current.mean;
    
    const medianMeanShift = Math.abs(currentMedianMeanRatio - baselineMedianMeanRatio);
    
    if (medianMeanShift > 0.2) {
      this.anomalies.push(
        `Distribution skew changed: median/mean ratio shifted by ${(medianMeanShift * 100).toFixed(1)}%`
      );
      return { detected: true, confidence: 55 };
    }
    
    return { detected: false, confidence: 0 };
  }

  /**
   * Detect range expansion/contraction
   */
  detectRangeAnomaly(baseline, current) {
    const baselineRange = baseline.max - baseline.min;
    const currentRange = current.max - current.min;
    
    if (baselineRange === 0) return { detected: false, confidence: 0 };
    
    const rangeRatio = currentRange / baselineRange;
    
    if (rangeRatio > 5.0) {
      this.anomalies.push(
        `Data range expanded significantly: ${(rangeRatio).toFixed(1)}x larger than baseline`
      );
      return { detected: true, confidence: 65 };
    } else if (rangeRatio < 0.2) {
      this.anomalies.push(
        `Data range contracted significantly: ${(rangeRatio * 100).toFixed(0)}% of baseline range`
      );
      return { detected: true, confidence: 65 };
    }
    
    return { detected: false, confidence: 0 };
  }

  /**
   * Main detection pipeline
   */
  analyze(baselineStats, currentStats) {
    this.anomalies = [];
    const confidenceScores = [];
    
    // Run all detection methods
    const magnitudeResult = this.detectMagnitudeShift(baselineStats, currentStats);
    if (magnitudeResult.detected) confidenceScores.push(magnitudeResult.confidence);
    
    const conversionResult = this.detectUnitConversion(baselineStats, currentStats);
    if (conversionResult.detected) confidenceScores.push(conversionResult.confidence);
    
    const distributionResult = this.detectDistributionShift(baselineStats, currentStats);
    if (distributionResult.detected) confidenceScores.push(distributionResult.confidence);
    
    const rangeResult = this.detectRangeAnomaly(baselineStats, currentStats);
    if (rangeResult.detected) confidenceScores.push(rangeResult.confidence);
    
    // Aggregate confidence using weighted maximum
    if (confidenceScores.length === 0) {
      this.confidence = 0;
    } else if (confidenceScores.length === 1) {
      this.confidence = confidenceScores[0];
    } else {
      // Use max confidence with boost for multiple signals
      const maxConfidence = Math.max(...confidenceScores);
      const boost = Math.min((confidenceScores.length - 1) * 5, 15);
      this.confidence = Math.min(maxConfidence + boost, 100);
    }
    
    return {
      confidence: this.confidence,
      anomalies: this.anomalies,
      detected: this.anomalies.length > 0
    };
  }

  /**
   * Generate diagnostic message based on comparison type
   */
  generateDiagnostics() {
    const heuristic = this.conversionHeuristics[this.comparisonType];
    const diagnostics = [];
    
    if (this.confidence >= 90) {
      diagnostics.push('🚨 HIGH CONFIDENCE: Silent data corruption detected');
    } else if (this.confidence >= 70) {
      diagnostics.push('⚠️  MEDIUM CONFIDENCE: Suspicious data patterns detected');
    } else if (this.confidence >= 50) {
      diagnostics.push('ℹ️  LOW CONFIDENCE: Minor anomalies detected');
    } else {
      diagnostics.push('✅ No significant semantic drift detected');
    }
    
    if (heuristic) {
      diagnostics.push(`\nAnalysis type: ${heuristic.description}`);
    }
    
    if (this.anomalies.length > 0) {
      diagnostics.push('\nDetected anomalies:');
      this.anomalies.forEach((anomaly, idx) => {
        diagnostics.push(`  ${idx + 1}. ${anomaly}`);
      });
      
      diagnostics.push('\nCommon causes:');
      diagnostics.push('  - Unit conversion (currency, temperature, length)');
      diagnostics.push('  - Locale/region change in data source');
      diagnostics.push('  - Schema migration without data transformation');
      diagnostics.push('  - Upstream API changes');
      diagnostics.push('  - Data aggregation level changes');
    }
    
    return diagnostics.join('\n');
  }
}

module.exports = { SemanticCorruptionDetector };