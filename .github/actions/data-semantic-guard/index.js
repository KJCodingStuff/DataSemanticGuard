const core = require('@actions/core');
const fs = require('fs');
const path = require('path');
const { SemanticCorruptionDetector } = require('./detector');

/**
 * Parse CSV file and extract numeric column
 */
function parseCSV(filePath, columnName) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.trim().split('\n');
  
  if (lines.length < 2) {
    throw new Error('CSV file must have header and at least one data row');
  }
  
  const headers = lines[0].split(',').map(h => h.trim());
  const columnIndex = headers.indexOf(columnName);
  
  if (columnIndex === -1) {
    throw new Error(`Column "${columnName}" not found in CSV. Available: ${headers.join(', ')}`);
  }
  
  const values = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(',');
    const value = parseFloat(cells[columnIndex]);
    if (!isNaN(value)) {
      values.push(value);
    }
  }
  
  return values;
}

/**
 * Parse JSON file and extract numeric values
 */
function parseJSON(filePath, columnName) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const data = JSON.parse(content);
  
  // Handle array of objects
  if (Array.isArray(data)) {
    const values = data
      .map(item => parseFloat(item[columnName]))
      .filter(val => !isNaN(val));
    
    if (values.length === 0) {
      throw new Error(`No numeric values found in column "${columnName}"`);
    }
    
    return values;
  }
  
  // Handle single object with array property
  if (typeof data === 'object' && Array.isArray(data[columnName])) {
    return data[columnName].filter(val => !isNaN(parseFloat(val))).map(v => parseFloat(v));
  }
  
  throw new Error('JSON must be array of objects or object with array property');
}

/**
 * Load baseline statistics or create new baseline
 */
function loadOrCreateBaseline(baselinePath, currentStats, currentData) {
  if (fs.existsSync(baselinePath)) {
    try {
      const content = fs.readFileSync(baselinePath, 'utf-8');
      const baseline = JSON.parse(content);
      core.info(`✓ Loaded baseline from ${baselinePath}`);
      return {
        stats: baseline.baseline_stats,
        sampleData: baseline.sample_data,
        isNew: false
      };
    } catch (error) {
      core.warning(`Failed to parse baseline file: ${error.message}`);
    }
  }
  
  // Create new baseline
  core.warning(`No baseline found at ${baselinePath}. Creating new baseline from current data.`);
  
  const baselineData = {
    created_at: new Date().toISOString(),
    baseline_stats: currentStats,
    sample_data: currentData.slice(0, 10)
  };
  
  // Ensure directory exists
  const dir = path.dirname(baselinePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    core.info(`✓ Created directory: ${dir}`);
  }
  
  fs.writeFileSync(baselinePath, JSON.stringify(baselineData, null, 2));
  core.info(`✓ Created new baseline at ${baselinePath}`);
  core.info(`✓ Baseline file size: ${fs.statSync(baselinePath).size} bytes`);
  
  return {
    stats: currentStats,
    sampleData: currentData.slice(0, 10),
    isNew: true
  };
}

/**
 * Generate artifact report
 */
function generateArtifact(detector, baselineStats, currentStats, currentData, metadata) {
  const artifact = {
    metadata: {
      timestamp: new Date().toISOString(),
      file_type: metadata.fileType,
      comparison_type: metadata.comparisonType,
      confidence_score: Math.round(detector.confidence),
      threshold: metadata.threshold
    },
    baseline_stats: baselineStats,
    current_stats: currentStats,
    detected_anomalies: detector.anomalies,
    diagnostics: detector.generateDiagnostics(),
    sample_before: metadata.sampleBefore || [],
    sample_after: currentData.slice(0, 10),
    status: detector.confidence >= metadata.threshold ? 'FAILED' : 'PASSED'
  };
  
  // Use new report directory structure
  const reportDir = path.join(process.cwd(), 'data', 'report');
  if (!fs.existsSync(reportDir)) {
    fs.mkdirSync(reportDir, { recursive: true });
  }
  
  const artifactPath = path.join(reportDir, 'silent-data-corruption-report.json');
  fs.writeFileSync(artifactPath, JSON.stringify(artifact, null, 2));
  
  return artifactPath;
}

/**
 * Main action logic
 */
async function run() {
  try {
    // Get inputs
    const fileType = core.getInput('file_type', { required: true });
    const fileName = core.getInput('file_name', { required: true });
    const filePath = 'data/raw/' + fileName;
    const comparisonType = core.getInput('comparison_type', { required: true });
    const baselineFile = 'data/baseline/' + fileName;
    const threshold = parseInt(core.getInput('confidence_threshold') || '90', 10);
    const numericColumn = core.getInput('numeric_column') || 'value';
    
    core.info('='.repeat(60));
    core.info('🛡️  Data Semantic Guard - Silent Corruption Detector');
    core.info('='.repeat(60));
    core.info(`File: ${filePath}`);
    core.info(`Type: ${fileType}`);
    core.info(`Comparison: ${comparisonType}`);
    core.info(`Threshold: ${threshold}%`);
    core.info('');
    
    // Validate file exists
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }
    
    // Parse data based on file type
    core.info('📊 Parsing data file...');
    let currentData;
    
    switch (fileType) {
      case 'csv':
        currentData = parseCSV(filePath, numericColumn);
        break;
      case 'json':
        currentData = parseJSON(filePath, numericColumn);
        break;
      case 'parquet':
        throw new Error('Parquet support requires additional dependencies. Use CSV or JSON.');
      default:
        throw new Error(`Unsupported file type: ${fileType}`);
    }
    
    core.info(`✓ Parsed ${currentData.length} numeric values`);
    
    // Calculate current statistics
    const detector = new SemanticCorruptionDetector(comparisonType);
    const currentStats = detector.calculateStats(currentData);
    
    core.info('');
    core.info('📈 Current Data Statistics:');
    core.info(`  Mean: ${currentStats.mean.toFixed(2)}`);
    core.info(`  Median: ${currentStats.median.toFixed(2)}`);
    core.info(`  Std Dev: ${currentStats.std_dev.toFixed(2)}`);
    core.info(`  Range: [${currentStats.min.toFixed(2)}, ${currentStats.max.toFixed(2)}]`);
    
    // Load or create baseline
    core.info('');
    const baseline = loadOrCreateBaseline(baselineFile, currentStats, currentData);
    
    if (baseline.isNew) {
      core.info('');
      core.info('ℹ️  First run detected. Baseline created. Exiting with success.');
      core.info(`📝 Baseline saved to: ${baselineFile}`);
      core.info('⚠️  IMPORTANT: Commit this baseline file to your repository!');
      core.info('   Run: git add data/baseline/ && git commit -m "Add data baseline"');
      core.setOutput('confidence_score', 0);
      core.setOutput('detected_anomalies', 0);
      core.setOutput('artifact_path', baselineFile);
      core.setOutput('baseline_created', 'true');
      return;
    }
    
    // Perform semantic drift analysis
    core.info('');
    core.info('🔍 Analyzing semantic drift...');
    
    const result = detector.analyze(baseline.stats, currentStats);
    
    core.info('');
    core.info('='.repeat(60));
    core.info(`📊 CONFIDENCE SCORE: ${Math.round(detector.confidence)}%`);
    core.info('='.repeat(60));
    
    if (result.detected) {
      core.info('');
      core.info(detector.generateDiagnostics());
    } else {
      core.info('✅ No semantic drift detected. Data appears consistent.');
    }
    
    // Generate artifact
    core.info('');
    core.info('💾 Generating artifact report...');
    
    const artifactPath = generateArtifact(detector, baseline.stats, currentStats, currentData, {
      fileType,
      comparisonType,
      threshold,
      sampleBefore: baseline.sampleData || []
    });
    
    core.info(`✓ Artifact saved to: ${artifactPath}`);
    core.info(`✓ File exists: ${fs.existsSync(artifactPath)}`);
    core.info(`✓ File size: ${fs.statSync(artifactPath).size} bytes`);
    
    // Set outputs
    core.setOutput('confidence_score', Math.round(detector.confidence));
    core.setOutput('detected_anomalies', detector.anomalies.length);
    core.setOutput('artifact_path', artifactPath);
    
    // Fail if confidence exceeds threshold
    if (detector.confidence >= threshold) {
      core.info('');
      core.info('='.repeat(60));
      core.error('❌ SILENT DATA CORRUPTION DETECTED');
      core.error(`Confidence ${Math.round(detector.confidence)}% exceeds threshold ${threshold}%`);
      core.info('='.repeat(60));
      core.setFailed(`Data corruption detected with ${Math.round(detector.confidence)}% confidence`);
    } else {
      core.info('');
      core.info('✅ Action completed successfully');
    }
    
  } catch (error) {
    core.setFailed(`Action failed: ${error.message}`);
  }
}

run();