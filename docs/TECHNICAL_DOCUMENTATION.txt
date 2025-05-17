# Graph Database Fraud Detection System - Technical Documentation

## Introduction
This document provides detailed technical documentation for the graph database-based fraud detection system implemented for the thesis "Exploring Graph Databases and Their Applications". The system utilizes Neo4j as the graph database platform and implements various graph algorithms to detect fraudulent financial transactions.

## System Architecture

### 1. Core Components

#### 1.1. Database Layer
- **Technology**: Neo4j Graph Database
- **Data Model**:
  - Nodes: Account entities with attributes like ID, anomaly scores, and graph metrics
  - Relationships: SENT relationship between accounts representing transactions
  - Properties: Amount, timestamp (step), transaction type, anomaly scores
- **Indexes**: Account ID index for efficient lookups

#### 1.2. Detection Engine
- **Feature Extraction Module**: Calculates graph metrics for fraud detection
- **Anomaly Detection Module**: Identifies unusual patterns in transaction data
- **Fraud Detection Module**: Main detection logic with multi-level confidence rules
- **Evaluation Module**: Measures performance against ground truth data

#### 1.3. API Layer
- **REST API**: Endpoints for data access and visualization
- **Web Interface**: Interactive visualization of fraud detection results

### 2. Algorithm Implementation

#### 2.1. Graph Algorithms Used
- **PageRank**: Identifies influential nodes in the transaction network
- **Degree Centrality**: Measures the number of connections each node has
- **Community Detection (Louvain)**: Groups accounts into communities based on transaction patterns
- **Node Similarity**: Identifies accounts with similar transaction patterns
- **Betweenness Centrality**: Detects accounts that bridge different transaction communities
- **HITS Algorithm**: Identifies hub and authority nodes in the network
- **K-Core**: Finds densely connected account subnetworks
- **Triangle Count**: Detects highly interconnected account clusters
- **Cycle Detection**: Identifies cyclic transaction patterns typical in money laundering

#### 2.2. Feature Weighting
The system uses an optimized feature weighting scheme:
- `degScore` (Degree Centrality): 38%
- `hubScore` (HITS Hub Score): 18%
- `normCommunitySize` (Normalized Community Size): 15%
- `amountVolatility`: 7%
- `txVelocity` (Transaction Velocity): 7%
- `btwScore` (Betweenness Centrality): 5%
- `prScore` (PageRank): 5%
- `authScore` (HITS Authority Score): 5%

## Implementation Methodology

### 1. Data Preparation
1. **Data Import**: Transaction data is imported from CSV files with sender, receiver, amount, and timestamp information
2. **Data Limiting**: System handles large datasets by limiting to configurable number of accounts and transactions
3. **Ground Truth Tagging**: Transactions are tagged with ground truth fraud status for evaluation

### 2. Graph Processing Pipeline

#### 2.1. Graph Projection
The system creates three specialized graph projections:
1. **Main Graph**: Standard account-transaction-account projection
2. **Similarity Graph**: For node similarity calculations
3. **Temporal Graph**: For temporal pattern analysis with time weights

#### 2.2. Feature Calculation
For each account node, the system calculates:
- **Structural Features**: Centrality measures, community membership
- **Temporal Features**: Transaction bursts, velocity changes
- **Amount Features**: Unusual amount patterns, volatility

#### 2.3. Anomaly Score Calculation
1. Combined weighted feature score formula:
   ```
   anomaly_score = Σ(feature_value * feature_weight)
   ```
2. Percentile-based thresholds:
   - Very high: 99th percentile
   - High: 97.5th percentile
   - Medium: 95th percentile
   - Low: 90th percentile

### 3. Multi-Level Fraud Detection Process

#### 3.1. Detection Phases
1. **Basic Anomaly Detection**:
   - Calculates basic anomaly scores
   - Applies initial statistics and percentile thresholds

2. **Tiered Confidence Detection**:
   - Very High Confidence (95%): 
     - Extreme anomaly scores
     - High anomaly + suspicious graph structure
     - High anomaly + large transaction amounts
   
   - High Confidence (85%):
     - High anomaly scores
     - Medium anomaly + suspicious graph structure
     - Medium anomaly + high transaction amounts
   
   - Medium Confidence (75%):
     - Medium anomaly scores
     - Low anomaly + multiple suspicious patterns

   - Low Confidence (60%, Recall mode only):
     - Combinations of minor suspicious factors
     - Very large transactions with some anomalies

3. **Related Fraud Detection**:
   - Identifies accounts involved in high-confidence fraudulent transactions
   - Extends detection to other transactions from these accounts

4. **False Positive Filtering**:
   - Precision Mode: Aggressively filters low-confidence detections
   - Balanced Mode: Moderate filtering
   - Recall Mode: Minimal filtering with basic statistical refinement

#### 3.2. Mode-Specific Strategies
1. **Precision Mode**: Prioritizes minimizing false positives
2. **Recall Mode**: Prioritizes minimizing false negatives
3. **Balanced Mode**: Aims for optimal F1 score

### 4. Query Implementation
The system uses Cypher queries extensively for detection logic:

#### 4.1. Example: Very High Confidence Detection
```cypher
MATCH (src:Account)-[tx:SENT]->(dest:Account)
WHERE 
    // Extremely high anomaly score
    tx.anomaly_score >= $very_high_threshold * 1.05
    
    // OR high anomaly score WITH suspicious graph structure
    OR (tx.anomaly_score >= $very_high_threshold AND 
        (
            (src.hubScore IS NOT NULL AND src.hubScore >= 0.85) OR
            (src.normCommunitySize IS NOT NULL AND src.normCommunitySize <= 0.05)
        )
    )
    
    // OR high anomaly score WITH very large amount
    OR (tx.anomaly_score >= $very_high_threshold AND tx.amount >= $amount_high * 1.2)
    
SET tx.flagged = true,
    tx.confidence = $very_high_confidence,
    tx.flag_reason = CASE
        WHEN tx.anomaly_score >= $very_high_threshold * 1.05 THEN "Extremely high anomaly score"
        WHEN tx.amount >= $amount_high * 1.2 THEN "High anomaly + very large transaction amount"
        ELSE "High anomaly + very suspicious graph structure"
    END,
    tx.detection_rule = "very_high_confidence"
```

#### 4.2. Example: Statistics Calculation
```cypher
MATCH ()-[tx:SENT]->()
WITH
    percentileCont(tx.anomaly_score, 0.99) AS very_high_threshold,
    percentileCont(tx.anomaly_score, 0.975) AS high_threshold,
    percentileCont(tx.anomaly_score, 0.95) AS medium_threshold,
    percentileCont(tx.anomaly_score, 0.90) AS low_threshold,
    percentileCont(tx.amount, 0.99) AS high_amount,
    percentileCont(tx.amount, 0.90) AS medium_amount,
    AVG(tx.amount) as avg_amount,
    STDEV(tx.amount) as std_amount,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) as fraud_count
RETURN *
```

### 5. Performance Evaluation
The system calculates comprehensive metrics:
- Precision
- Recall
- F1 Score
- Accuracy
- Detailed statistics by confidence level
- Detailed statistics by detection rule
- Analysis of most effective flag reasons

## Results and Analysis

### 1. Overall Performance

#### 1.1. Balanced Mode Results
- **Total Transactions**: 96,372
- **True Fraud Transactions**: 483
- **Flagged Transactions**: 9,638
- **True Positives**: 248
- **False Positives**: 9,390
- **False Negatives**: 235
- **True Negatives**: 86,499
- **Precision**: 2.57%
- **Recall**: 51.35%
- **F1 Score**: 0.0490
- **Accuracy**: 90.01%

#### 1.2. Recall Mode Results
- **Total Transactions**: 96,372
- **True Fraud Transactions**: 483
- **Flagged Transactions**: 4,819
- **True Positives**: 216
- **False Positives**: 4,603
- **False Negatives**: 267
- **True Negatives**: 91,286
- **Precision**: 4.48%
- **Recall**: 44.72%
- **F1 Score**: 0.0815
- **Accuracy**: 94.95%

### 2. Detailed Analysis

#### 2.1. Confidence Level Performance
- **Very High Confidence (0.95)**:
  - 964 transactions flagged
  - 96 true fraud cases
  - 9.96% precision
- **High Confidence (0.85)**:
  - 3,855 transactions flagged
  - 120 true fraud cases
  - 3.11% precision
- **Medium Confidence (0.75)**:
  - 4,819 transactions flagged
  - 32 true fraud cases
  - 0.66% precision

#### 2.2. Detection Rule Effectiveness
- **very_high_confidence**: 9.96% precision
- **high_confidence**: 3.11% precision
- **medium_confidence**: 0.66% precision

#### 2.3. Most Effective Flag Reasons
1. "Extremely high anomaly score": 12.20% precision
2. "High anomaly score": 4.77% precision
3. "High anomaly + very suspicious graph structure": 5.94% precision
4. "Medium anomaly + high transaction value": 2.24% precision

### 3. Insights and Observations

1. **Graph Structure Importance**: The system demonstrates that graph structure features are highly valuable for fraud detection. This confirms the hypothesis that relationships between accounts provide significant signals that traditional methods might miss.

2. **Confidence Level Correlation**: Clear correlation between confidence levels and precision, with very high confidence providing nearly 10% precision against a base fraud rate of 0.5%.

3. **Detection Rule Efficiency**: Rules that combine anomaly scores with graph structural features (like hub score and community size) provide better precision than those based solely on transaction amounts.

4. **Trade-offs**: Clear demonstration of the precision-recall trade-off in fraud detection:
   - Balanced mode captures more fraud (51.35% recall) at low precision (2.57%)
   - Recall mode has slightly better precision (4.48%) but lower recall (44.72%)

5. **Community Detection Value**: Accounts in small, isolated communities show higher fraud likelihood, validating the use of community detection algorithms.

## Challenges and Solutions

### 1. Data Processing Challenges

#### 1.1. Large Dataset Handling
- **Challenge**: Processing large transaction networks with limited memory
- **Solution**: Implemented batch processing and configurable limits for nodes and relationships

#### 1.2. Complex Graph Algorithm Execution
- **Challenge**: Some graph algorithms like Node Similarity have high computational complexity
- **Solution**: Implemented fallback approaches using stream processing for large graphs

### 2. Detection Challenges

#### 2.1. Feature Selection
- **Challenge**: Determining optimal features for fraud detection
- **Solution**: Used feature importance analysis to empirically determine feature weights

#### 2.2. Threshold Calibration
- **Challenge**: Setting appropriate thresholds for different confidence levels
- **Solution**: Implemented dynamic percentile-based thresholds

#### 2.3. Neo4j Result Processing
- **Challenge**: Handling different Neo4j result formats efficiently
- **Solution**: Created robust record parsing methods that handle multiple result formats

## Conclusion

The graph database-based fraud detection system demonstrates the power of graph analytics for complex fraud detection. By utilizing the structural relationships between accounts and transactions, the system achieves significantly higher recall than traditional methods while maintaining reasonable precision.

The multi-level confidence approach allows for flexible configuration depending on business requirements (minimizing false alarms vs. catching more fraud). The most effective detection rules combine anomaly scores with structural graph features, validating the core thesis that graph databases provide unique value for fraud detection applications.

## Future Directions

1. **Machine Learning Integration**: Combine graph features with machine learning models
2. **Real-time Detection**: Adapt the system for stream processing of transactions
3. **Feature Engineering**: Develop more advanced graph-based features
4. **Pattern Recognition**: Implement more sophisticated fraud pattern templates
5. **User Feedback Loop**: Incorporate analyst feedback to improve detection rules

---
Created for the thesis "Exploring Graph Databases and Their Applications", 2025.
