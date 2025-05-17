# Graph Database Fraud Detection System

## Overview
This project implements an advanced financial fraud detection system using graph database technology (Neo4j) and graph algorithms to identify suspicious transactions in payment networks. The system leverages the structural relationships between accounts and transactions to detect complex fraud patterns that traditional methods might miss.

## Features
- **Graph-Based Analysis**: Utilizes the power of graph algorithms (PageRank, Community Detection, Node Similarity, etc.) to detect suspicious patterns
- **Multi-Level Detection**: Implements a tiered approach with different confidence levels for fraud detection
- **Flexible Operation Modes**: Supports precision-focused, recall-focused, and balanced detection modes
- **Performance Metrics**: Detailed evaluation with precision, recall, F1 score, and accuracy metrics
- **Interactive Visualization**: Web interface to explore and analyze the fraud detection results
- **Comprehensive Analysis**: Detailed statistics on detection rules, confidence levels, and flag reasons

## Tech Stack
- **Database**: Neo4j Graph Database
- **Backend**: Python with Neo4j Python Driver
- **Algorithms**: Graph Data Science (GDS) library for Neo4j
- **Frontend**: HTML/CSS/JavaScript with D3.js for graph visualization
- **API**: Flask-based REST API

## Project Structure
```
├── detector/                # Core detection modules
│   ├── queries/             # Neo4j Cypher queries
│   └── utils/               # Utility functions
├── routes/                  # API endpoints
├── static/                  # Frontend assets
├── templates/               # HTML templates
├── uploads/                 # Data upload directory
└── final_fraud_detection.py # Main application
```

## Detection Process
1. **Graph Structure Analysis**: Building account and transaction network
2. **Feature Extraction**: Calculating graph-based metrics like degree centrality, PageRank, etc.
3. **Anomaly Scoring**: Assigning anomaly scores based on multiple features
4. **Multi-Level Detection**: Applying different thresholds for confidence levels
5. **Pattern Recognition**: Identifying suspicious patterns and transaction chains
6. **Performance Evaluation**: Measuring detection effectiveness against ground truth

## Performance
- **Balanced Mode**: 51.35% recall with 2.57% precision
- **Recall-Focused Mode**: 44.72% recall with 4.48% precision
- **Best Detection Rules**:
  - Very High Confidence: 9.96% precision
  - High Anomaly Score: 12.20% precision

## Usage
```bash
# Run with default balanced mode
python final_fraud_detection.py

# Run with specific mode
python final_fraud_detection.py --mode precision
python final_fraud_detection.py --mode recall

# Skip basic anomaly detection (use existing scores)
python final_fraud_detection.py --skip-basic
```

## Requirements
- Python 3.6+
- Neo4j Database 5.0+
- Neo4j GDS Library 2.0+
- Python packages: neo4j, pandas, flask (see requirements.txt)

## Future Improvements
- Implementation of more advanced graph algorithms
- Incorporation of machine learning models
- Real-time fraud detection capabilities
- Enhanced visualization tools
