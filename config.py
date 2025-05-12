# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

# Processing settings
BATCH_SIZE = 2000
MAX_NODES = 200000
MAX_RELATIONSHIPS = 400000

# Sampling settings
DEFAULT_TARGET_NODES = 180000
DEFAULT_TARGET_EDGES = 193500
DEFAULT_FRAUD_RATE = 0.00129

# Fraud detection thresholds
FRAUD_SCORE_THRESHOLD = 0.65     # Increased threshold for better precision
SUSPICIOUS_THRESHOLD = 0.45      # Increased threshold for suspicious accounts
HIGH_RISK_THRESHOLD = 0.80       # Increased threshold for high-risk accounts

# Enhanced detection configuration
MULE_DETECTION_ENABLED = True
TEMPORAL_ANALYSIS_ENABLED = True
COMPLEX_PATTERN_DETECTION = True