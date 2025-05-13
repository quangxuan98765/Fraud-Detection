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
FRAUD_SCORE_THRESHOLD = 0.72     # Significantly lowered to improve detection
SUSPICIOUS_THRESHOLD = 0.45      # Lowered to catch more potential patterns
HIGH_RISK_THRESHOLD = 0.82       # Adjusted down for better detection
VERY_HIGH_RISK_THRESHOLD = 0.92  # Lowered but still strict for highest risk

# Pattern detection thresholds
MIN_CONFIDENCE_LEVEL = 0.2       # Lowered to consider more patterns
BURST_CONFIDENCE = 0.6           # Adjusted for better sensitivity
CHAIN_CONFIDENCE = 0.55          # Lowered to detect more chain patterns
FUNNEL_CONFIDENCE = 0.5          # Lowered for improved funnel detection
VELOCITY_THRESHOLD = 2           # Lowered to detect more high velocity accounts
ROUND_AMOUNT_MIN = 1000          # Lowered to catch more round transactions
CHAIN_TIME_WINDOW = 72          # Increased window for chain detection
MIN_CHAIN_LENGTH = 3            # Lowered to catch shorter chains
FUNNEL_MIN_SOURCES = 3          # Lowered to detect more funnel patterns
BURST_WINDOW_HOURS = 2          # Increased window for burst detection

# Enhanced detection configuration
MULE_DETECTION_ENABLED = True
TEMPORAL_ANALYSIS_ENABLED = True
COMPLEX_PATTERN_DETECTION = True

# Trọng số mô hình
MODEL1_WEIGHT = 0.25             # Trọng số cho mô hình cấu trúc mạng
MODEL2_WEIGHT = 0.55             # Trọng số cho mô hình hành vi
MODEL3_WEIGHT = 0.20             # Trọng số cho mô hình mẫu phức tạp