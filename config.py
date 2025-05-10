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
FRAUD_SCORE_THRESHOLD = 0.6     # Ngưỡng chính của điểm gian lận khi hiển thị giao dịch đáng ngờ
SUSPICIOUS_THRESHOLD = 0.5      # Ngưỡng cho tài khoản đáng ngờ
HIGH_RISK_THRESHOLD = 0.75      # Ngưỡng cho tài khoản nguy cơ cao