# Neo4j connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

# Import/Export parameters
BATCH_SIZE = 2000
MAX_NODES = 400000
MAX_RELATIONSHIPS = 600000
ALLOWED_EXTENSIONS = {'csv'}
UPLOAD_FOLDER = 'uploads'

# Feature weights
FEATURE_WEIGHTS = {
    'degScore': 0.60,
    'prScore': 0.02,
    'simScore': 0.01,
    'btwScore': 0.02,
    'hubScore': 0.08,
    'authScore': 0.01,
    'coreScore': 0.01,
    'triCount': 0.01,
    'cycleCount': 0.01,
    'tempBurst': 0.05,
    'txVelocity': 0.01,
    'amountVolatility': 0.02,
    'maxAmountRatio': 0.12,
    'stdTimeBetweenTx': 0.01,
    'normCommunitySize': 0.02,
}

# Detection parameters
DEFAULT_PERCENTILE = 0.995  # Tăng từ 0.99 lên 0.995 để giảm false positives