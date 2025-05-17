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
    'degScore': 0.38,
    'hubScore': 0.18,
    'normCommunitySize': 0.15,
    'amountVolatility': 0.07,
    'txVelocity': 0.07,
    'btwScore': 0.05,
    'prScore': 0.05,
    'authScore': 0.05,
    'maxAmountRatio': 0.00,
    'tempBurst': 0.00,
    'simScore': 0.00,
    'coreScore': 0.00,
    'triCount': 0.00,
    'cycleCount': 0.00,
    'stdTimeBetweenTx': 0.00,
}

# Detection parameters
DEFAULT_PERCENTILE = 0.99  # Tăng từ 0.99 lên 0.995 để giảm false positives