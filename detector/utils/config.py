# Neo4j connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

# Import/Export parameters
BATCH_SIZE = 5000
MAX_NODES = 100000
MAX_RELATIONSHIPS = 500000
ALLOWED_EXTENSIONS = {'csv'}
UPLOAD_FOLDER = 'uploads'

# Feature weights
FEATURE_WEIGHTS = {
    'degScore': 0.15,
    'prScore': 0.15,
    'simScore': 0.1,
    'btwScore': 0.1,
    'hubScore': 0.05,
    'authScore': 0.05,
    'coreScore': 0.05,
    'triCount': 0.05,
    'cycleCount': 0.05,
    'tempBurst': 0.08,
    'txVelocity': 0.05,
    'amountVolatility': 0.07,
    'maxAmountRatio': 0.05,
    'stdTimeBetweenTx': 0.05
}

# Detection parameters
DEFAULT_PERCENTILE = 0.97