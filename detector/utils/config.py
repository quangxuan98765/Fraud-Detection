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
    'degScore': 0.35,         # Increased significantly (from 0.15) as it has the highest correlation
    'prScore': 0.05,          # Decreased (from 0.15) due to low/negative correlation
    'simScore': 0.03,         # Decreased (from 0.1) due to very low correlation
    'btwScore': 0.05,         # Decreased (from 0.1) due to low/negative correlation
    'hubScore': 0.08,         # Increased slightly (from 0.05) due to positive correlation
    'authScore': 0.03,        # Decreased (from 0.05) due to low correlation
    'coreScore': 0.03,        # Decreased (from 0.05) due to low correlation
    'triCount': 0.03,         # Decreased (from 0.05) due to low correlation
    'cycleCount': 0.03,       # Decreased (from 0.05) due to low correlation
    'tempBurst': 0.08,        # Kept the same as it has positive correlation
    'txVelocity': 0.03,       # Decreased (from 0.05) due to low correlation
    'amountVolatility': 0.05, # Decreased slightly (from 0.07) due to low correlation
    'maxAmountRatio': 0.09,   # Increased (from 0.05) as it has the second highest correlation
    'stdTimeBetweenTx': 0.02, # Decreased (from 0.05) due to very low correlation
    'normCommunitySize': 0.08 # Decreased (from 0.15) but still significant due to positive correlation
}

# Detection parameters
DEFAULT_PERCENTILE = 0.99  # Increased from 0.97 to reduce false positives