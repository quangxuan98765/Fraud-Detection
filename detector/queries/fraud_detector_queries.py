"""
Chứa các truy vấn Cypher cho FraudDetector
"""

# Truy vấn cho prepare_ground_truth
CHECK_FRAUD_FIELD_QUERY = """
MATCH ()-[r:SENT]->()
RETURN 
    COUNT(r) AS total,
    SUM(CASE WHEN r.is_fraud IS NOT NULL THEN 1 ELSE 0 END) AS has_is_fraud
"""

MAP_FRAUD_TO_GROUND_TRUTH_QUERY = """
MATCH ()-[r:SENT]->()
WHERE r.is_fraud IS NOT NULL AND r.ground_truth_fraud IS NULL
SET r.ground_truth_fraud = CASE 
    WHEN r.is_fraud = 1 OR r.is_fraud = true OR r.is_fraud = '1' THEN true 
    WHEN r.is_fraud = 0 OR r.is_fraud = false OR r.is_fraud = '0' THEN false
    ELSE null 
END
RETURN COUNT(*) AS mapped
"""

# Check the actual values of is_fraud before mapping
CHECK_IS_FRAUD_VALUES = """
MATCH ()-[r:SENT]->()
WHERE r.is_fraud IS NOT NULL
RETURN DISTINCT r.is_fraud, COUNT(*) AS count
"""

CHECK_GROUND_TRUTH_RESULT_QUERY = """
MATCH ()-[r:SENT]->()
RETURN 
    COUNT(r) AS total,
    SUM(CASE WHEN r.ground_truth_fraud IS NOT NULL THEN 1 ELSE 0 END) AS has_ground_truth,
    SUM(CASE WHEN r.ground_truth_fraud = true THEN 1 ELSE 0 END) AS fraud_cases
"""

# Truy vấn cho examine_data
GROUND_TRUTH_DISTRIBUTION_QUERY = """
MATCH ()-[tx:SENT]->()
WITH 
    COUNT(tx) AS total,
    SUM(CASE WHEN tx.ground_truth_fraud IS NOT NULL THEN 1 ELSE 0 END) AS has_ground_truth,
    SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS fraud_cases
RETURN 
    total, 
    has_ground_truth, 
    fraud_cases,
    toFloat(has_ground_truth) / total AS coverage_ratio,
    CASE WHEN has_ground_truth > 0 
        THEN toFloat(fraud_cases) / has_ground_truth 
        ELSE 0 
    END AS fraud_ratio
"""

TYPE_CHECK_QUERY = """
MATCH ()-[tx:SENT]->()
WHERE tx.ground_truth_fraud IS NOT NULL
RETURN 
    CASE 
        WHEN toString(tx.ground_truth_fraud) IN ['true', 'false'] THEN 'String'
        WHEN toString(tx.ground_truth_fraud) IN ['0', '1'] THEN 'String'
        WHEN tx.ground_truth_fraud IN [true, false] THEN 'Boolean'
        ELSE 'Unknown'
    END AS data_type,
    COUNT(*) as count
"""

SCORE_DISTRIBUTION_QUERY = """
MATCH ()-[tx:SENT]->()
WHERE tx.anomaly_score IS NOT NULL
WITH 
    MIN(tx.anomaly_score) AS min_score,
    MAX(tx.anomaly_score) AS max_score,
    AVG(tx.anomaly_score) AS avg_score,
    STDEV(tx.anomaly_score) AS std_score,
    percentileCont(tx.anomaly_score, 0.5) AS median_score,
    percentileCont(tx.anomaly_score, 0.95) AS p95_score,
    percentileCont(tx.anomaly_score, 0.99) AS p99_score,
    COUNT(*) AS count
RETURN min_score, max_score, avg_score, std_score, median_score, p95_score, p99_score, count
"""

# Truy vấn cho cleanup_properties_and_relationships
def get_node_cleanup_query(properties):
    """Tạo truy vấn xóa các thuộc tính được chỉ định khỏi tất cả các node."""
    properties_to_remove = ", ".join([f"n.{prop}" for prop in properties])
    return f"""
    MATCH (n)
    REMOVE {properties_to_remove}
    """

RELATIONSHIP_CLEANUP_QUERY = """
MATCH ()-[r:SENT]->()
REMOVE r.degScore, r.prScore, r.simScore, r.btwScore, r.hubScore, r.authScore, 
       r.coreScore, r.triCount, r.cycleCount, r.tempBurst, r.txVelocity, 
       r.amountVolatility, r.maxAmountRatio, r.stdTimeBetweenTx, r.normCommunitySize
// Keep anomaly_score, flagged, combined_score and fraud_pattern_score for analysis
"""

DELETE_SIMILAR_RELATIONSHIPS_QUERY = "MATCH ()-[r:SIMILAR]-() DELETE r"