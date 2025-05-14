"""
Chứa các truy vấn dùng trong đánh giá hiệu suất và phân tích
"""

# Truy vấn đánh giá hiệu suất
PERFORMANCE_EVALUATION_QUERY = """
MATCH ()-[tx:SENT]->()
WITH
    SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS false_negatives,
    SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS true_negatives,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS total_fraud

// Calculate precision and recall first
WITH
    true_positives, false_positives, false_negatives, true_negatives, 
    total_transactions, total_fraud,
    CASE WHEN (true_positives + false_positives) > 0 
        THEN toFloat(true_positives) / (true_positives + false_positives) 
        ELSE 0 
    END AS precision,
    CASE WHEN (true_positives + false_negatives) > 0 
        THEN toFloat(true_positives) / (true_positives + false_negatives) 
        ELSE 0 
    END AS recall

// Then use precision and recall to calculate F1 score
RETURN 
    true_positives,
    false_positives,
    false_negatives,
    true_negatives,
    total_transactions,
    total_fraud,
    precision,
    recall,
    CASE 
        WHEN (precision + recall) > 0 
        THEN 2 * precision * recall / (precision + recall) 
        ELSE 0 
    END AS f1_score
"""

# Truy vấn để lấy phân phối điểm bất thường
SCORE_DISTRIBUTION_QUERY = """
MATCH ()-[tx:SENT]->()
WHERE tx.anomaly_score IS NOT NULL
RETURN tx.anomaly_score AS score, tx.flagged AS flagged, tx.ground_truth_fraud AS is_fraud
ORDER BY score DESC
"""

# Function để tạo truy vấn phân tích tầm quan trọng của đặc trưng
def get_feature_importance_query(feature):
    return f"""
    MATCH (a:Account)-[tx:SENT]->()
    WHERE tx.ground_truth_fraud IS NOT NULL AND a.{feature} IS NOT NULL
    RETURN tx.ground_truth_fraud AS fraud, a.{feature} AS feature_value
    """