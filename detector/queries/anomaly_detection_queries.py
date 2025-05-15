"""
Chứa tất cả các truy vấn CQL cho module phát hiện bất thường
"""

# Truy vấn liên quan đến anomaly detection
COMPUTE_ANOMALY_SCORE = """
MATCH (a:Account)
WITH a, 
    a.degScore AS degScore, 
    a.prScore AS prScore,
    a.simScore AS simScore,
    a.btwScore AS btwScore,
    a.hubScore AS hubScore,
    a.authScore AS authScore,
    a.coreScore AS coreScore,
    a.triCount AS triCount,
    a.cycleCount AS cycleCount,
    a.tempBurst AS tempBurst,
    a.txVelocity AS txVelocity,
    a.amountVolatility AS amountVolatility,
    a.maxAmountRatio AS maxAmountRatio,
    a.stdTimeBetweenTx AS stdTimeBetweenTx,
    a.normCommunitySize AS normCommunitySize

// Tính anomaly score = weighted sum của các đặc trưng
WITH a, 
    (degScore * 0.35) + 
    (prScore * 0.05) + 
    (simScore * 0.03) + 
    (btwScore * 0.05) + 
    (hubScore * 0.08) + 
    (authScore * 0.03) + 
    (coreScore * 0.03) + 
    (triCount * 0.03) + 
    (cycleCount * 0.03) + 
    (tempBurst * 0.08) + 
    (txVelocity * 0.03) +
    (amountVolatility * 0.05) +
    (maxAmountRatio * 0.09) +
    (stdTimeBetweenTx * 0.02) +
    (0.08 * (1 - coalesce(normCommunitySize, 0))) AS score
SET a.anomaly_score = score
"""

TRANSFER_SCORE_TO_RELATIONSHIP = """
MATCH (a:Account)-[r:SENT]->()
SET r.anomaly_score = a.anomaly_score
"""

def get_flag_anomalies_query(percentile_cutoff):
    """Trả về truy vấn đánh dấu giao dịch bất thường với ngưỡng phân vị được chỉ định."""
    return f"""
    MATCH ()-[tx:SENT]->()
    WITH percentileCont(tx.anomaly_score, {percentile_cutoff}) AS threshold
    MATCH ()-[tx2:SENT]->()
    WHERE tx2.anomaly_score >= threshold
    SET tx2.flagged = true
    RETURN threshold, COUNT(tx2) AS flagged_count
    """

DEFAULT_FLAG = """
MATCH ()-[tx:SENT]->()
WHERE tx.flagged IS NULL
SET tx.flagged = false
"""

EXPORT_ANOMALY_SCORES = """
MATCH ()-[r:SENT]->()
WHERE r.anomaly_score IS NOT NULL
RETURN id(r) AS transaction_id, r.anomaly_score AS anomaly_score, r.isFraud AS isFraud
"""