"""
Chứa các truy vấn liên quan đến việc trích xuất đặc trưng
"""

# Truy vấn trích xuất đặc trưng thời gian
TRANSACTION_VELOCITY_QUERY = """
MATCH (from:Account)-[tx:SENT]->()
WITH from, tx.step as step
ORDER BY from, step
WITH from, collect(step) AS steps
WITH from, steps, 
    size(steps) AS transaction_count,
    CASE WHEN size(steps) <= 1 THEN 0 ELSE toFloat(last(steps) - head(steps)) END AS time_span
WITH from, transaction_count, 
    CASE WHEN time_span = 0 THEN 0 ELSE transaction_count / (time_span + 1) END AS velocity
SET from.txVelocity = velocity
"""

SIMPLE_VOLATILITY_QUERY = """
MATCH (from:Account)-[tx:SENT]->()
WITH from, tx
ORDER BY from, tx.step
WITH from, collect(tx.amount) as amount_list
WITH from, amount_list,
    CASE WHEN size(amount_list) <= 1 THEN 0 
        ELSE (
            // Tính range từng phần tử một
            REDUCE(max_val = 0, x IN amount_list | 
              CASE WHEN x > max_val THEN x ELSE max_val END
            ) - 
            REDUCE(min_val = toFloat(9999999999), x IN amount_list | 
              CASE WHEN x < min_val AND x IS NOT NULL THEN x ELSE min_val END
            )
        ) 
    END AS amount_range,
    CASE WHEN size(amount_list) = 0 THEN 0 
        ELSE REDUCE(sum = 0, x IN amount_list | sum + x) / size(amount_list) 
    END AS avg_amount
SET from.amountVolatility = CASE WHEN avg_amount = 0 THEN 0 ELSE amount_range / avg_amount END,
    from.maxAmountRatio = CASE WHEN avg_amount = 0 THEN 0 
                              ELSE REDUCE(max_val = 0, x IN amount_list | 
                                     CASE WHEN x > max_val THEN x ELSE max_val END
                                   ) / avg_amount
                         END
"""

BURST_DETECTION_QUERY = """
MATCH (from:Account)-[tx:SENT]->()
WITH from, tx.step as step
ORDER BY from, step
WITH from, collect(step) AS steps
UNWIND range(0, size(steps)-2) AS i
WITH from, steps[i+1] - steps[i] AS time_diff
WITH from, collect(time_diff) AS time_diffs
WITH from, time_diffs,
    CASE WHEN size(time_diffs) = 0 THEN 0 
        ELSE size([t IN time_diffs WHERE t <= 3]) / toFloat(size(time_diffs)) 
    END AS burst_ratio
SET from.tempBurst = burst_ratio
"""

TIME_PATTERNS_QUERY = """
MATCH (from:Account)-[tx:SENT]->()
WITH from, tx.step as step
ORDER BY from, step
WITH from, collect(step) AS steps
UNWIND range(0, size(steps)-2) AS i
WITH from, steps[i+1] - steps[i] AS time_diff
WITH from, avg(time_diff) AS avg_time_between_tx,
    stDev(time_diff) AS std_time_between_tx
SET from.avgTimeBetweenTx = avg_time_between_tx,
    from.stdTimeBetweenTx = CASE WHEN avg_time_between_tx = 0 THEN 0 
                                 ELSE std_time_between_tx / avg_time_between_tx 
                            END
"""

# Truy vấn normalize đặc trưng
def get_normalize_query(feature):
    """Tạo truy vấn normalize một đặc trưng cụ thể."""
    return f"""
    MATCH (n) 
    WHERE n.{feature} IS NOT NULL
    WITH MIN(n.{feature}) AS min_val, MAX(n.{feature}) AS max_val
    WHERE max_val <> min_val
    MATCH (m)
    WHERE m.{feature} IS NOT NULL
    SET m.{feature}_norm = (m.{feature} - min_val) / (max_val - min_val)
    """

def get_rename_query(feature):
    """Tạo truy vấn đổi tên đặc trưng sau khi normalize."""
    return f"""
    MATCH (n)
    WHERE n.{feature}_norm IS NOT NULL
    SET n.{feature} = n.{feature}_norm
    REMOVE n.{feature}_norm
    """

def get_default_query(feature):
    """Tạo truy vấn thiết lập giá trị mặc định cho các node thiếu đặc trưng."""
    return f"""
    MATCH (n)
    WHERE n.{feature} IS NULL
    SET n.{feature} = 0
    """