from config import FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD
from queries.account_queries import AccountQueries

class AccountAnalyzer:
    def __init__(self, driver):
        self.driver = driver
        self.queries = AccountQueries()

    def process_account_behaviors(self):
        """Mark abnormal behaviors using improved criteria for higher precision and recall"""
        with self.driver.session() as session:
            print("🔍 Applying enhanced account behavior analysis...")
            
            # Get total account count
            total = session.run(self.queries.GET_ACCOUNT_COUNT).single()["count"]
            print(f"  Total accounts to analyze: {total}")
            
            # Define batch size
            batch_size = 1000
            total_batches = (total + batch_size - 1) // batch_size
            
            # Process in batches with improved criteria
            for i in range(total_batches):
                start = i * batch_size
                
                # Execute query
                result = session.run(self.queries.ACCOUNT_BEHAVIOR_QUERY, skip=start, limit=batch_size).single()
                processed = result["processed"] if result else 0

            # Additional analysis: Identify accounts with rapid turnover (sign of money laundering)
            session.run(self.queries.RAPID_TURNOVER_QUERY)
            
            # Identify structuring patterns (multiple small transactions)
            session.run(self.queries.STRUCTURING_PATTERNS_QUERY)

    def process_transaction_anomalies(self):
        """Analyze transactions for anomalies with improved detection criteria"""
        with self.driver.session() as session:
            print("🔍 Analyzing transactions for anomalous patterns...")
            
            # Get total accounts
            total = session.run(self.queries.GET_ACCOUNT_COUNT).single()["count"]
            batch_size = 1000
            total_batches = (total + batch_size - 1) // batch_size
            
            # Clear old flags
            session.run(self.queries.CLEAR_TX_ANOMALY_FLAGS)

            # Analyze in batches with improved criteria
            for i in range(total_batches):
                start = i * batch_size
                
                result = session.run(self.queries.TRANSACTION_ANOMALIES_QUERY, skip=start, limit=batch_size).single()
                processed = result["processed"] if result else 0
            
            # Identify accounts with high-value transactions with improved criteria
            session.run(self.queries.HIGH_VALUE_TRANSACTIONS_QUERY)
            
            # Additional check: Identify accounts involved in round-number transactions
            session.run(self.queries.ROUND_NUMBER_TRANSACTIONS_QUERY)
            
            # Check for accounts sending to known high-risk accounts
            session.run(self.queries.HIGH_RISK_TARGETS_QUERY)
    
    def analyze_account_neighborhood(self):
        """Analyze the network neighborhood of accounts to find suspicious patterns"""
        with self.driver.session() as session:
            print("🔍 Analyzing account relationships and neighborhoods...")
            
            # Find accounts that frequently transfer to/from the same set of accounts
            session.run(self.queries.CONCENTRATED_TRANSFERS_QUERY)
            
            # Find accounts that receive from many sources but send to few destinations
            session.run(self.queries.FUNNEL_PATTERN_QUERY)
            
            # Find accounts with fan-out pattern (potentially distributing funds)
            session.run(self.queries.FAN_OUT_PATTERN_QUERY)
    
    def analyze_temporal_patterns(self):
        """Analyze transaction timing patterns if timestamp data is available"""
        with self.driver.session() as session:
            # Check if timestamp data exists
            has_timestamps = session.run(self.queries.CHECK_TIMESTAMPS_QUERY).single()
            
            if has_timestamps and has_timestamps.get("has_timestamps", False):
                print("🔍 Analyzing temporal transaction patterns...")
                
                # Identify rapid burst transactions (multiple transactions in short period)
                session.run(self.queries.BURST_ACTIVITY_QUERY)
                
                # Identify accounts with unusual activity times
                session.run(self.queries.UNUSUAL_HOURS_QUERY)
    
    def detect_complex_patterns(self):
        """Detect complex fraud patterns combining multiple indicators"""
        with self.driver.session() as session:
            print("🔍 Detecting complex fraud patterns...")
            
            # Pattern 1: Layering pattern (sequential transfers through multiple accounts)
            session.run(self.queries.LAYERING_PATTERN_QUERY)
            
            # Pattern 2: Combined risk indicators
            session.run(self.queries.COMBINED_RISK_INDICATORS_QUERY)
            
            # Pattern 3: Identify suspicious communities
            session.run(self.queries.SUSPICIOUS_COMMUNITIES_QUERY)