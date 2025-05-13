from config import (FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD, HIGH_RISK_THRESHOLD,
                 VERY_HIGH_RISK_THRESHOLD, ROUND_AMOUNT_MIN, 
                 CHAIN_TIME_WINDOW, MIN_CHAIN_LENGTH, FUNNEL_MIN_SOURCES)
import time
from queries.pattern_queries import PatternQueries

class PatternDetector:
    def __init__(self, driver):
        self.driver = driver
        self.queries = PatternQueries()

    def analyze_fraud(self):
        with self.driver.session() as session:
            print("🔍 Detecting complex fraud patterns...")
            start_time = time.time()
            
            # First detect temporal patterns and bursts
            self.detect_temporal_patterns(session)
            
            # Then detect specialized transaction patterns
            self.detect_specialized_patterns(session)

            # Analyze transaction velocity and bursts
            self.analyze_transaction_velocity(session)

            # Calculate initial scores using improved ensemble
            self.calculate_fraud_scores()

            # Đảm bảo thu thập đầy đủ số liệu 
            print("Collecting pattern statistics...")
            pattern_stats = self.collect_pattern_stats(session)
            print(f"Pattern detection found: {pattern_stats}")

            # Apply final calibration with improved thresholds
            self.calculate_final_risk_score(session)
            
            print(f"✅ Pattern detection completed in {time.time() - start_time:.2f} seconds")
            return True

    def detect_temporal_patterns(self, session):
        """Phân tích các mẫu thời gian dựa trên step"""
        print("Analyzing temporal patterns in transactions...")
        
        temporal_queries = [
            self.queries.BURST_PATTERN_QUERY,
            self.queries.TRANSACTION_CYCLE_QUERY,
            self.queries.AMOUNT_CHAIN_QUERY,
            self.queries.HOUR_DISTRIBUTION_QUERY
        ]
        
        print("\nAnalyzing temporal patterns:")
        for i, query in enumerate(temporal_queries, 1):
            result = session.run(query).single()
            if result:
                count = list(result.values())[0]
                pattern_types = ["Burst patterns", "Transaction cycles", 
                               "Amount chains", "Hour distribution"]
                print(f"  Pattern {i} ({pattern_types[i-1]}): {count} accounts")

        # Update risk scores based on temporal patterns
        session.run(self.queries.UPDATE_TEMPORAL_RISK_QUERY)
        
        return True

    def calculate_fraud_scores(self):
        """Calculate optimized fraud scores with temporal patterns and improved weighting"""
        with self.driver.session() as session:
            print("Calculating optimized fraud scores with temporal analysis...")
            
            # Execute all models
            print("  Running Model 1 (Network Structure)...")
            result1 = session.run(self.queries.MODEL1_NETWORK_QUERY).single()
            processed1 = result1["processed_count"] if result1 else 0
            print(f"  ✓ Scored {processed1} accounts with Model 1")
            
            print("  Running Model 2 (Behavioral Patterns)...")
            result2 = session.run(self.queries.MODEL2_BEHAVIOR_QUERY).single()
            processed2 = result2["processed_count"] if result2 else 0
            print(f"  ✓ Scored {processed2} accounts with Model 2")
            
            print("  Running Model 3 (Complex Integration)...")
            result3 = session.run(self.queries.MODEL3_INTEGRATION_QUERY).single()
            processed3 = result3["processed_count"] if result3 else 0
            print(f"  ✓ Scored {processed3} accounts with Model 3")

            # Final ensemble combination
            print("  Calculating final ensemble scores...")
            self._calculate_ensemble_scores(session)

            print("  Detecting high-confidence fraud patterns...")
            high_confidence_result = session.run(self.queries.HIGH_CONFIDENCE_PATTERN_QUERY).single()
            high_confidence_count = high_confidence_result.get("high_confidence_accounts", 0) if high_confidence_result else 0
            print(f"  ✓ Found {high_confidence_count} high-confidence fraud patterns")

            print("  Detecting round amount patterns...")
            round_result = session.run(self.queries.ROUND_AMOUNT_QUERY).single()
            round_count = round_result.get("round_pattern_accounts", 0) if round_result else 0
            print(f"  ✓ Found {round_count} accounts with round amount patterns")
            
            print("  Detecting chain transaction patterns...")
            chain_result = session.run(self.queries.CHAIN_PATTERN_QUERY).single()
            chain_count = chain_result.get("chain_pattern_accounts", 0) if chain_result else 0
            print(f"  ✓ Found {chain_count} accounts with chain patterns")
            
            print("  Detecting accounts similar to known fraud...")
            similar_result = session.run(self.queries.SIMILAR_TO_FRAUD_QUERY).single()
            similar_count = similar_result.get("similar_accounts", 0) if similar_result else 0
            print(f"  ✓ Found {similar_count} accounts similar to known fraud")
                    
            return True

    def detect_specialized_patterns(self, session):
        """Phát hiện các mẫu giao dịch phức tạp"""
        print("Phát hiện các mẫu giao dịch chuyên biệt...")
        
        patterns_detected = 0
        
        # Phát hiện mẫu chuỗi
        try:
            chain_count = self.detect_chain_patterns(session)
            patterns_detected += chain_count
        except Exception as e:
            print(f"Error detecting chain patterns: {str(e)}")
        
        # Phát hiện mẫu phễu
        try:
            funnel_count = self.detect_funnel_patterns(session)
            patterns_detected += funnel_count
        except Exception as e:
            print(f"Error detecting funnel patterns: {str(e)}")
        
        # Phát hiện giao dịch số tròn
        try:
            round_count = self.detect_round_number_transactions(session)
            patterns_detected += round_count
        except Exception as e:
            print(f"Error detecting round number transactions: {str(e)}")
        
        # Phát hiện tài khoản tương tự với gian lận đã biết
        try:
            similar_count = session.run(self.queries.SIMILAR_TO_FRAUD_QUERY).single()
            if similar_count:
                patterns_detected += similar_count["similar_accounts"]
                print(f"  Tìm thấy {similar_count['similar_accounts']} tài khoản tương tự với gian lận đã biết")
        except Exception as e:
            print(f"Error detecting similar accounts: {str(e)}")
        
        # Cập nhật điểm rủi ro dựa trên các mẫu phát hiện được
        self._calculate_pattern_risk_scores(session)
        
        # Tính điểm cho Model 3 dựa trên các mẫu phức tạp phát hiện được
        try:
            model3_result = session.run(self.queries.MODEL3_INTEGRATION_QUERY).single()
            if model3_result:
                print(f"  Tính điểm Model 3 cho {model3_result['processed_count']} tài khoản")
        except Exception as e:
            print(f"Error calculating Model 3 scores: {str(e)}")
        
        print(f"  Tổng cộng phát hiện {patterns_detected} mẫu phức tạp")
        return True

    def _calculate_pattern_risk_scores(self, session):
        """Calculate risk scores based on detected patterns"""
        session.run(self.queries.PATTERN_RISK_SCORE_QUERY, {
            "minChainLength": MIN_CHAIN_LENGTH,
            "funnelMinSources": FUNNEL_MIN_SOURCES
        })

    def analyze_transaction_velocity(self, session):
        """Analyze the velocity of transactions to identify burst patterns"""
        print("Analyzing transaction velocity patterns...")
        
        # Check if we have timestamp data
        has_timestamps = session.run(self.queries.CHECK_TIMESTAMPS_QUERY).single()

        # Execute velocity analysis
        velocity_result = session.run(self.queries.VELOCITY_ANALYSIS_QUERY).single()
        velocity_accounts = velocity_result["high_velocity_accounts"] if velocity_result else 0
        print(f"  Found {velocity_accounts} accounts with high transaction velocity")
        
        return velocity_accounts

    def calculate_final_risk_score(self, session):
        """Tính toán điểm rủi ro cuối cùng với thuật toán cải tiến"""
        print("Tính điểm rủi ro cuối cùng với thuật toán tối ưu...")
        
        from config import MODEL1_WEIGHT, MODEL2_WEIGHT, MODEL3_WEIGHT
        
        result = session.run(self.queries.FINAL_RISK_SCORE_QUERY, {
            "model1Weight": MODEL1_WEIGHT,
            "model2Weight": MODEL2_WEIGHT,
            "model3Weight": MODEL3_WEIGHT,
            "fraudThreshold": FRAUD_SCORE_THRESHOLD,
            "highRisk": HIGH_RISK_THRESHOLD,
            "veryHighRisk": VERY_HIGH_RISK_THRESHOLD,
            "suspicious": SUSPICIOUS_THRESHOLD
        }).single()
        
        if result:
            print(f"\nRisk Score Summary:")
            print(f"  Total accounts analyzed: {result['updated_accounts']}")
            print(f"  Accounts above fraud threshold: {result['fraud_accounts']}")
            print(f"  Score range: {result['min_score']:.3f} - {result['max_score']:.3f}")
            print(f"  Average risk score: {result['avg_score']:.3f}")
        
        return True

    def _calculate_ensemble_scores(self, session):
        """Calculate ensemble scores using the ensemble model"""
        try:
            result = session.run(self.queries.ENSEMBLE_SCORE_QUERY, {
                "fraudThreshold": FRAUD_SCORE_THRESHOLD
            }).single()
            
            print("\nEnsemble Score Summary:")
            if result and result['scored_accounts'] > 0:
                print(f"  Total accounts processed: {result['scored_accounts']}")
                print(f"  Accounts above fraud threshold: {result['fraud_accounts']}")
                if result['avg_score'] is not None:
                    print(f"  Average score: {result['avg_score']:.3f}")
                    print(f"  Maximum score: {result['max_score']:.3f}")
                else:
                    print("  Average score: N/A")
            else:
                print("  No accounts were scored")
                
            return True
            
        except Exception as e:
            print(f"Error calculating ensemble scores: {str(e)}")
            return False
        
    # Add this method to the PatternDetector class
    def collect_pattern_stats(self, session):
        print("Collecting complete pattern statistics...")
        
        result = session.run(self.queries.PATTERN_STATS_QUERY).single()
        
        if result:
            # Create a structure for easier access to statistics
            stats = {
                'total': result.get("total_accounts", 0),
                'model1': {'count': result.get("model1_count", 0), 'txs': result.get("model1_txs", 0)},
                'model2': {'count': result.get("model2_count", 0), 'txs': result.get("model2_txs", 0)},
                'model3': {'count': result.get("model3_count", 0), 'txs': result.get("model3_txs", 0)},
                'high_confidence': {'count': result.get("high_confidence_count", 0), 'txs': result.get("high_confidence_txs", 0)},
                'funnel': {'count': result.get("funnel_count", 0), 'txs': result.get("funnel_txs", 0)},
                'round': {'count': result.get("round_count", 0), 'txs': result.get("round_txs", 0)},
                'chain': {'count': result.get("chain_count", 0), 'txs': result.get("chain_txs", 0)},
                'similar': {'count': result.get("similar_count", 0), 'txs': result.get("similar_txs", 0)},
                'velocity': {'count': result.get("velocity_count", 0), 'txs': result.get("velocity_txs", 0)}
            }
        else:
            stats = {}
        
        print("\nComplete pattern statistics:")
        print(f"  Model 1 (Network Structure): {stats.get('model1', {}).get('count', 0)} accounts, {stats.get('model1', {}).get('txs', 0)} transactions")
        print(f"  Model 2 (Behavioral Patterns): {stats.get('model2', {}).get('count', 0)} accounts, {stats.get('model2', {}).get('txs', 0)} transactions")
        print(f"  Model 3 (Complex Patterns): {stats.get('model3', {}).get('count', 0)} accounts, {stats.get('model3', {}).get('txs', 0)} transactions")
        print(f"  High Confidence Patterns: {stats.get('high_confidence', {}).get('count', 0)} accounts, {stats.get('high_confidence', {}).get('txs', 0)} transactions")
        print(f"  Funnel Patterns: {stats.get('funnel', {}).get('count', 0)} accounts, {stats.get('funnel', {}).get('txs', 0)} transactions")
        print(f"  Round Amount Patterns: {stats.get('round', {}).get('count', 0)} accounts, {stats.get('round', {}).get('txs', 0)} transactions")
        print(f"  Chain Patterns: {stats.get('chain', {}).get('count', 0)} accounts, {stats.get('chain', {}).get('txs', 0)} transactions")
        print(f"  Similar to Fraud: {stats.get('similar', {}).get('count', 0)} accounts, {stats.get('similar', {}).get('txs', 0)} transactions")
        print(f"  High Velocity: {stats.get('velocity', {}).get('count', 0)} accounts, {stats.get('velocity', {}).get('txs', 0)} transactions")
        
        return stats
    
    def detect_funnel_patterns(self, session):
        """Phát hiện mẫu phễu - nhận từ nhiều nguồn, chuyển đến ít đích"""
        print("Phát hiện mẫu phễu giao dịch...")
        
        result = session.run(self.queries.FUNNEL_PATTERN_QUERY, {
            "funnelMinSources": FUNNEL_MIN_SOURCES
        }).single()
        
        funnel_count = result["funnel_accounts"] if result else 0
        print(f"  Tìm thấy {funnel_count} tài khoản có mẫu phễu")
        return funnel_count

    def detect_round_number_transactions(self, session):
        """Phát hiện mẫu giao dịch số tròn - dấu hiệu của gian lận có tổ chức"""
        print("Phát hiện giao dịch số tròn...")
        
        result = session.run(self.queries.ROUND_AMOUNT_QUERY, {
            "roundAmountMin": ROUND_AMOUNT_MIN
        }).single()
        
        round_count = result["round_pattern_accounts"] if result else 0
        print(f"  Tìm thấy {round_count} tài khoản có mẫu giao dịch số tròn")
        return round_count
    
    def detect_chain_patterns(self, session):
        """Phát hiện chuỗi giao dịch liên tiếp - dấu hiệu rửa tiền"""
        print("Phát hiện chuỗi giao dịch...")
        
        result = session.run(self.queries.CHAIN_PATTERN_QUERY, {
            "chainTimeWindow": CHAIN_TIME_WINDOW,
            "minChainLength": MIN_CHAIN_LENGTH
        }).single()
        
        chain_count = result["chain_pattern_accounts"] if result else 0
        print(f"  Tìm thấy {chain_count} tài khoản tham gia chuỗi giao dịch")
        return chain_count