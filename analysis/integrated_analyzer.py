from analysis.account_analyzer import AccountAnalyzer
from analysis.pattern_detector import PatternDetector
from queries.metrics_queries import MetricsQueries

class IntegratedAnalyzer:
    """Combines all enhanced detection algorithms into a unified analysis pipeline"""
    
    def __init__(self, driver):
        self.driver = driver
        self.enhanced_account_analyzer = AccountAnalyzer(driver)
        self.advanced_pattern_detector = PatternDetector(driver)
        self.metrics_queries = MetricsQueries()
        
    def perform_comprehensive_analysis(self):
        """Run the complete enhanced analysis pipeline"""
        with self.driver.session() as session:
            print("🔍 Starting comprehensive fraud analysis with enhanced algorithms...")
            
            # Step 1: Enhanced account behavior analysis
            print("\n[1/5] Analyzing account behaviors with enhanced criteria...")
            self.enhanced_account_analyzer.process_account_behaviors()
            
            # Step 2: Enhanced transaction anomaly detection
            print("\n[2/5] Detecting transaction anomalies with improved thresholds...")
            self.enhanced_account_analyzer.process_transaction_anomalies()
            
            # Step 3: Network neighborhood analysis
            print("\n[3/5] Analyzing account relationships and network patterns...")
            self.enhanced_account_analyzer.analyze_account_neighborhood()
            
            # Step 4: Temporal pattern analysis (if available)
            print("\n[4/5] Analyzing temporal transaction patterns...")
            self.enhanced_account_analyzer.analyze_temporal_patterns()
            
            # Step 5: Complex pattern detection
            print("\n[5/5] Detecting complex fraud patterns...")
            self.enhanced_account_analyzer.detect_complex_patterns()
            
            # Final step: Advanced fraud scoring
            print("\nCalculating comprehensive fraud scores...")
            self.advanced_pattern_detector.calculate_fraud_scores()
            
            # Detect money mule accounts
            print("Detecting potential money mule accounts...")
            mule_count = self.advanced_pattern_detector.detect_mule_accounts(session)
            
            # Detect temporal patterns
            print("Analyzing transaction timing patterns...")
            self.advanced_pattern_detector.detect_temporal_patterns(session)
            
            # Calculate final metrics
            self._calculate_metrics(session)
            
            return True
            
    def _calculate_metrics(self, session):
        """Calculate and display the effectiveness metrics of the fraud detection"""
        metrics = session.run(self.metrics_queries.CALCULATE_METRICS).single()
        
        if metrics:
            total = metrics.get("total_accounts", 0)
            detected = metrics.get("detected_high_risk", 0)
            actual = metrics.get("actual_fraud", 0)
            true_pos = metrics.get("true_positives", 0)
            false_pos = metrics.get("false_positives", 0)
            false_neg = metrics.get("false_negatives", 0)
            precision = metrics.get("precision", 0)
            recall = metrics.get("recall", 0)
            f1 = metrics.get("f1_score", 0)
            
            print("\n===== Fraud Detection Performance Metrics =====")
            print(f"Total Accounts:            {total:,}")
            print(f"Detected High Risk:        {detected:,} ({detected/total:.2%} of total)")
            print(f"Actual Fraud Accounts:     {actual:,} ({actual/total:.2%} of total)")
            print(f"True Positives:            {true_pos:,}")
            print(f"False Positives:           {false_pos:,}")
            print(f"False Negatives:           {false_neg:,}")
            print(f"Precision:                 {precision:.2%}")
            print(f"Recall:                    {recall:.2%}")
            print(f"F1-Score:                  {f1:.2%}")
            
            # Calculate metrics for different thresholds
            print("\n===== Performance at Different Thresholds =====")
            thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
            
            for threshold in thresholds:
                threshold_metrics = session.run(self.metrics_queries.THRESHOLD_METRICS, threshold=threshold).single()
                
                if threshold_metrics:
                    t_detected = threshold_metrics.get("detected_high_risk", 0)
                    t_precision = threshold_metrics.get("precision", 0)
                    t_recall = threshold_metrics.get("recall", 0)
                    t_f1 = threshold_metrics.get("f1_score", 0)
                    
                    print(f"Threshold {threshold:.1f}: Flagged: {t_detected:,}, Precision: {t_precision:.2%}, " +
                          f"Recall: {t_recall:.2%}, F1: {t_f1:.2%}")
            
            # Community-based metrics
            community_metrics = session.run(self.metrics_queries.COMMUNITY_METRICS).single()
            
            if community_metrics:
                community_count = community_metrics.get("community_count", 0)
                high_fraud = community_metrics.get("high_fraud_communities", 0)
                high_risk = community_metrics.get("high_risk_communities", 0)
                avg_size = community_metrics.get("avg_community_size", 0)
                fraud_ratio = community_metrics.get("avg_fraud_ratio", 0)
                flagged_ratio = community_metrics.get("avg_flagged_ratio", 0)
                
                print("\n===== Community Analysis =====")
                print(f"Total Communities:           {community_count:,}")
                print(f"High Fraud Communities:      {high_fraud:,} ({high_fraud/community_count:.2%} of communities)")
                print(f"High Risk Communities:       {high_risk:,} ({high_risk/community_count:.2%} of communities)")
                print(f"Average Community Size:      {avg_size:.2f}")
                print(f"Average Fraud Ratio:         {fraud_ratio:.2%}")
                print(f"Average Flagged Ratio:       {flagged_ratio:.2%}")
            
            return metrics
        else:
            print("No metrics data available.")
            return None