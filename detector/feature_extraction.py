from .utils.config import FEATURE_WEIGHTS
from .database_manager import DatabaseManager
from .queries.feature_extraction_queries import (
    TRANSACTION_VELOCITY_QUERY,
    SIMPLE_VOLATILITY_QUERY,
    BURST_DETECTION_QUERY,
    TIME_PATTERNS_QUERY,
    get_normalize_query,
    get_rename_query,
    get_default_query
)

class FeatureExtractor:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.weights = FEATURE_WEIGHTS
    
    def extract_temporal_features(self):
        """Trích xuất các đặc trưng thời gian (temporal features) để phát hiện mẫu bất thường."""
        print("🔄 Đang trích xuất đặc trưng thời gian...")
        
        # 1. Tính tốc độ giao dịch (giao dịch/giờ) trong cửa sổ thời gian
        self.db_manager.run_query(TRANSACTION_VELOCITY_QUERY)
        
        # 2. Phát hiện sự thay đổi đột ngột trong số tiền giao dịch (sửa lại để hoạt động đúng)
        self.db_manager.run_query(SIMPLE_VOLATILITY_QUERY)
        
        # 3. Phát hiện burst (nhiều giao dịch trong thời gian ngắn) - (Đã hoạt động)
        self.db_manager.run_query(BURST_DETECTION_QUERY)
        
        # 4. Thời gian trung bình và độ lệch chuẩn - (Đã hoạt động)
        self.db_manager.run_query(TIME_PATTERNS_QUERY)
        
        # Cập nhật trọng số
        self.weights['txVelocity'] = 0.05
        self.weights['amountVolatility'] = 0.07
        self.weights['tempBurst'] = 0.08
        self.weights['maxAmountRatio'] = 0.05
        self.weights['stdTimeBetweenTx'] = 0.05
        
        print("✅ Đã trích xuất các đặc trưng thời gian.")
        
    def normalize_features(self):
        """Min-max normalize tất cả các đặc trưng về khoảng [0, 1]."""
        print("🔄 Đang normalize các đặc trưng...")
        
        features_to_normalize = [
            'degScore', 'prScore', 'simScore', 'btwScore', 'hubScore', 
            'authScore', 'coreScore', 'triCount', 'cycleCount', 'tempBurst',
            'txVelocity', 'amountVolatility', 'maxAmountRatio', 'stdTimeBetweenTx'
        ]
        
        for feature in features_to_normalize:
            # Sử dụng các hàm tạo query thay vì hardcode truy vấn
            self.db_manager.run_query(get_normalize_query(feature))
            self.db_manager.run_query(get_rename_query(feature))
            self.db_manager.run_query(get_default_query(feature))
            
        print("✅ Đã normalize xong tất cả các đặc trưng.")