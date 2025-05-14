import logging
import time
from functools import wraps

def setup_logger(name='fraud_detector', level=logging.INFO):
    """Thiết lập và trả về logger với cấu hình mặc định."""
    logger = logging.getLogger(name)
    
    # Nếu logger đã có handler, không cần thiết lập lại
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    # Tạo formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Tạo console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Tạo file handler
    file_handler = logging.FileHandler(f'{name}.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def log_execution_time(logger=None):
    """Decorator để ghi lại thời gian thực thi của một hàm."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            if logger:
                logger.info(f"Bắt đầu thực thi {func.__name__}")
            
            result = func(*args, **kwargs)
            
            execution_time = time.time() - start_time
            
            if logger:
                logger.info(f"Hoàn thành {func.__name__} trong {execution_time:.2f} giây")
            else:
                print(f"Hoàn thành {func.__name__} trong {execution_time:.2f} giây")
                
            return result
        return wrapper
    return decorator