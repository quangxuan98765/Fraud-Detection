from flask import render_template, request, flash, redirect, url_for
from werkzeug.utils import secure_filename
import os
import time
import traceback
from . import views_bp
from detector.fraud_detector import FraudDetector
from detector.database_manager import DatabaseManager
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from detector.utils.config import DEFAULT_PERCENTILE

# Khởi tạo database manager với thông tin kết nối từ config
db_manager = DatabaseManager(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

# Khởi tạo detector với db_manager
detector = FraudDetector(db_manager)

# Cấu hình upload
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}

# Đảm bảo thư mục uploads tồn tại
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@views_bp.route('/')
def index():
    """Trang chủ"""
    try:
        # Kiểm tra trạng thái database
        has_data, stats = db_manager.check_data()
        
        # Kiểm tra xem đã có phân tích chưa
        has_analysis = False
        with db_manager.driver.session() as session:
            result = session.run("""
                MATCH (a:Account) 
                WHERE a.fraud_score IS NOT NULL 
                RETURN count(a) > 0 as has_analysis
            """).single()
            if result:
                has_analysis = result.get("has_analysis", False)
        
        return render_template('index.html', has_data=has_data, stats=stats, has_analysis=has_analysis)
    except Exception as e:
        print(f"Lỗi trang chủ: {str(e)}")
        traceback.print_exc()
        flash(f"Lỗi kết nối database: {str(e)}", "error")
        return render_template('index.html', has_data=False, stats={}, error=str(e), has_analysis=False)

@views_bp.route('/upload', methods=['GET', 'POST'])
def upload():
    """Trang tải lên dữ liệu"""
    if request.method == 'GET':
        return render_template('upload.html')
    
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Không tìm thấy file', 'error')
            return redirect(url_for('views.upload'))
        
        file = request.files['file']
        
        if file.filename == '':
            flash('Chưa chọn file', 'error')
            return redirect(url_for('views.upload'))
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(filepath)
            
            try:
                start_time = time.time()
                success = detector.import_data(filepath)
                if success:
                    import_time = time.time() - start_time
                    flash(f'Import thành công trong {import_time:.2f} giây', 'success')
                else:
                    flash('Import không thành công', 'error')
            except Exception as e:
                print(f"Lỗi import: {str(e)}")
                traceback.print_exc()
                flash(f'Lỗi: {str(e)}', 'error')
                
            return redirect(url_for('views.index'))
        
        flash('File không hợp lệ, chỉ chấp nhận file CSV', 'error')
        return redirect(url_for('views.upload'))

@views_bp.route('/results')
def results():
    """Trang kết quả"""
    try:
        # Không cần truy vấn gì từ server - frontend sẽ lấy dữ liệu từ API
        return render_template('results.html')
    except Exception as e:
        print(f"Lỗi tổng quan trang kết quả: {str(e)}")
        traceback.print_exc()
        flash(f'Lỗi: {str(e)}', 'error')
        return render_template('results.html')

@views_bp.route('/run-analysis', methods=['POST'])
def run_analysis():
    """Thực hiện phân tích"""
    try:
        start_time = time.time()
        result = detector.run_pipeline(percentile_cutoff=DEFAULT_PERCENTILE)
        end_time = time.time()
        
        if result:
            flash(f'Phân tích hoàn tất trong {end_time - start_time:.2f} giây', 'success')
        else:
            flash('Có lỗi khi phân tích dữ liệu', 'warning')
        
        return redirect(url_for('views.results'))
    except Exception as e:
        print(f"Lỗi khi phân tích: {str(e)}")
        traceback.print_exc()
        flash(f'Lỗi khi phân tích: {str(e)}', 'error')
        return redirect(url_for('views.index'))

@views_bp.route('/clear-database', methods=['POST'])
def clear_database():
    """Xóa dữ liệu database"""
    try:
        success = detector.clear_database()
        if success:
            flash('Đã xóa thành công tất cả dữ liệu', 'success')
        else:
            flash('Có lỗi khi xóa dữ liệu', 'error')
    except Exception as e:
        print(f"Lỗi xóa database: {str(e)}")
        traceback.print_exc()
        flash(f'Lỗi: {str(e)}', 'error')
    
    return redirect(url_for('views.index'))