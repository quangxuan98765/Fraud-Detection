from flask import render_template, request, flash, redirect, url_for
from werkzeug.utils import secure_filename
import os
import time
from . import views_bp
from config import FRAUD_SCORE_THRESHOLD
from fraud_detector import FraudDetector

detector = FraudDetector()

# Cấu hình upload
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@views_bp.route('/')
def index():
    try:
        # Kiểm tra trạng thái database mỗi khi load trang
        has_data, stats = detector.check_data()
        return render_template('index.html', has_data=has_data, stats=stats)
    except Exception as e:
        # Xử lý lỗi kết nối database
        flash(f"Lỗi kết nối database: {str(e)}", "error")
        return render_template('index.html', has_data=False, stats={}, error=str(e))

@views_bp.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('Không tìm thấy file', 'error')
        return redirect(request.url)
    
    file = request.files['file']
    
    if file.filename == '':
        flash('Chưa chọn file', 'error')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        # Import dữ liệu
        start_time = time.time()
        
        try:
            success = detector.import_data(filepath)
            if success:
                import_time = time.time() - start_time
                flash(f'Import thành công trong {import_time:.2f} giây', 'success')
            else:
                flash('Import không thành công', 'error')
        except Exception as e:
            flash(f'Lỗi: {str(e)}', 'error')
            
        return redirect(url_for('views.index'))
    
    flash('File không hợp lệ, chỉ chấp nhận file CSV', 'error')
    return redirect(request.url)

@views_bp.route('/analyze', methods=['POST'])
def analyze():
    try:
        # Thực hiện phân tích
        start_time = time.time()
        detector.analyze_fraud()
        end_time = time.time()
        print(f"Hàm analyze_fraud() chạy trong {end_time - start_time:.2f} giây")
        flash('Phân tích hoàn tất', 'success')
    except Exception as e:
        flash(f'Lỗi khi phân tích: {str(e)}', 'error')
    
    return redirect(url_for('views.results'))

@views_bp.route('/results')
def results():
    return render_template('results.html', fraud_threshold=FRAUD_SCORE_THRESHOLD)

@views_bp.route('/clear_database', methods=['POST'])
def clear_database():
    try:
        success = detector.clear_database()
        if success:
            flash('Đã xóa thành công tất cả dữ liệu', 'success')
        else:
            flash('Có lỗi khi xóa dữ liệu', 'error')
    except Exception as e:
        flash(f'Lỗi: {str(e)}', 'error')
    
    return redirect(url_for('views.index'))
