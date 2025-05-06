from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
import os
import time
from fraud_detector import FraudDetector
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Cấu hình upload
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB limit

# Đảm bảo thư mục upload tồn tại
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    detector = FraudDetector()
    
    try:
        # Kiểm tra trạng thái database mỗi khi load trang
        has_data, stats = detector.check_data()
        return render_template('index.html', has_data=has_data, stats=stats)
    except Exception as e:
        # Xử lý lỗi kết nối database
        flash(f"Lỗi kết nối database: {str(e)}", "error")
        return render_template('index.html', has_data=False, stats={}, error=str(e))
    finally:
        detector.close()

@app.route('/upload', methods=['POST'])
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
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Import dữ liệu
        detector = FraudDetector()
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
        finally:
            detector.close()
            
        return redirect(url_for('index'))
    
    flash('File không hợp lệ, chỉ chấp nhận file CSV', 'error')
    return redirect(request.url)

@app.route('/analyze', methods=['POST'])
def analyze():
    detector = FraudDetector()
    try:
        # Thực hiện phân tích
        detector.analyze_fraud()
        flash('Phân tích hoàn tất', 'success')
    except Exception as e:
        flash(f'Lỗi khi phân tích: {str(e)}', 'error')
    finally:
        detector.close()
    
    return redirect(url_for('results'))

@app.route('/results')
def results():
    return render_template('results.html')

# Thêm route để xóa dữ liệu
@app.route('/clear_database', methods=['POST'])
def clear_database():
    detector = FraudDetector()
    
    try:
        success = detector.clear_database()
        if success:
            flash('Đã xóa thành công tất cả dữ liệu', 'success')
        else:
            flash('Có lỗi khi xóa dữ liệu', 'error')
    except Exception as e:
        flash(f'Lỗi: {str(e)}', 'error')
    finally:
        detector.close()
    
    return redirect(url_for('index'))

# Cập nhật route để lấy thông tin database
@app.route('/api/status')
def get_status():
    detector = FraudDetector()
    
    try:
        has_data, stats = detector.check_data()
        return jsonify({
            'has_data': has_data,
            'stats': stats
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'has_data': False,
            'stats': {}
        }), 500
    finally:
        detector.close()

@app.route('/api/data')
def get_data():
    detector = FraudDetector()
    
    try:
        with detector.driver.session() as session:
            # Thống kê cơ bản
            metrics = session.run("""
                MATCH (a:Account)
                RETURN count(a) AS total_accounts
            """).single()
            
            tx_count = session.run("""
                MATCH ()-[r:SENT]->()
                RETURN count(r) AS count
            """).single()
            
            # Kiểm tra phân tích
            has_analysis = False
            try:
                check = session.run("MATCH (a:Account) WHERE a.fraud_score > 0 RETURN count(a) AS count LIMIT 1").single()
                has_analysis = check and check['count'] > 0
            except:
                pass
            
            # Tài khoản đáng ngờ - giới hạn số lượng để tối ưu
            suspicious = []
            if has_analysis:
                suspicious = session.run("""
                    MATCH (a:Account)
                    WHERE a.fraud_score > 0.5
                    RETURN a.id AS id, a.fraud_score AS score, 
                           a.community AS community
                    ORDER BY a.fraud_score DESC
                    LIMIT 10
                """).data()
            
            # Lấy dữ liệu đồ thị giới hạn
            network = {"nodes": [], "links": []}
            if has_analysis:
                # Lấy top nodes đáng ngờ
                top_nodes = session.run("""
                    MATCH (a:Account)
                    WHERE a.fraud_score > 0.6
                    RETURN a.id AS id
                    ORDER BY a.fraud_score DESC
                    LIMIT 5
                """).data()
                
                top_ids = [node["id"] for node in top_nodes]
                
                if top_ids:
                    # Lấy nodes và kết nối trong mạng lưới
                    nodes_result = session.run("""
                        MATCH (a:Account)
                        WHERE a.id IN $ids OR a.fraud_score > 0.5
                        RETURN a.id AS id, a.fraud_score AS score
                        LIMIT 20
                    """, {"ids": top_ids}).data()
                    
                    all_ids = [node["id"] for node in nodes_result]
                    
                    # Lấy liên kết
                    links_result = session.run("""
                        MATCH (a:Account)-[r:SENT]->(b:Account)
                        WHERE a.id IN $ids AND b.id IN $ids
                        RETURN a.id AS source, b.id AS target, r.amount AS value
                        LIMIT 30
                    """, {"ids": all_ids}).data()
                    
                    # Chuyển đổi dữ liệu
                    nodes = []
                    for node in nodes_result:
                        nodes.append({
                            "id": node["id"],
                            "score": node.get("score", 0.1)
                        })
                    
                    links = []
                    for link in links_result:
                        links.append({
                            "source": link["source"],
                            "target": link["target"],
                            "value": link.get("value", 1)
                        })
                    
                    network = {"nodes": nodes, "links": links}
            
            # Phân loại giao dịch
            tx_types = session.run("""
                MATCH ()-[r:SENT]->()
                RETURN r.type AS type, count(r) AS count
                LIMIT 10
            """).data()
            
            # Thống kê cộng đồng giản lược
            communities = []
            if has_analysis:
                communities = session.run("""
                    MATCH (a:Account)
                    WHERE a.community IS NOT NULL
                    RETURN a.community AS community, count(a) AS count
                    ORDER BY a.community
                    LIMIT 10
                """).data()
            
            # Đếm số gian lận
            fraud_count = len(suspicious)
            
            # Kết quả trả về
            return jsonify({
                'metrics': {
                    'accounts': metrics['total_accounts'] if metrics else 0,
                    'transactions': tx_count['count'] if tx_count else 0,
                    'fraud': fraud_count,
                    'communities': len(communities) if communities else 0
                },
                'has_analysis': has_analysis,
                'suspicious': suspicious,
                'fraud_by_type': tx_types,
                'communities': communities,
                'network': network
            })
            
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu: {e}")
        return jsonify({
            'error': str(e), 
            'has_analysis': False,
            'metrics': {'accounts': 0, 'transactions': 0, 'fraud': 0, 'communities': 0},
            'suspicious': [],
            'tx_types': [],
            'communities': [],
            'network': {'nodes': [], 'links': []}
        }), 500
    finally:
        detector.close()

if __name__ == '__main__':
    app.run(debug=True, port=8080)  # Thay đổi port từ 5000 (mặc định) sang 8080