from flask import Flask
import os
from routes import api_bp, views_bp

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Cấu hình upload
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB limit

# Đảm bảo thư mục upload tồn tại
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Đăng ký các blueprints
app.register_blueprint(views_bp)
app.register_blueprint(api_bp, url_prefix='/api')

if __name__ == '__main__':
    app.run(debug=True, port=8080)
