# Fraud Detection System - Community Viewer

## Giới thiệu

Tính năng "Community Viewer" (Trình xem cộng đồng rủi ro) là một công cụ mạnh mẽ để phân tích chi tiết các cộng đồng rủi ro được phát hiện trong hệ thống phát hiện gian lận. Tài liệu này hướng dẫn cách sử dụng chức năng mới này.

## Cộng đồng rủi ro là gì?

Cộng đồng rủi ro là một nhóm các tài khoản có mối liên hệ với nhau thông qua giao dịch và có khả năng liên quan đến hoạt động gian lận. Hệ thống phát hiện gian lận của chúng tôi xác định các cộng đồng này bằng cách phân tích mạng lưới giao dịch và nhóm các tài khoản có mối liên hệ chặt chẽ.

## Cách sử dụng Community Viewer

### Mở Community Viewer

Có hai cách để mở Community Viewer:

1. **Từ màn hình kết quả phân tích**: Nhấn vào nút "Xem chi tiết" ở góc phải trên của phần "Phân Tích Cộng Đồng Rủi Ro".
2. **Từ biểu đồ cộng đồng**: Nhấn trực tiếp vào bất kỳ phần nào của biểu đồ để xem chi tiết cộng đồng tương ứng.

### Danh sách cộng đồng

Khi mở Community Viewer, bạn sẽ thấy danh sách tất cả các cộng đồng rủi ro được phát hiện trong hệ thống:

- **Bộ lọc**: Sử dụng các bộ lọc ở phía trên để lọc cộng đồng theo kích thước (nhỏ, trung bình, lớn) hoặc mức độ rủi ro (thấp, trung bình, cao).
- **Tìm kiếm**: Nhập ID cộng đồng vào ô tìm kiếm để nhanh chóng tìm một cộng đồng cụ thể.
- **Thẻ cộng đồng**: Mỗi thẻ hiển thị thông tin cơ bản về cộng đồng, bao gồm ID, số lượng tài khoản và điểm rủi ro trung bình.

### Xem chi tiết cộng đồng

Khi nhấn vào nút "Xem chi tiết" trên thẻ cộng đồng, một cửa sổ modal sẽ hiển thị với thông tin chi tiết về cộng đồng đó, bao gồm:

#### 1. Tổng quan

- **Thông tin cơ bản**: ID cộng đồng, kích thước, mức độ rủi ro.
- **Số liệu thống kê**: Tổng số tài khoản, số tài khoản rủi ro, số giao dịch nội bộ, điểm rủi ro trung bình.

#### 2. Tab "Tài khoản"

Hiển thị danh sách các tài khoản trong cộng đồng với thông tin chi tiết:

- **ID Tài khoản**: Mã định danh của tài khoản.
- **Điểm rủi ro**: Điểm gian lận được tính toán cho tài khoản.
- **Đặc điểm**: Đặc điểm nổi bật của tài khoản (vòng tròn giao dịch, mất cân bằng, trung tâm mạng lưới...).
- **Giao dịch ra/vào**: Số lượng và giá trị giao dịch gửi và nhận.
- **Chênh lệch**: Chênh lệch giữa giao dịch ra và vào.

#### 3. Tab "Mạng lưới"

- **Biểu đồ mạng lưới**: Hiển thị trực quan mối quan hệ giữa các tài khoản trong cộng đồng.
  - Các nút đại diện cho tài khoản, kích thước nút tương ứng với điểm rủi ro.
  - Các cạnh đại diện cho giao dịch giữa các tài khoản.
  - Màu sắc thể hiện mức độ rủi ro (đỏ: cao, cam: trung bình, xanh: thấp).
- **Giao dịch đáng chú ý**: Bảng hiển thị các giao dịch quan trọng nhất trong cộng đồng.

#### 4. Tab "Liên kết"

Hiển thị danh sách các cộng đồng khác có mối liên hệ với cộng đồng đang xem, bao gồm:

- **ID cộng đồng liên quan**.
- **Số tài khoản** trong cộng đồng liên quan.
- **Số giao dịch liên quan** giữa hai cộng đồng.
- **Tổng giá trị giao dịch** giữa hai cộng đồng.
- **Điểm rủi ro trung bình** của cộng đồng liên quan.

## Lợi ích của Community Viewer

1. **Phân tích hệ thống**: Hiểu rõ hơn về cấu trúc và mối quan hệ của các cộng đồng rủi ro.
2. **Xác định các trung tâm gian lận**: Tìm ra các tài khoản trung tâm trong mạng lưới gian lận.
3. **Theo dõi mối quan hệ**: Phân tích cách các nhóm gian lận liên kết với nhau.
4. **Đánh giá rủi ro**: Đánh giá mức độ rủi ro dựa trên nhiều yếu tố khác nhau.

## Ghi chú kỹ thuật

Community Viewer sử dụng:
- API RESTful để lấy dữ liệu từ backend
- D3.js để hiển thị biểu đồ mạng lưới tương tác
- Bootstrap để xây dựng giao diện đáp ứng
- Thuật toán phân tích graph để tính toán mối quan hệ giữa các cộng đồng

---

Liên hệ team phát triển nếu bạn có bất kỳ câu hỏi hoặc phản hồi nào về tính năng này.
