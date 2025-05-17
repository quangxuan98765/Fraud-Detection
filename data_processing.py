import pandas as pd
from detector.utils.config import MAX_NODES, MAX_RELATIONSHIPS
import os

def network_preserving_sampling(input_path, output_path, target_nodes=200000, target_edges=400000, target_fraud_rate=0.00129):
    """Phương pháp lấy mẫu bảo toàn cấu trúc mạng lưới và tỷ lệ gian lận với kiểm soát chặt chẽ số lượng node"""
    print("⚙️ Đang lọc dataset với phương pháp bảo toàn cấu trúc mạng...")
    
    # Đọc dữ liệu
    df = pd.read_csv(input_path)
    
    # Bước 1: Tạo danh sách các tài khoản và giao dịch
    all_accounts = set(df['nameOrig']).union(set(df['nameDest']))
    print(f"Tổng số tài khoản trong dataset gốc: {len(all_accounts)}")
    print(f"Tổng số giao dịch trong dataset gốc: {len(df)}")
    
    # Bước 2: Tính số lượng giao dịch gian lận cần lấy mẫu
    target_fraud_txs = int(target_edges * target_fraud_rate)
    fraud_df = df[df['isFraud'] == 1]
    
    if len(fraud_df) <= target_fraud_txs:
        # Nếu có ít giao dịch gian lận hơn mục tiêu, lấy tất cả
        fraud_sample = fraud_df
        print(f"Lấy tất cả {len(fraud_sample)} giao dịch gian lận")
    else:
        # Ngược lại, lấy mẫu theo số lượng mục tiêu
        fraud_sample = fraud_df.sample(n=target_fraud_txs, random_state=42)
        print(f"Lấy mẫu {len(fraud_sample)} giao dịch gian lận từ {len(fraud_df)}")
    
    # Bước 3: Xác định các tài khoản liên quan đến gian lận
    fraud_accounts = set(fraud_sample['nameOrig']).union(set(fraud_sample['nameDest']))
    print(f"Số tài khoản liên quan đến gian lận: {len(fraud_accounts)}")
    
    # Theo dõi số lượng node đã chọn
    selected_accounts = set(fraud_accounts)
    
    # Bước 4: Lấy thêm các giao dịch có liên quan đến tài khoản gian lận (1-hop)
    related_query = df['nameOrig'].isin(fraud_accounts) | df['nameDest'].isin(fraud_accounts)
    related_txs = df[related_query & ~df.index.isin(fraud_sample.index)]
    print(f"Số giao dịch liên quan đến tài khoản gian lận: {len(related_txs)}")
    
    # Bước 5: Lấy mẫu một tỷ lệ các giao dịch liên quan nếu quá nhiều
    # Lựa chọn một số lượng hợp lý để không tăng số node quá nhiều
    related_sample_size = min(len(related_txs), target_edges // 4)
    if len(related_txs) > related_sample_size:
        related_sample = related_txs.sample(n=related_sample_size, random_state=42)
    else:
        related_sample = related_txs
    print(f"Lấy {len(related_sample)} giao dịch liên quan")
    
    # Bước 6: Cập nhật danh sách tài khoản đã được chọn
    for _, row in related_sample.iterrows():
        selected_accounts.add(row['nameOrig'])
        selected_accounts.add(row['nameDest'])
    print(f"Số tài khoản đã chọn sau khi thêm 1-hop: {len(selected_accounts)}")
    
    # Kiểm tra xem đã vượt quá giới hạn node chưa
    if len(selected_accounts) > target_nodes * 0.7:
        print(f"Đã đạt {len(selected_accounts)} nodes, giới hạn node sampling để tránh vượt quá {target_nodes}")
        # Kết hợp các giao dịch đã chọn
        filtered_df = pd.concat([fraud_sample, related_sample])
    else:
        # Bước 7: Tính số lượng giao dịch còn lại cần lấy mẫu
        remaining_txs = target_edges - len(fraud_sample) - len(related_sample)
        print(f"Số giao dịch cần lấy thêm: {remaining_txs}")
        
        # Bước 8: Ưu tiên lấy các giao dịch có liên quan đến mạng lưới hiện tại (2-hop)
        second_hop_query = (df['nameOrig'].isin(selected_accounts) | 
                           df['nameDest'].isin(selected_accounts))
        second_hop_txs = df[second_hop_query & 
                          ~df.index.isin(fraud_sample.index) & 
                          ~df.index.isin(related_sample.index)]
        
        # Chỉ lấy một số lượng 2-hop giao dịch để không tăng số node quá nhiều
        second_hop_sample_size = min(len(second_hop_txs), remaining_txs // 2)
        if second_hop_txs.empty:
            second_hop_sample = pd.DataFrame()
        elif len(second_hop_txs) > second_hop_sample_size:
            second_hop_sample = second_hop_txs.sample(n=second_hop_sample_size, random_state=42)
        else:
            second_hop_sample = second_hop_txs
        print(f"Lấy thêm {len(second_hop_sample)} giao dịch từ 2-hop")
        
        # Cập nhật danh sách node đã chọn
        for _, row in second_hop_sample.iterrows():
            selected_accounts.add(row['nameOrig'])
            selected_accounts.add(row['nameDest'])
        print(f"Số tài khoản đã chọn sau khi thêm 2-hop: {len(selected_accounts)}")
        
        # Bước 9: Lấy mẫu ngẫu nhiên từ các giao dịch còn lại
        # Nhưng chỉ chọn các giao dịch không tạo thêm quá nhiều node mới
        remaining_txs = target_edges - len(fraud_sample) - len(related_sample) - len(second_hop_sample)
        
        # Chỉ chọn nodes đã biết để giữ giới hạn node
        if len(selected_accounts) > target_nodes * 0.9:
            print(f"Đã đạt {len(selected_accounts)} nodes, chỉ lấy giao dịch giữa các node đã biết")
            known_nodes_query = (df['nameOrig'].isin(selected_accounts) & 
                                df['nameDest'].isin(selected_accounts))
            candidate_txs = df[known_nodes_query & 
                             ~df.index.isin(fraud_sample.index) & 
                             ~df.index.isin(related_sample.index) &
                             ~df.index.isin(second_hop_sample.index)]
        else:
            # Có thể chấp nhận một số node mới
            candidate_txs = df[~df.index.isin(fraud_sample.index) & 
                             ~df.index.isin(related_sample.index) &
                             ~df.index.isin(second_hop_sample.index)]
        
        random_sample = pd.DataFrame()
        if remaining_txs > 0 and not candidate_txs.empty:
            random_sample = candidate_txs.sample(n=min(remaining_txs, len(candidate_txs)), random_state=42)
            print(f"Lấy thêm {len(random_sample)} giao dịch ngẫu nhiên")
            
            # Cập nhật danh sách node
            for _, row in random_sample.iterrows():
                selected_accounts.add(row['nameOrig'])
                selected_accounts.add(row['nameDest'])
        
        # Kết hợp tất cả các giao dịch
        filtered_df = pd.concat([fraud_sample, related_sample, second_hop_sample, random_sample])
    
    # Tính tỉ lệ gian lận sau khi lấy mẫu
    current_fraud_rate = len(filtered_df[filtered_df['isFraud'] == 1]) / len(filtered_df)
    target_fraud_count = int(len(filtered_df) * target_fraud_rate)
    current_fraud_count = len(filtered_df[filtered_df['isFraud'] == 1])
    
    # Điều chỉnh tỉ lệ gian lận nếu cần
    if current_fraud_count > target_fraud_count:
        # Quá nhiều gian lận - Thêm giao dịch bình thường
        non_fraud_df = df[df['isFraud'] == 0]
        non_fraud_df = non_fraud_df[~non_fraud_df.index.isin(filtered_df.index)]
        
        # Ưu tiên giao dịch giữa các node đã biết
        known_nodes_query = (non_fraud_df['nameOrig'].isin(selected_accounts) & 
                            non_fraud_df['nameDest'].isin(selected_accounts))
        priority_txs = non_fraud_df[known_nodes_query]
        
        if not priority_txs.empty:
            additional_count = min(len(priority_txs), int((current_fraud_count - target_fraud_count) / target_fraud_rate))
            additional_txs = priority_txs.sample(n=min(additional_count, len(priority_txs)), random_state=42)
            filtered_df = pd.concat([filtered_df, additional_txs])
            print(f"Thêm {len(additional_txs)} giao dịch bình thường để điều chỉnh tỉ lệ gian lận")
    
    # Kiểm tra kích thước cuối cùng
    nodes = len(set(filtered_df['nameOrig']).union(set(filtered_df['nameDest'])))
    relationships = len(filtered_df)
    fraud_count = len(filtered_df[filtered_df['isFraud'] == 1])
    
    # Nếu vẫn vượt quá giới hạn node, áp dụng lọc mạnh
    if nodes > target_nodes:
        print(f"Vẫn vượt quá giới hạn node ({nodes} > {target_nodes}), áp dụng lọc nghiêm ngặt...")
        
        # Tính số lượng node cần loại bỏ
        excess_ratio = nodes / target_nodes
        target_txs = int(len(filtered_df) / excess_ratio)
        
        # Ưu tiên giữ lại giao dịch gian lận
        fraud_txs = filtered_df[filtered_df['isFraud'] == 1]
        non_fraud_txs = filtered_df[filtered_df['isFraud'] == 0]
        
        # Lấy danh sách tài khoản từ giao dịch gian lận
        fraud_accounts = set(fraud_txs['nameOrig']).union(set(fraud_txs['nameDest']))
        
        # Tạo danh sách tài khoản ưu tiên giữ lại
        priority_accounts = set(fraud_accounts)
        
        # Tính số lượng giao dịch không gian lận cần giữ lại
        target_non_fraud = max(1, min(target_txs - len(fraud_txs), len(non_fraud_txs)))
        
        # Ưu tiên giữ lại giao dịch liên quan đến tài khoản gian lận
        related_query = non_fraud_txs['nameOrig'].isin(priority_accounts) | non_fraud_txs['nameDest'].isin(priority_accounts)
        related_txs = non_fraud_txs[related_query]
        
        if len(related_txs) > target_non_fraud:
            # Nếu có quá nhiều giao dịch liên quan, ưu tiên các giao dịch có ít node mới nhất
            kept_non_fraud = pd.DataFrame()
            
            # Tính số node mới được thêm bởi mỗi giao dịch
            txs_with_scores = []
            current_nodes = set(fraud_txs['nameOrig'].unique()) | set(fraud_txs['nameDest'].unique())
            
            for idx, row in related_txs.iterrows():
                new_nodes = 0
                if row['nameOrig'] not in current_nodes:
                    new_nodes += 1
                if row['nameDest'] not in current_nodes:
                    new_nodes += 1
                txs_with_scores.append((idx, new_nodes))
            
            # Sắp xếp theo số node mới (ít nhất trước)
            txs_with_scores.sort(key=lambda x: x[1])
            
            # Lấy n giao dịch đầu tiên
            selected_indices = [x[0] for x in txs_with_scores[:target_non_fraud]]
            kept_non_fraud = related_txs.loc[selected_indices]
        else:
            kept_non_fraud = related_txs
            remaining = target_non_fraud - len(related_txs)
            if remaining > 0:
                # Lấy các giao dịch còn lại, ưu tiên các giao dịch với node đã biết
                remaining_txs = non_fraud_txs[~related_query]
                if not remaining_txs.empty:
                    # Tính điểm ưu tiên cho mỗi giao dịch
                    current_nodes = set(fraud_txs['nameOrig'].unique()) | set(fraud_txs['nameDest'].unique()) | \
                                  set(kept_non_fraud['nameOrig'].unique()) | set(kept_non_fraud['nameDest'].unique())
                    
                    txs_with_scores = []
                    for idx, row in remaining_txs.iterrows():
                        new_nodes = 0
                        if row['nameOrig'] not in current_nodes:
                            new_nodes += 1
                        if row['nameDest'] not in current_nodes:
                            new_nodes += 1
                        txs_with_scores.append((idx, new_nodes))
                    
                    # Sắp xếp theo số node mới (ít nhất trước)
                    txs_with_scores.sort(key=lambda x: x[1])
                    
                    # Lấy n giao dịch đầu tiên
                    selected_indices = [x[0] for x in txs_with_scores[:remaining]]
                    if selected_indices:
                        kept_non_fraud = pd.concat([kept_non_fraud, remaining_txs.loc[selected_indices]])
        
        # Kết hợp lại
        filtered_df = pd.concat([fraud_txs, kept_non_fraud])
        
        # Kiểm tra lại
        nodes = len(set(filtered_df['nameOrig']).union(set(filtered_df['nameDest'])))
        relationships = len(filtered_df)
        fraud_count = len(filtered_df[filtered_df['isFraud'] == 1])
    
    print(f"\n✅ Kết quả cuối cùng:")
    print(f"- Nodes: {nodes} (mục tiêu: {target_nodes})")
    print(f"- Relationships: {relationships} (mục tiêu: {target_edges})")
    print(f"- Giao dịch gian lận: {fraud_count}")
    print(f"- Tỷ lệ fraud: {fraud_count/relationships*100:.4f}% (mục tiêu: {target_fraud_rate*100:.4f}%)")
    
    filtered_df.to_csv(output_path, index=False)
    return output_path