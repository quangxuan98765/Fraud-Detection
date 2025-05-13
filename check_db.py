from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def check_database():
    # Kết nối tới database
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        # Kiểm tra các loại node
        node_types = session.run("""
            MATCH (n) 
            RETURN DISTINCT labels(n) as node_types, count(*) as count 
            ORDER BY count DESC
        """).data()
        
        print("\n===== NODE TYPES =====")
        for type_info in node_types:
            print(f"{type_info['node_types']}: {type_info['count']}")
        
        # Kiểm tra các loại relationship
        rel_types = session.run("""
            MATCH ()-[r]->()
            RETURN DISTINCT type(r) as rel_type, count(*) as count
            ORDER BY count DESC
        """).data()
        
        print("\n===== RELATIONSHIP TYPES =====")
        for rel_info in rel_types:
            print(f"{rel_info['rel_type']}: {rel_info['count']}")
        
        # Kiểm tra thuộc tính của Account nodes
        account_properties = session.run("""
            MATCH (a:Account)
            WITH a LIMIT 1
            RETURN properties(a) as props
        """).single()
        
        if account_properties:
            print("\n===== ACCOUNT PROPERTIES =====")
            for key, value in account_properties["props"].items():
                print(f"{key}: {type(value).__name__} = {value}")
        
        # Kiểm tra các thuộc tính về gian lận
        fraud_properties = session.run("""
            MATCH (a:Account)
            RETURN 
                count(a) as total_accounts,
                count(a.is_fraud) as is_fraud_count,
                count(a.fraud) as fraud_count,
                count(a.known_fraud) as known_fraud_count,
                count(a.fraud_score) as fraud_score_count,
                sum(CASE WHEN a.fraud_score >= 0.7 THEN 1 ELSE 0 END) as high_score_count
        """).single()
        
        if fraud_properties:
            print("\n===== FRAUD PROPERTIES =====")
            print(f"Total accounts: {fraud_properties['total_accounts']}")
            print(f"With is_fraud property: {fraud_properties['is_fraud_count']}")
            print(f"With fraud property: {fraud_properties['fraud_count']}")
            print(f"With known_fraud property: {fraud_properties['known_fraud_count']}")
            print(f"With fraud_score property: {fraud_properties['fraud_score_count']}")
            print(f"With high fraud score (>=0.7): {fraud_properties['high_score_count']}")
        
        # Kiểm tra các thuộc tính của relationship
        rel_properties = session.run("""
            MATCH ()-[r]->()
            WITH r LIMIT 1
            RETURN type(r) as type, properties(r) as props
        """).single()
        
        if rel_properties:
            print("\n===== RELATIONSHIP PROPERTIES =====")
            print(f"Type: {rel_properties['type']}")
            for key, value in rel_properties["props"].items():
                print(f"{key}: {type(value).__name__} = {value}")
        
        # Kiểm tra số lượng giao dịch từ các tài khoản có fraud_score cao
        high_score_txs = session.run("""
            MATCH (a:Account)-[t]->()
            WHERE a.fraud_score >= 0.7
            RETURN count(DISTINCT t) as high_score_txs
        """).single()
        
        if high_score_txs:
            print("\n===== HIGH SCORE TRANSACTIONS =====")
            print(f"Transactions from accounts with fraud_score >= 0.7: {high_score_txs['high_score_txs']}")
    
    # Đóng kết nối
    driver.close()
    print("\nDatabase check completed.")

if __name__ == "__main__":
    check_database()
