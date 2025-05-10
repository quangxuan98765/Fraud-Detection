from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class DatabaseManager:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def check_data(self):
        """Kiểm tra xem đã có dữ liệu trong database chưa"""
        with self.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            
            # Lấy thêm thống kê
            stats = {}
            try:
                accounts = session.run("MATCH (a:Account) RETURN count(a) as count").single()
                stats["accounts"] = accounts["count"] if accounts else 0
                
                transactions = session.run("MATCH ()-[r:SENT]->() RETURN count(r) as count").single()
                stats["transactions"] = transactions["count"] if transactions else 0
                
                has_analysis = session.run("MATCH (a:Account) WHERE a.fraud_score IS NOT NULL RETURN count(a) as count").single()
                stats["has_analysis"] = has_analysis["count"] > 0 if has_analysis else False
            except:
                stats = {"accounts": 0, "transactions": 0, "has_analysis": False}
                
            return count > 0, stats
        
    def clear_database(self):
        """Xóa toàn bộ dữ liệu trong database"""
        with self.driver.session() as session:
            try:
                # Xóa các indexes trước (nếu có)
                try:
                    session.run("DROP INDEX ON :Account(id)")
                except:
                    pass
                    
                # Xóa tất cả nodes và relationships
                session.run("MATCH (n) DETACH DELETE n")
                
                # Kiểm tra lại để đảm bảo đã xóa thành công
                result = session.run("MATCH (n) RETURN count(n) as count").single()
                is_empty = result["count"] == 0
                
                if is_empty:
                    print("Đã xóa thành công toàn bộ dữ liệu từ database")
                else:
                    print(f"Vẫn còn {result['count']} nodes trong database")
                    
                return is_empty
            except Exception as e:
                print(f"Lỗi khi xóa database: {e}")
                return False

    def cleanup_projected_graph(self):
        """Xóa projected graph với cơ chế timeout và bỏ qua việc kiểm tra tồn tại"""
        with self.driver.session() as session:
            print("🔍 Đang xóa projected graph...")
            try:
                # Thử xóa trực tiếp mà không kiểm tra trước
                session.run("""
                    CALL gds.graph.drop('fraud_graph', false)
                    YIELD graphName
                    RETURN 'Đã xóa ' + graphName AS message
                """)
                print("  Đã xóa projected graph thành công")
            except Exception as e:
                # Nếu lỗi vì graph không tồn tại - không sao cả
                if "Graph with name fraud_graph does not exist" in str(e):
                    print("  Projected graph không tồn tại, không cần xóa")
                else:
                    print(f"  Không thể xóa projected graph: {str(e)[:150]}...")
                
                # Tiếp tục xử lý bình thường
                return True

    def close(self):
        """Đóng kết nối đến Neo4j"""
        if self.driver:
            self.driver.close()
