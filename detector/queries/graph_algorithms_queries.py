"""
Chứa các truy vấn Cypher cho các thuật toán đồ thị
"""

# Queries cho Degree Centrality
def get_degree_query(graph_name):
    return f"""
    CALL gds.degree.write(
        '{graph_name}',
        {{
            writeProperty: 'degScore',
            relationshipWeightProperty: 'weight'
        }}
    )
    """

# Queries cho PageRank
def get_pagerank_query(graph_name):
    return f"""
    CALL gds.pageRank.write(
        '{graph_name}',
        {{
            writeProperty: 'prScore',
            relationshipWeightProperty: 'weight',
            maxIterations: 20,
            dampingFactor: 0.85
        }}
    )
    """

# Queries cho Community Detection
def get_community_query(graph_name):
    return f"""
    CALL gds.louvain.write(
        '{graph_name}',
        {{
            writeProperty: 'communityId',
            relationshipWeightProperty: 'weight',
            includeIntermediateCommunities: false,
            tolerance: 0.0001,
            maxIterations: 10,
            concurrency: 4
        }}
    )
    """

# Query tính kích thước cộng đồng
COMMUNITY_SIZE_QUERY = """
MATCH (n)
WHERE n.communityId IS NOT NULL
WITH n.communityId AS communityId, COUNT(*) AS size
WHERE size >= 3
MATCH (m)
WHERE m.communityId = communityId
SET m.communitySize = size

WITH MIN(size) AS minSize, MAX(size) AS maxSize
MATCH (n)
WHERE n.communitySize IS NOT NULL
SET n.normCommunitySize = 
    CASE 
        WHEN (maxSize - minSize) = 0 THEN 0
        ELSE (n.communitySize - minSize) / (maxSize - minSize)
    END
"""

# Queries cho Node Similarity
def get_similarity_query(graph_name):
    return f"""
    CALL gds.nodeSimilarity.write(
        '{graph_name}',
        {{
            writeProperty: 'simScore',
            writeRelationshipType: 'SIMILAR',
            similarityCutoff: 0.2,
            topK: 5,
            concurrency: 4
        }}
    )
    """

# Fallback query cho Node Similarity nếu phiên bản ghi thất bại
def get_fallback_similarity_query(graph_name):
    return f"""
    CALL gds.nodeSimilarity.stream(
        '{graph_name}',
        {{
            similarityCutoff: 0.2,
            topK: 3,
            concurrency: 4
        }}
    )
    YIELD node1, node2, similarity
    WITH gds.util.asNode(node1) AS source, gds.util.asNode(node2) AS target, similarity
    LIMIT 50000
    SET source.simScore = CASE 
        WHEN source.simScore IS NULL OR similarity > source.simScore 
        THEN similarity ELSE source.simScore END
    RETURN COUNT(*) as relationshipsProcessed
    """

# Query thiết lập giá trị mặc định cho similarity score
SET_DEFAULT_SIM_QUERY = """
MATCH (n)
WHERE n.simScore IS NULL
SET n.simScore = 0.0
"""

# Queries cho Betweenness Centrality
def get_betweenness_query(graph_name):
    return f"""
    CALL gds.betweenness.write(
        '{graph_name}',
        {{
            writeProperty: 'btwScore'
        }}
    )
    """

# Queries cho HITS Algorithm
def get_hits_query(graph_name):
    return f"""
    CALL gds.alpha.hits.write(
        '{graph_name}',
        {{
            writeProperty: '',
            hitsIterations: 20,
            authProperty: 'authScore',
            hubProperty: 'hubScore' 
        }}
    )
    """

# Queries cho K-Core Decomposition
def get_kcore_projection_query(graph_name):
    kcore_graph_name = f'{graph_name}-undirected'
    return f"""
    CALL gds.graph.project(
        '{kcore_graph_name}',
        'Account',
        {{
            SENT: {{
                type: 'SENT',
                orientation: 'UNDIRECTED',
                properties: {{
                    weight: {{
                        property: 'amount',
                        defaultValue: 0.0,
                        aggregation: 'NONE'
                    }}
                }}
            }}
        }}
    )
    """

def get_kcore_query(graph_name):
    kcore_graph_name = f'{graph_name}-undirected'
    return f"""
    CALL gds.kcore.write(
        '{kcore_graph_name}',
        {{
            writeProperty: 'coreScore'
        }}
    )
    """

def get_kcore_cleanup_query(graph_name):
    kcore_graph_name = f'{graph_name}-undirected'
    return f"""
    CALL gds.graph.drop('{kcore_graph_name}', false)
    """

# Queries cho Triangle Count
def get_triangle_projection_query(graph_name):
    triangle_graph_name = f'{graph_name}-undirected-tri'
    return f"""
    CALL gds.graph.project(
        '{triangle_graph_name}',
        'Account',
        {{
            SENT: {{
                type: 'SENT',
                orientation: 'UNDIRECTED'
            }}
        }}
    )
    """

def get_triangle_query(graph_name):
    triangle_graph_name = f'{graph_name}-undirected-tri'
    return f"""
    CALL gds.triangleCount.write(
        '{triangle_graph_name}',
        {{
            writeProperty: 'triCount'
        }}
    )
    """

def get_triangle_cleanup_query(graph_name):
    triangle_graph_name = f'{graph_name}-undirected-tri'
    return f"""
    CALL gds.graph.drop('{triangle_graph_name}', false)
    """

# Query thiết lập giá trị mặc định cho triangle count
SET_DEFAULT_TRI_QUERY = """
MATCH (n)
WHERE n.triCount IS NULL
SET n.triCount = 0
"""

# Query phát hiện chu trình
CYCLE_QUERY = """
MATCH (a:Account)
OPTIONAL MATCH path = (a)-[:SENT]->(tx1:Transaction)-[:RECEIVED]->(b:Account)-[:SENT]->(tx2:Transaction)-[:RECEIVED]->(c:Account)-[:SENT]->(tx3:Transaction)-[:RECEIVED]->(a)
WITH a, COUNT(path) AS cycleCount
SET a.cycleCount = cycleCount
"""

# Query phân tích bất thường thời gian
TEMPORAL_BURST_QUERY = """
// Tính số lượng giao dịch trong 1 giờ và 24 giờ cho mỗi account
MATCH (a:Account)-[tx:SENT]->()
WITH a, tx.step AS step
WITH a, step, COUNT(*) AS hourlyCount
WITH a, COLLECT({step: step, count: hourlyCount}) AS hourlyCounts

// Tính tempBurst1h (giá trị cao nhất trong 1 giờ)
WITH a, hourlyCounts, 
    REDUCE(max = 0, h IN hourlyCounts | CASE WHEN h.count > max THEN h.count ELSE max END) AS maxHourly
SET a.tempBurst1h = maxHourly

// Tính tempBurst24h (số giờ liên tiếp với ít nhất 1 giao dịch - cách đơn giản hóa)
WITH a, hourlyCounts
UNWIND hourlyCounts AS h
WITH a, h.step AS step
ORDER BY step
// Sử dụng tỷ lệ số giờ có giao dịch so với tổng khoảng thời gian làm proxy cho burst
WITH a, MIN(step) AS minStep, MAX(step) AS maxStep, COUNT(DISTINCT step) AS uniqueSteps
WITH a, 
    CASE 
        WHEN maxStep = minStep THEN 1  // Nếu chỉ có 1 giờ, burst là 1
        ELSE toFloat(uniqueSteps) / (maxStep - minStep + 1)  // Tỷ lệ giờ hoạt động trên tổng thời gian
    END * 24 AS burstDensity  // Scale to 24 hours
SET a.tempBurst24h = burstDensity

// Tính tempBurst tổng hợp (kết hợp cả 1h và 24h)
WITH a
SET a.tempBurst = (a.tempBurst1h * 0.7) + (a.tempBurst24h * 0.3)
"""

# Query thiết lập giá trị mặc định cho tất cả node
SET_DEFAULT_VALUES_QUERY = """
MATCH (n)
SET n.degScore = COALESCE(n.degScore, 0),
    n.prScore = COALESCE(n.prScore, 0),
    n.communityId = COALESCE(n.communityId, -1),
    n.normCommunitySize = COALESCE(n.normCommunitySize, 0),
    n.simScore = COALESCE(n.simScore, 0),
    n.btwScore = COALESCE(n.btwScore, 0),
    n.authScore = COALESCE(n.authScore, 0),
    n.hubScore = COALESCE(n.hubScore, 0),
    n.coreScore = COALESCE(n.coreScore, 0),
    n.triCount = COALESCE(n.triCount, 0),
    n.cycleCount = COALESCE(n.cycleCount, 0),
    n.tempBurst = COALESCE(n.tempBurst, 0)
"""