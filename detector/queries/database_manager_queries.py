"""
Chứa tất cả các truy vấn Cypher sử dụng trong DatabaseManager
"""

# Queries liên quan đến setup và index
CREATE_ACCOUNT_INDEX = "CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)"

# Queries liên quan đến import data
CREATE_ACCOUNTS_QUERY = """
UNWIND $accounts AS id
MERGE (a:Account {id: id})
"""

CREATE_TRANSACTIONS_QUERY = """
UNWIND $batch AS tx
MATCH (from:Account {id: tx.from_ac})
MATCH (to:Account {id: tx.to_ac})
CREATE (from)-[r:SENT {
    amount: tx.amount,
    step: tx.step,
    is_fraud: tx.is_fraud,
    type: tx.type
}]->(to)
"""

# Queries liên quan đến kiểm tra dữ liệu
COUNT_ALL_NODES = "MATCH (n) RETURN count(n) as count"
COUNT_ACCOUNTS = "MATCH (a:Account) RETURN count(a) as count"
COUNT_TRANSACTIONS = "MATCH ()-[r:SENT]->() RETURN count(r) as count" 
CHECK_ANALYZED = "MATCH (a:Account) WHERE a.fraud_score IS NOT NULL RETURN count(a) as count"

# Queries liên quan đến cleanup
DROP_ACCOUNT_INDEX = "DROP INDEX ON :Account(id)"
DELETE_ALL = "MATCH (n) DETACH DELETE n"

# Graph projections
def get_main_projection(graph_name):
    return f"""
    CALL gds.graph.project(
        '{graph_name}',
        'Account',
        {{
            SENT: {{
                type: 'SENT',
                orientation: 'NATURAL',
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

def get_similarity_projection(graph_name):
    return f"""
    CALL gds.graph.project.cypher(
        '{graph_name}',
        'MATCH (a:Account) 
        WHERE EXISTS((a)-[:SENT]->())  // Đảm bảo node có gửi transaction
        RETURN id(a) AS id, labels(a) AS labels',
        'MATCH (a:Account)-[:SENT]->(tx:Transaction)-[:RECEIVED]->(b:Account)
        RETURN id(a) AS source, id(b) AS target, "TRANSFER" AS type',
        {{
            validateRelationships: false
        }}
    ) YIELD graphName AS filteredGraphName
    RETURN filteredGraphName
    """

def get_temporal_projection(graph_name):
    return f"""
    CALL gds.graph.project(
        '{graph_name}',
        'Account',
        {{
            SENT: {{
                type: 'SENT',
                orientation: 'NATURAL',
                properties: {{
                    weight: {{
                        property: 'step',
                        defaultValue: 0,
                        aggregation: 'NONE'
                    }}
                }}
            }}
        }}
    )
    """

def get_drop_graph_query(graph_name):
    return f"CALL gds.graph.drop('{graph_name}', false)"

# Cleanup properties
def get_cleanup_node_properties_query(properties):
    properties_to_remove = ", ".join([f"n.{prop}" for prop in properties])
    return f"""
    MATCH (n)
    REMOVE {properties_to_remove}
    """

CLEANUP_RELATIONSHIP_PROPERTIES = """
MATCH ()-[r:SENT]->()
REMOVE r.anomaly_score, r.flagged
"""

DELETE_SIMILAR_RELATIONSHIPS = "MATCH ()-[r:SIMILAR]-() DELETE r"