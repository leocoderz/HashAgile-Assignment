from elasticsearch import Elasticsearch, NotFoundError

# Initialize Elasticsearch client
es = Elasticsearch()

# Create a new collection (index) in Elasticsearch
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Index '{p_collection_name}' created.")
    else:
        print(f"Index '{p_collection_name}' already exists.")

# Index data into the collection excluding a specific column
def indexData(p_collection_name, p_exclude_column):
    # Sample employee data (can be loaded from a dataset)
    employees = [
        {'EmployeeID': 'E02001', 'Name': 'John Doe', 'Department': 'IT', 'Gender': 'Male'},
        {'EmployeeID': 'E02002', 'Name': 'Jane Smith', 'Department': 'HR', 'Gender': 'Female'},
        {'EmployeeID': 'E02003', 'Name': 'Alice Johnson', 'Department': 'Finance', 'Gender': 'Female'},
        # More data can be added here
    ]
    
    for emp in employees:
        if p_exclude_column in emp:
            emp.pop(p_exclude_column)
        es.index(index=p_collection_name, id=emp['EmployeeID'], body=emp)
    
    print(f"Data indexed in '{p_collection_name}' excluding column '{p_exclude_column}'.")

# Search for records where a specific column matches a value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    return result['hits']['hits']

# Get the count of employees in the collection
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)
    print(f"Total employee count in '{p_collection_name}': {count['count']}")
    return count['count']

# Delete an employee by ID from the collection
def delEmpById(p_collection_name, p_employee_id):
    try:
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Employee ID '{p_employee_id}' deleted from '{p_collection_name}'.")
    except NotFoundError:
        print(f"Employee ID '{p_employee_id}' not found in '{p_collection_name}'.")

# Get the count of employees grouped by department (facets)
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_facet": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    department_counts = result['aggregations']['department_facet']['buckets']
    for department in department_counts:
        print(f"Department: {department['key']}, Employee Count: {department['doc_count']}")
    return department_counts

# ------------------------
# Function Executions

v_nameCollection = 'Hash_Nataraj'
v_phoneCollection = 'Hash_1549'

# 1. Create collections
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# 2. Get employee count (before indexing)
getEmpCount(v_nameCollection)

# 3. Index data excluding specific columns
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

# 4. Delete an employee by ID
delEmpById(v_nameCollection, 'E02003')

# 5. Get employee count (after deletion)
getEmpCount(v_nameCollection)

# 6. Search by column
print(searchByColumn(v_nameCollection, 'Department', 'IT'))
print(searchByColumn(v_nameCollection, 'Gender', 'Male'))
print(searchByColumn(v_phoneCollection, 'Department', 'IT'))

# 7. Get department facets
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
