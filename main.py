"""
This code is for Task A - Dependencies Orchestration
"""
import os
import re
import json
from multiprocessing import Pool

#constants
SQL_SCRIPTS_ROOT_FOLDER = 'res'
SQL_SCRIPTS_DIR = ['tmp', 'final'] 

def build_master_table(schemas):
    # return list of tables and respective dependencies
    master={}
    def extrapolate_tables(content):
        # function that finds dependent tables from sql statement
        # assumes tables are identified by surrounding "`" character in sql
        tables = re.findall(r"`(.*?)`", content, re.DOTALL)
        dependent_table=[]
        for table in tables:
            schema = table.split('.')[0]
            if schema in schemas:
                dependent_table.append(table)
        return dependent_table

    # get sql files
    sql_script_path=[]
    for root, dirs, files in os.walk(SQL_SCRIPTS_ROOT_FOLDER):
        dirs[:] = [d for d in dirs if d in SQL_SCRIPTS_DIR] #ignore raw
        for file in files:
            path=os.path.join(root,file)
            sql_script_path.append(path)
    # open sql files and extrapolates dependant tables
    for path in sql_script_path:
        path_split = path.split('\\')
        parent_folder = path_split[len(path_split)-2]
        file = path_split[len(path_split)-1]
        file_name = file.split('.')[0]
        table='{0}.{1}'.format(parent_folder, file_name)
        with open(path) as f:
            content = f.readlines()
        sql_content = ''.join(content)
        master[table] = extrapolate_tables(sql_content)
    return master

def a1_show_result(json_dict):
    json_str = json.dumps(master_table, indent=4, sort_keys=True)
    print(json_str)

def build_master_table_ordered_sync(master_table):
    # return list of tables, respective dependencies and run sequence
    class Node:
        # class object representing a graph data structure (nodes and edges)
        # in this instance, an edge from one node to another represents dependency
        def __init__(self, name, level):
            self.name = name
            self.level = level
            self.edges = []
    
        def addEdge(self, node):
            self.edges.append(node)

    def resolve_dependency(node, resolved):
        # recursive algorithm that traverses through each node and their dependencies
        # returns synchronous run order
        for edge in node.edges:
            if edge not in resolved:
                resolve_dependency(edge, resolved)
        if node not in resolved:
            resolved.append(node)

    # create nodes
    table_node_dict = {}
    for table_name in master_table:
        table_node_dict[table_name] = Node(table_name, None)
    # add edges to nodes
    for table_name in master_table:
        node = table_node_dict.get(table_name)
        edges = master_table.get(table_name, None)
        if edges:
            for edge_str in edges:
                node.addEdge(table_node_dict.get(edge_str))
    # run algorithm to get sequence
    table_node_ordered = []
    for table_node in table_node_dict:
        resolve_dependency(table_node_dict[table_node], table_node_ordered) 
    return table_node_ordered

def a2_show_result(master_list):
    for node in master_list:
        print('name: {0}, edges: {1}'.format(node.name, len(node.edges)))

def run_process(process):                                                             
    os.system('python {}'.format(process)) 

def get_fake_processes(master_list):
    list = []
    for node in master_list:
        list.append('fake/' + node.name.split('.')[0] + '/' + node.name.split('.')[1] + '.py')
    return list

if __name__ == '__main__':   
    '''
    A.1
    Approach:
        1. Build a master table containing tables and their dependencies
        2. Dynamically generate this table via parsing sql scripts
    '''
    master_table = build_master_table(schemas=['tmp', 'final', 'raw'])
    #a1_show_result(master_table)
    '''
    A.2
    Approach:
        1. Enhance master table from A.1 to include sequencing (...synchronous)
        2. Base logic loosely on graph algorithms (...concept of nodes and edges).
        3. Run dummy scripts based on master table sequence
    '''
    master_table = build_master_table(schemas=['tmp', 'final'])
    master_table_graph_sync = build_master_table_ordered_sync(master_table)
    #a2_show_result(master_table_graph_sync)
    processes = get_fake_processes(master_table_graph_sync) 
    #processes = ('fake/tmp/products.py', 'fake/tmp/variants.py', 'fake/final/products.py', 'fake/tmp/product_images.py') 
    #pool = Pool(processes=4)
    pool = Pool(processes=1) #synchronous
    pool.map(run_process, processes)