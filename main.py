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
FAKE_SCRIPTS_ROOT_FOLDER = 'fake'
FAKE_SCRIPTS_EXT = 'py'

def build_master_table(schemas):
    '''
    return list of tables and respective dependencies
    '''
    master={}
    def extrapolate_tables(content):
        '''
        function that finds dependent tables from sql statement
        assumes tables are identified by surrounding "`" character in sql
        '''
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
    '''
    return list of tables, respective dependencies and run sequence
    '''
    class Node:
        '''
        class object representing a graph data structure (nodes and edges)
        in this instance, an edge from one node to another represents dependency
        '''
        def __init__(self, name, level):
            self.name = name
            self.level = level
            self.edges = []
    
        def addEdge(self, node):
            self.edges.append(node)

    def resolve_dependency(node, resolved):
        '''
        recursive algorithm that traverses through each node and their dependencies
        returns synchronous run order
        '''
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

def get_fake_process(node):
    node_name_split = node.name.split('.')
    return '{0}/{1}/{2}.{3}'.format(FAKE_SCRIPTS_ROOT_FOLDER, node_name_split[0], node_name_split[1], FAKE_SCRIPTS_EXT)

def build_master_table_levels(master_table_graph):
    '''
    algorithm that assigns a "level" to a node based on dependencies
    '''
    current_level = 1
    #1. set level for root tables (tables without any dependencies)
    for node in master_table_graph:
        if len(node.edges) == 0:
            node.level = current_level
    #2. set level for tables with dependencies
    while len([node.name for node in master_table_graph if node.level is None]) > 0:
        processed_level_tables = [level_node.name for level_node in master_table_graph if (level_node.level is not None and level_node.level <= current_level)]
        for node in master_table_graph:
            if node.level is None:
                node_edges = [e.name for e in node.edges]
                complete_match =  all(elem in processed_level_tables for elem in node_edges)
                if complete_match:
                    node.level = current_level + 1
        current_level += 1
    return master_table_graph

def a3_show_result(master_list):
    for node in master_list:
        print('name: {0}, edges: {1}, level {2}'.format(node.name, len(node.edges), node.level))

def get_max_parallel_run(master_list):
    '''
    derive max number of processes that will run at any one time
    '''
    jobs = []
    [jobs.append(node.level) for node in master_list]
    freq = {} 
    for items in jobs: 
        freq[items] = jobs.count(items)
    max_key = max(freq, key=freq.get)
    return freq[max_key]

if __name__ == '__main__':   
    print(
        '''
        ##############################################################################################
        A.1
        Approach:
            1. Build a master table containing all tables and their dependencies
            2. Dynamically generate this table via parsing sql scripts
        
        ##############################################################################################
        '''
    )
    master_table = build_master_table(schemas=['tmp', 'final', 'raw'])
    a1_show_result(master_table)
    print(
        '''
        ##############################################################################################
        A.2
        Approach:
            1. Enhance master table from A.1 to include sequencing
            2. Base dependency logic loosely on graph algorithms (...concept of nodes and edges)
            3. A table will be represented as node, and dependencies is the edge(s)
            4. Run dummy scripts in the correct order based on master table sequence
            5. Scripts will run sychronously one after another for this excercise
        
        ##############################################################################################
         '''
    )
    master_table = build_master_table(schemas=['tmp', 'final'])
    master_table_graph_sync = build_master_table_ordered_sync(master_table)
    #a2_show_result(master_table_graph_sync)
    # run scripts sequentially...
    processes = [get_fake_process(node) for node in master_table_graph_sync]
    pool = Pool(processes=1) # single process
    print('\nBegin synchronous processing...')
    pool.map(run_process, processes)
    print(
        '''
        ##############################################################################################
        A.3
        Approach:
            1. Enhance master table from A.2 to include concept of "level"
            2. Every node (table) is assigned a "level" which will determine when it can be run
            3. Levels are run sequentially: run level 1, ...then level 2, ..then level 3 etc.. 
            4. However, all tables within the same "level" can be run in parallel
            5. Run dummy scripts based on master table sequence and levels
            6. Scripts will run utilising parallelism for this excercise
        
        ##############################################################################################
        '''
    )
    master_table_graph_levels = build_master_table_levels(master_table_graph_sync)
    #a3_show_result(master_table_graph_levels)
    # run scripts utilising parallism...
    processor_count = get_max_parallel_run(master_table_graph_levels)
    pool = Pool(processes=processor_count) # multi processing
    max_level = master_table_graph_levels[len(master_table_graph_levels)-1].level
    current_level = 1
    while current_level <= max_level:
        current_level_processes = []
        for node in master_table_graph_levels:
            if node.level == current_level:
                current_level_processes.append(get_fake_process(node))
        #execute scripts for the level
        print('\nBegin processing level {0} nodes...'.format(str(current_level)))
        pool.map(run_process, current_level_processes)
        current_level += 1
