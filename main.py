"""
This code is for Task A - Dependencies Orchestration
"""
import os
import re
import json
import multiprocessing
from pathlib import Path

#constants
SQL_SCRIPTS_ROOT_FOLDER = 'res'
SQL_SCRIPTS_DIR = ['tmp', 'final'] 
FAKE_SCRIPTS_ROOT_FOLDER = 'fake'
FAKE_SCRIPTS_EXT = 'py'

def extrapolate_tables(content, schemas):
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

def build_master_table(schemas):
    '''
    return list of tables and respective dependencies
    '''
    master={}
    working_dir = Path.cwd()
    for ds in SQL_SCRIPTS_DIR:
        for file in working_dir.glob('{0}/{1}/*.*'.format(SQL_SCRIPTS_ROOT_FOLDER, ds)):
            table='{0}.{1}'.format(ds, file.stem)
            with open(file) as f:
                content = f.readlines()
            sql_content = ''.join(content)
            master[table] = extrapolate_tables(sql_content, schemas)
    return master

def a1_show_result(json_dict):
    json_str = json.dumps(master_table, indent=4, sort_keys=True)
    print(json_str)

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

def build_master_table_ordered_sync(master):
    '''
    return list of tables, respective dependencies and run sequence
    '''
    # create nodes
    table_node_dict = {}
    for table_name in master:
        table_node_dict[table_name] = Node(table_name, None)
    # add edges to nodes
    for table_name in master:
        node = table_node_dict.get(table_name)
        edges = master.get(table_name, None)
        if edges:
            for edge_str in edges:
                node.addEdge(table_node_dict.get(edge_str))
    # run algorithm to get sequence
    table_node_ordered = []
    for table_node in table_node_dict:
        resolve_dependency(table_node_dict[table_node], table_node_ordered) 
    return table_node_ordered

def a2_show_result(master):
    for node in master:
        print('name: {0}, edges: {1}'.format(node.name, len(node.edges)))

def get_process_path(node, root, ext):
    '''
    helper that returns the full path of table script
    '''
    node_name_split = node.name.split('.')
    return '{0}/{1}/{2}.{3}'.format(root, node_name_split[0], node_name_split[1], ext)

def build_master_table_levels(master):
    '''
    algorithm that assigns a "level" to a node based on dependencies
    '''
    current_level = 1
    #1. set level for root tables (tables without any dependencies)
    for node in master:
        if len(node.edges) == 0:
            node.level = current_level
    #2. set level for tables with dependencies
    while len([node.name for node in master if node.level is None]) > 0:
        processed_level_tables = [level_node.name for level_node in master if (level_node.level is not None and level_node.level <= current_level)]
        for node in master:
            if node.level is None:
                node_edges = [e.name for e in node.edges]
                complete_match =  all(elem in processed_level_tables for elem in node_edges)
                if complete_match:
                    node.level = current_level + 1
        current_level += 1
    return master

def run_process(process):                                                             
    os.system('python {}'.format(process)) 

def execute_jobs_sequential(master):
    '''
    Function that will execute the warehouse load based on master table
    Will only run sequentially
    '''
    tables = [get_process_path(node, FAKE_SCRIPTS_ROOT_FOLDER, FAKE_SCRIPTS_EXT) for node in master]
    print('\nBegin synchronous processing...')
    for table in tables:
        process = multiprocessing.Process(target=run_process, args=(table,))
        process.start()
        process.join()

def execute_jobs(master):
    '''
    Function that will execute the warehouse load based on master table
    Will utilise parallel at each "level"
    '''
    max_level = master[len(master)-1].level
    current_level = 1
    running_processes = []
    while current_level <= max_level:
        current_level_tables = []
        for node in master:
            if node.level == current_level:
                current_level_tables.append(get_process_path(node, FAKE_SCRIPTS_ROOT_FOLDER, FAKE_SCRIPTS_EXT))
        #execute processes for the level
        print('\nBegin processing level {0} nodes...'.format(str(current_level)))
        for table in current_level_tables:
            process = multiprocessing.Process(target=run_process, args=(table,))
            running_processes.append(process)
            process.start()
        # wait for processes to finish
        for running_process in running_processes:
            running_process.join()
        current_level += 1

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
    a2_show_result(master_table_graph_sync)
    # run scripts sequentially...
    execute_jobs_sequential(master_table_graph_sync)
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
    # run scripts utilising parallism...
    execute_jobs(master_table_graph_levels)