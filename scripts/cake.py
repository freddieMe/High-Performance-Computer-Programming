"""Author: Jiyu Chen 908066
   Purpose: comp90024 cluster and cloud computing
   Date: 09/04/2018
"""

from mpi4py import MPI
import json
import re
import time

instagram_path = "bigInstagram.json"
melbGrid_path = "melbGrid.json"



def get_coordinates(melbGrid,block):
    """extract coordinates from melbgrid file
    para: melbGrid: json loaded grid data
    para: block: block id
    return: coordinates
    """
    grid = melbGrid
    #grid = json.load(open(melbGrid_path))
    for feature in grid['features']:
        prop = feature.get('properties')
        if prop.get('id') == block:
            xmin = prop.get('xmin')
            xmax = prop.get('xmax')
            ymin = prop.get('ymin')
            ymax = prop.get('ymax')
    return xmin, xmax, ymin, ymax

def cons_coordinates_lookup():
    """construct grid blocks coordinates look-up dictionary
    return: grid blocks coodinates dictionary
    """
    cord = {}
    melb_grid = json.load(open(melbGrid_path))
    array = ['A1','A2','A3','A4','B1','B2','B3','B4','C1','C2','C3','C4','C5','D3','D4','D5']
    for block in array:
        cord[(get_coordinates(melb_grid,block))] = block
    return cord

#BELOW code shall be parallized
def fit_coordinate():
    """fit dataset into grid
    return: fitted grid
    """
    cord_lookup = cons_coordinates_lookup()
    
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    
       
    grid = {}
    line_pool = []
    
    
    f=open(instagram_path,encoding="utf-8")
   
    while True:
        # one process scenario
        if comm_size<2 and comm_rank==0:
            line_pool = []
            line = f.readline()                
            if line:
                if re.match("{\"id\":",line):
                    line = line.rstrip(",\n")                                   
                    line_pool.append(line)
                else:
                    line_pool.append("move")                
            else:
                #padding none object
                line_pool.append(None)
            
        # parallel computing scenario               
        elif comm_size>1 and comm_rank == 0:
            line_pool=[]
            for _ in range(comm_size):
                line = f.readline()
                if line:
                    line = line.rstrip(",\n")
                    line_pool.append(line)
                else:
                    # padding none object when reaching the end of dataset
                    line_pool.append(None)
        recv_line = None
        # parallel scenario
        if comm_size > 1:    
            recv_line = comm.scatter(line_pool, root=0)
        # single process scenario
        else:
            try:
                recv_line = line_pool[0]
            except:
                None
    
        if recv_line:
            #fitting recieving data into grid
            grid = fit_grid(recv_line,grid,cord_lookup)
        else:
            break
    
        
    grid = comm.gather(grid) 
    
    # master process reduce processed data
    if comm_rank == 0:
        grid = reduce(grid)
   
        return grid

def fit_grid(recv_line,grid,cord_lookup):
    """load a json data and fit grid according to coodinates look-up
    para: recv_line: a line of json format data
    para: grid: dictionary format{block: count}
    para: cord_lookup: coodinate dictionary
    return: grid: fitted grid
    """
    try:
        line = json.loads(recv_line)
        #print(ins)
        doc = line.get('doc')
        # ignore location tag
        #location = doc.get('location')
        #extract coodinate tag
        coordinates = doc.get('coordinates')
        if coordinates:
            y = coordinates.get('coordinates')[0]
            x = coordinates.get('coordinates')[1]
            for (xmin,xmax,ymin,ymax) in list(cord_lookup):
                if x>xmin and x<=xmax and y>=ymin and y<ymax:
                    block = cord_lookup.get((xmin,xmax,ymin,ymax))
                    grid[block] = grid.get(block,0) + 1               
    except:
        None
    #print(grid)
    return grid

def reduce(grid):
    """master process reduce gathered data
    para: grid: a list of grids
    return: reduced grid
    """
    cord = {}
    array = ['A1','A2','A3','A4','B1','B2','B3','B4','C1','C2','C3','C4','C5','D3','D4','D5']
    for i in grid:
        for t in array:
            if i.get(t):
                cord[t] = cord.get(t,0) + i.get(t)
    return cord


def summary(grid):
    """provide a hotspots summary on instagram by block, rows and columns
    para: grid: fitted grid
    print: summaries
    """
    
    
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()    
    
    if comm_rank == 0:
        # summary of individual blocks
        block = {}
        for x in list(grid):
            block[grid.get(x)] = x
            
        blocks = list(block)
        blocks.sort()
        blocks.reverse()
        for b in blocks:
            print(block.get(b)+": "+str(b))
        
        # summary by rows
        row = {}
        print("summary by row")
        A = grid.get('A1')+grid.get('A2')+grid.get('A3')+grid.get('A4')
        B = grid.get('B1')+grid.get('B2')+grid.get('B3')+grid.get('B4')
        C = grid.get('C1')+grid.get('C2')+grid.get('C3')+grid.get('C4')+grid.get('C5')
        D = grid.get('D3')+grid.get('D4')+grid.get('D5')
        row[A] = 'A'
        row[B] = 'B'
        row[C] = 'C'
        row[D] = 'D'
        rows = list(row)
        rows.sort()
        rows.reverse()
        for x in rows:
            print(row.get(x)+": "+str(x))
            
        # summary by columns
        print("summary by col")
        col = {}
        col1 = grid.get('A1')+grid.get('B1')+grid.get('C1')
        col2 = grid.get('A2')+grid.get('B2')+grid.get('C2')
        col3 = grid.get('A3')+grid.get('B3')+grid.get('C3')+grid.get('D3')
        col4 = grid.get('A4')+grid.get('B4')+grid.get('C4')+grid.get('D4')
        col5 = grid.get('C5')+grid.get('D5')
        col[col1] = 'col1'
        col[col2] = 'col2'
        col[col3] = 'col3'
        col[col4] = 'col4'
        col[col5] = 'col5'
        cols = list(col)
        cols.sort()
        cols.reverse()
        for x in cols:
            print(col.get(x)+": "+str(x))        

        
start = time.time()
grid = fit_coordinate()
print(grid)

summary(grid)

end = time.time()


print(end-start)
