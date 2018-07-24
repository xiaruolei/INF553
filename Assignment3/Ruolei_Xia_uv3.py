import csv
import sys
import numpy as np
import math



if __name__ == "__main__":
    data = open(sys.argv[1]).readlines() 
    n = int(sys.argv[2])
    m = int(sys.argv[3])
    f = int(sys.argv[4])
    iterations = int(sys.argv[5])

    user = []
    movie = []
    rownum = len(data)
    mat = np.zeros(shape=(n, m))
    
    # initial mat
    for i in range(1, rownum):
        lines = data[i].split(',')
        user.append(int(lines[0]))
        movie.append(int(lines[1]))
    
    user = sorted(set(user))
    movie = sorted(set(movie))

    for i in range(1, rownum):
        lines = data[i].split(',')
        mat[user.index(int(lines[0]))][movie.index(int(lines[1]))] = float(lines[2])

    # UV decomposition
    U = np.ones(shape=(n, f))
    V = np.ones(shape=(f, m))

    for it in range(iterations):    
        for r in range(n):
            for s in range(f):
                denom = 0
                numer = 0
                for j in range(m):
                    # tmp = 0.0
                    if mat[r][j] > 0: 
                        # for k in range(f):
                        #     if(k != s):
                        #         tmp += U[r][k] * V[k][j]
                        tmp = np.dot(U[r,:],V[:,j]) - U[r][s] * V[s][j]
                        numer += V[s][j] * (mat[r][j] - tmp)
                        denom += math.pow(V[s][j], 2)
                U[r][s] = float(numer)/float(denom)
#         print(U)
        
        for r in range(f):
            for s in range(m):
                denom = 0
                numer = 0
                for i in range(n):
                    # tmp = 0.0
                    if mat[i][s] > 0:
                        # for k in range(f):
                        #     if(k != r):
                        #         tmp += U[i][k] * V[k][s]
                        tmp = np.dot(U[i,:], V[:,s]) - U[i][r] * V[r][s]         
                        numer += U[i][r] * (mat[i][s] - tmp)
                        denom += U[i][r] * U[i][r]
                        
                V[r][s] = float(numer)/float(denom)
#         print(V) 
        
        approx = np.dot(U, V)
#         print(approx)
        count = 0
        error = 0.0
        for i in range(n):
            for j in range(m):
                if mat[i][j] > 0:
                    error += math.pow(mat[i][j] - approx[i][j], 2)
                    count += 1
        error = error/count
        error = math.sqrt(error)
        print("%.4f" % error)
         
