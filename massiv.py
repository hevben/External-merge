import numpy as np
import os
N = 256*12 # количество чисел в массиве
path = 'massiv.bin'
arr = np.random.randint(0,100,N,dtype = np.int_) # N -
# = np.array([1,2,3,4,5,6,7,8,9,10])
arr.tofile(path)

# np.int_ size is 8 bytes
# but it seems like it's actually 4

f_size = os.path.getsize(path)
print('File created')
print('File size:',f_size,'bytes')
print('Numbers in array:', N)
# y = np.fromfile(path, count=2, offset=0, dtype=np.int_).astype('int')
# print(y)
# y = np.fromfile(path, count=1, offset=9*4, dtype=np.int_).astype('int')
# print(y)
