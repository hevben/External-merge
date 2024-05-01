from multiprocessing import Process, Manager, Queue, Pipe
import multiprocessing
import os
import numpy as np
import math



RAM = 1024 # maximum amount of RAM we are able to use (in bytes)
    # лучше, чтобы было кратно (cores*int_size) - в нашем случае 64 байт (не так уж и много)
int_size = 4 # np.int_ in our case (почему-то не 8, а 4)
cores = multiprocessing.cpu_count()//2 # ядер процессора

path = 'massiv.bin'
f_size = os.path.getsize(path) # file size in bytes

def sort_m(data):
    length = len(data)
    if length <= 1:
        return data
    middle = length // 2
    left = merge_sort(data[:middle])
    right = merge_sort(data[middle:])
    return merge(left, right)

def merge(data1,data2):
    len1 = len(data1)
    len2 = len(data2)
    result = np.array([], dtype = np.int_)
    p1 = 0
    p2 = 0
    i = 0
    while p1<len1 and p2<len2:
        if data1[p1] < data2[p2]:
            result = np.append(result, data1[p1])
            p1 += 1
        else:
            result = np.append(result, data2[p2])
            p2 += 1
    if p1 == len1:
        if p2 <len2:
            result = np.concatenate([result, data2[p2:]])
    if p2 == len2:
        if p1 < len1:
            result = np.concatenate([result, data1[p1:]])
    return result

def merge_parallel(tup):
    a1, a2, iteration = tup

    path1 = './outputs/' + str(a1) + '_' + str(iteration) + '.bin'
    path2 = './outputs/' + str(a2) + '_' + str(iteration) + '.bin'
    path_out = './outputs/' + str(a1//2) + '_' + str(iteration+1) + '.bin'

    len1 = int(os.path.getsize(path1)/int_size)
    len2 = int(os.path.getsize(path2)/int_size)
    p1 = 0
    p2 = 0
    i = 0

    chunk_size = 1024 # в байтах
    chunk_size_num = chunk_size // int_size # в количестве цифр
    chunk = np.array([], dtype = np.int_)

    with open(path_out, 'wb') as f:
        while p1 < len1 and p2 < len2:
            value1 = int(np.fromfile(path1, count=1, offset=p1 * int_size, dtype=np.int_))
            value2 = int(np.fromfile(path2, count=1, offset=p2 * int_size, dtype=np.int_))
            if value1 < value2:
                chunk = np.append(chunk, value1)
                p1 += 1
            else:
                chunk = np.append(chunk, value2)
                p2 += 1
            if len(chunk) > chunk_size_num:
                f.write(chunk.tobytes())
                chunk = np.array([])
            if p1 == len1 and p2 < len2:
                f.write(chunk.tobytes())
                chunk = np.array([], dtype = np.int_)
                ostalos = len2 - p2
                n = ostalos // chunk_size_num
                ostatok = n%chunk_size_num
                for i in range(n):
                    chunk = np.fromfile(path2, count = chunk_size_num, offset = p2+chunk_size*i, dtype=np.int_)
                    f.write(chunk.tobytes())
                chunk = np.array([])
                if ostatok >0:
                    chunk = np.fromfile(path2, count = ostatok, offset = p2+chunk_size*n, dtype=np.int_)
            if p2 == len2 and p1 < len1:
                f.write(chunk.tobytes())
                chunk = np.array([], dtype = np.int_)
                ostalos = len1 - p1
                n = ostalos // chunk_size_num
                ostatok = n % chunk_size_num
                for i in range(n):
                    chunk = np.fromfile(path1, count=chunk_size_num, offset=p1 + chunk_size * i, dtype=np.int_)
                    f.write(chunk.tobytes())
                chunk = np.array([])
                if ostatok > 0:
                    chunk = np.fromfile(path1, count=ostatok, offset=p1 + chunk_size * n, dtype=np.int_)


def k_merge(k,iteration, len_of_files):
    # k - длина последовательности массивов
    #mem = RAM / (k+1) # объем памяти, который мы можем использовать для одного участка массива
    #cnt = mem//int_size
    N = math.ceil(max(len_of_files)/cnt) # сколько максимум раз нам надо будет прочитать каждый файл
    M = k//2
    if k%2 == 1:
        #print('Renaming, k = ', k)
        path_to = './outputs/' + str(M) + '_' + str(iteration+1) + '.bin'
        if os.path.exists(path_to):
            os.remove(path_to)
        os.rename('./outputs/' + str(k-1) + '_' + str(iteration) + '.bin', path_to) # то самое переименование конечного файла
        #print('rename_done, k = ', k)
    listt = [(i,i+1,iteration) for i in range(0,M*2,2)]
    #print(k)
    iter = 0 # не относится к переменной iterations
    while M > cores:
        with multiprocessing.Pool(processes = cores) as pool:
            xi = pool.map(merge_parallel, listt[iter*cores:cores*(iter+1)])
            #print(listt[iter*cores:cores*(iter+1)])
            M -= cores
            iter+=1
    if M>0:
        with multiprocessing.Pool(processes = M) as pool:
            xi2 = pool.map(merge_parallel, listt[-M:])
            #print(listt[-M:])
    k = k//2 + k%2
    return k


if __name__ == "__main__":
    cnt = RAM//int_size # how much numbers we are able to take from file to RAM
    N = math.ceil(f_size / RAM)
    file_num = 0
    len_of_files = [] # сюда будем записывать длины каждого из файлов (в количестве символов)
    for i in range(N):
        data = []

        for j in range(cores):
            data.append(np.fromfile(path, count=cnt//cores, offset=j*RAM//cores + i*RAM, dtype = np.int_))
        with multiprocessing.Pool(processes=cores) as pool:
        # pool = multiprocessing.Pool(processes=cores)
            data = pool.map(np.sort, data) # пункт 4



        if not os.path.exists('./outputs'):
            os.makedirs('./outputs/')

        for j, d in enumerate(data):
            if len(d)>0:
                d.tofile('./outputs/' + str(file_num) + '_0.bin')
                file_num += 1
                len_of_files.append(len(d))
    iteration = 0
    while file_num > 1:
        #print(iteration)
        file_num = k_merge(file_num,iteration, len_of_files)
        iteration += 1

    final_path = './outputs/0_'+str(iteration)+'.bin'

    # то, что ниже, только для маленьких массивов !

    final_result = np.fromfile(final_path,count = 256, dtype = np.int_)
    is_sorted = lambda a: np.all(a[:-1] <= a[1:])
    print(is_sorted(final_result))